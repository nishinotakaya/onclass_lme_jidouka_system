# app/workers/lme_line_counts_worker.rb
# frozen_string_literal: true

require 'json'
require 'active_support'
require 'active_support/core_ext'
require 'google/apis/sheets_v4'
require 'googleauth'

#
# 「Line流入者」シートを読み、ダッシュボードへ集計を書き込む
#
module Lme
  class LineCountsWorker
    include Sidekiq::Worker
    sidekiq_options queue: 'lme_line_counts', retry: 3

    GOOGLE_SCOPE = [Google::Apis::SheetsV4::AUTH_SPREADSHEETS].freeze

    # ==== ダッシュボードの行・列レイアウト（基準: 2025年ブロック） =================
    BASE_YEAR     = 2025
    BLOCK_HEIGHT  = 13               # 1年分で下に追加する行数
    TOTAL_ROW     = 5                # 総数の行
    MONTH_START   = 10               # 1月行
    MONTH_END     = 21               # 12月行
    REF_ROW_START = 25               # 流入経路 1月行
    REF_ROW_END   = 36               # 流入経路 12月行

    TOTAL_CELL_COL = 'D'
    TOTAL_CELL_BASE = 'D5'           # 2025年の総数セル

    # 出力ラベル（4行目固定）
    TAG_LABEL_MAP = {
      'G4' => '動画1ダイジェスト',
      'I4' => '動画1',
      'K4' => '動画2',
      'M4' => '動画3',
      'O4' => '動画4'
    }.freeze

    # タグ別「月別件数」の書き込み先列（10〜21行）
    TAG_MONTHLY_COL = {
      '動画1ダイジェスト' => 'F', # ← G は率列
      '動画1'             => 'H', # ← I は率列
      '動画2'             => 'J', # ← K は率列
      '動画3'             => 'L', # ← M は率列
      '動画4'             => 'N'  # ← O は率列
    }.freeze

    # 月次の率を書き込む列（10〜21行）
    # 「率列 => 分子（件数）列」の対応
    RATE_MONTH_COL_FROM_COUNT = {
      'G' => 'F', # 動画1ダイジェスト 率 = F/D
      'I' => 'H', # 動画1 率 = H/D
      'K' => 'J', # 動画2 率 = J/D
      'M' => 'L', # 動画3 率 = L/D
      'O' => 'N', # 動画4 率 = N/D
      'Q' => 'P', # 動画4クリック 率 = P/D
      'S' => 'R', # 予備
      'U' => 'T'  # 予備
    }.freeze

    # 流入経路 → ダッシュボードの件数列（25〜36行）
    REFERRER_OUT_COL = {
      '小松'           => 'E',
      '西野'           => 'G',
      '加藤'           => 'I',
      '西野 ショート'  => 'M',
      'YouTube概要欄'  => 'O',
      'YouTubeTop'     => 'Q'
    }.freeze

    # 流入経路 率（25〜36行）: 「率列 => 分子（件数）列」の対応（左隣 基本）
    REF_RATE_COL_FROM_COUNT = {
      'F' => 'E',
      'H' => 'G',
      'J' => 'I',
      'L' => 'K', # 予備（K列に値が無ければ0）
      'N' => 'M',
      'P' => 'O',
      'R' => 'Q'
    }.freeze

    # ==== エントリポイント =====================================================
    def perform(spreadsheet_id = nil, source_sheet_name = nil, dashboard_sheet_name = nil, target_year = nil)
      Time.zone = 'Asia/Tokyo'

      spreadsheet_id       ||= resolve_spreadsheet_id_from_env!
      dashboard_sheet_name ||= ENV['LME_DASHBOARD_SHEET_NAME'].presence || 'ダッシュボード'
      target_year = (target_year.presence || BASE_YEAR).to_i
      year_offset = (target_year - BASE_YEAR) * BLOCK_HEIGHT

      service = build_sheets_service

      # --- 読み元シート名を決定（"Line流入者" を最優先） --------------------------
      source_sheet_name ||= resolve_source_sheet_title(service, spreadsheet_id)

      # --- ヘッダ取得（B6:ZZ6） --------------------------------------------------
      header_range = a1(source_sheet_name, 'B6:ZZ6')
      header_vr = service.get_spreadsheet_values(spreadsheet_id, header_range)
      header = Array(header_vr.values).first || []

      # 年ブロックが存在しなければ 13 行追加して確保
      ensure_year_block_exists!(service, spreadsheet_id, dashboard_sheet_name, year_offset)

      if header.blank?
        Rails.logger.info("[LmeLineCountsWorker] header empty at #{header_range}")
        write_dashboard!(service, spreadsheet_id, dashboard_sheet_name, year_offset, 0, Array.new(12, 0), {}, {})
        write_tag_monthlies!(service, spreadsheet_id, dashboard_sheet_name, default_tag_monthlies, year_offset)
        write_video4_counts!(service, spreadsheet_id, dashboard_sheet_name, Array.new(12, 0), year_offset)
        write_referrer_counts!(service, spreadsheet_id, dashboard_sheet_name, Hash.new { |h,k| h[k]=Array.new(12,0) }, year_offset)
        write_ref_total_row_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset) # ← D25〜36 合計式
        write_monthly_percentage_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_referrer_rate_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_qsu_row5_zero!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_labels!(service, spreadsheet_id, dashboard_sheet_name)
        return
      end

      # --- データ範囲決定 --------------------------------------------------------
      last_col_index  = header.size # B 起点の個数
      last_col_letter = a1_col(('B'.ord - 'A'.ord) + last_col_index)
      data_range = a1(source_sheet_name, "B7:#{last_col_letter}100000")

      vr = service.get_spreadsheet_values(spreadsheet_id, data_range)
      values = Array(vr.values)
      if values.blank?
        Rails.logger.info("[LmeLineCountsWorker] no rows at #{data_range}")
        write_dashboard!(service, spreadsheet_id, dashboard_sheet_name, year_offset, 0, Array.new(12, 0), {}, {})
        write_tag_monthlies!(service, spreadsheet_id, dashboard_sheet_name, default_tag_monthlies, year_offset)
        write_video4_counts!(service, spreadsheet_id, dashboard_sheet_name, Array.new(12, 0), year_offset)
        write_referrer_counts!(service, spreadsheet_id, dashboard_sheet_name, Hash.new { |h,k| h[k]=Array.new(12,0) }, year_offset)
        write_ref_total_row_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset) # ← 追加
        write_monthly_percentage_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_referrer_rate_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_qsu_row5_zero!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_labels!(service, spreadsheet_id, dashboard_sheet_name)
        return
      end

      # --- 2) ヘッダ名 → インデックス解決（ゆれ対応） ---------------------------
      header_map  = build_header_index_map(header)
      idx_follow  = index_for_followed_at(header_map)
      idx_name    = index_for(header_map, ['名前', '氏名', 'お名前', 'LINE表示名', 'Name']) || col_index_from_letter('F') # F列保険
      idx_referrer= index_for(header_map, ['流入経路', '流入元', '参照元', '経路']) || col_index_from_letter('D')

      raise "Source header not found: 友達追加時刻/日時 @ #{source_sheet_name}!B6" if idx_follow.nil?

      # タグ列インデックス（① / 1 のゆれ吸収）
      idx_tags = {
        '動画1ダイジェスト' => index_for(header_map, ['動画①_ダイジェスト', '動画1_ダイジェスト', '動画1ダイジェスト']),
        '動画1'             => index_for(header_map, ['プロアカ_動画①', 'プロアカ_動画1', '動画①', '動画1']),
        '動画2'             => index_for(header_map, ['プロアカ_動画②', 'プロアカ_動画2', '動画②', '動画2']),
        '動画3'             => index_for(header_map, ['プロアカ_動画③', 'プロアカ_動画3', '動画③', '動画3']),
        '動画4'             => index_for(header_map, ['プロアカ_動画④', 'プロアカ_動画4', '動画④', '動画4'])
      }

      # --- 3) 5月以降 & NG名前除外で抽出 ----------------------------------------
      rows_target = []
      values.each do |row|
        name = normalize_name(safe_at(row, idx_name))
        next if banned_name?(name)

        t = parse_followed_at(safe_at(row, idx_follow))
        next unless t && t.year == target_year && t.month >= 5
        rows_target << row
      end

      total = rows_target.size

      # --- 4) 友だち「月別」総数 ------------------------------------------------
      monthly_total = Array.new(12, 0)
      rows_target.each do |row|
        t = parse_followed_at(safe_at(row, idx_follow))
        next unless t
        monthly_total[t.month - 1] += 1
      end

      # --- 5) タグ別 総数 & 率（総数に対する％） --------------------------------
      tag_totals = {}
      tag_rates  = {}
      idx_tags.each do |out_label, idx|
        cnt = 0
        if idx
          rows_target.each do |row|
            v = safe_at(row, idx)
            cnt += 1 if v.to_s.include?('タグあり')
          end
        end
        tag_totals[out_label] = cnt
        tag_rates[out_label]  = total.positive? ? ((cnt.to_f / total) * 100).round(1) : ''
      end

      # --- 6) タグ別「月別」件数 ------------------------------------------------
      tag_monthlies = default_tag_monthlies
      rows_target.each do |row|
        t = parse_followed_at(safe_at(row, idx_follow))
        next unless t
        m_idx = t.month - 1
        idx_tags.each do |out_label, idx|
          next unless idx
          v = safe_at(row, idx)
          tag_monthlies[out_label][m_idx] += 1 if v.to_s.include?('タグあり')
        end
      end

      # --- 7) 動画4クリック数（P列）: Q以降の「{月}/{日}参加」ヘッダで◯カウント ---
      video4_counts = Array.new(12, 0)
      q_start_idx = col_index_from_letter('Q') # B6基準の相対index（B=0）
      (q_start_idx...header.size).each do |i|
        h = header[i].to_s
        if h =~ /\A(\d{1,2})\/(\d{1,2})参加\z/
          mm = Regexp.last_match(1).to_i
          next if mm < 5 # 5月以降のみ
          values.each do |row|
            name = normalize_name(safe_at(row, idx_name))
            next if banned_name?(name)
            cell = safe_at(row, i)
            video4_counts[mm - 1] += 1 if cell.to_s.strip == '◯'
          end
        end
      end

      # --- 8) 流入経路（D列）月別集計 -------------------------------------------
      referrer_counts = Hash.new { |h, k| h[k] = Array.new(12, 0) }
      if idx_referrer
        rows_target.each do |row|
          t = parse_followed_at(safe_at(row, idx_follow))
          next unless t
          m_idx = t.month - 1
          v = safe_at(row, idx_referrer).to_s
          REFERRER_OUT_COL.keys.each do |ref_kw|
            referrer_counts[ref_kw][m_idx] += 1 if v.include?(ref_kw)
          end
        end
      end

      # --- ログ -----------------------------------------------------------------
      Rails.logger.info("[Counts] year=#{target_year} total=#{total}")
      Rails.logger.info("[Counts] monthly total=#{monthly_total.each_with_index.map { |c,i| "#{i+1}月:#{c}" }.join(', ')}")
      Rails.logger.info("[Counts] tag totals=#{tag_totals}")
      Rails.logger.info("[Counts] tag rates(%)=#{tag_rates}")
      Rails.logger.info("[Counts] video4 counts=#{video4_counts}")
      Rails.logger.info("[Counts] referrer counts=#{referrer_counts}")

      # --- 9) 書き込み（年オフセット適用） ---------------------------------------
      ensure_sheet_exists!(service, spreadsheet_id, dashboard_sheet_name)

      write_dashboard!(service, spreadsheet_id, dashboard_sheet_name, year_offset, total, monthly_total, tag_totals, tag_rates)
      write_tag_monthlies!(service, spreadsheet_id, dashboard_sheet_name, tag_monthlies, year_offset)
      write_video4_counts!(service, spreadsheet_id, dashboard_sheet_name, video4_counts, year_offset)
      write_referrer_counts!(service, spreadsheet_id, dashboard_sheet_name, referrer_counts, year_offset)

      write_labels!(service, spreadsheet_id, dashboard_sheet_name) # ラベル（4行目）

      # D25〜36 の合計式（E+G+I+K+M+O+Q+S）
      write_ref_total_row_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)

      # 率の関数（10〜21行の G/I/K/M/O/Q/S/U、25〜36行の F/H/J/L/N/P/R）
      write_monthly_percentage_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
      write_referrer_rate_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)

      # Q/S/U の 5行目は 0
      write_qsu_row5_zero!(service, spreadsheet_id, dashboard_sheet_name, year_offset)

      # ※ E10〜E21 には何も入れません（本コードでは一切書き込んでいません）
    end

    # ==== ラベル書き込み（4行目） ===============================================
    def write_labels!(service, spreadsheet_id, dashboard_sheet_name)
      label_updates = TAG_LABEL_MAP.map do |cell, label|
        Google::Apis::SheetsV4::ValueRange.new(
          range: a1(dashboard_sheet_name, cell),
          values: [[label]]
        )
      end
      service.batch_update_values(
        spreadsheet_id,
        Google::Apis::SheetsV4::BatchUpdateValuesRequest.new(
          value_input_option: 'USER_ENTERED',
          data: label_updates
        )
      )
    end

    # ==== 総数・月別・タグ総数/率 ===============================================
    def write_dashboard!(service, spreadsheet_id, sheet, year_offset, total_friends, monthly, tag_totals, tag_rates)
      total_row = TOTAL_ROW + year_offset
      month_from = MONTH_START + year_offset
      month_to   = MONTH_END + year_offset

      # 総数
      service.update_spreadsheet_value(
        spreadsheet_id,
        a1(sheet, "#{TOTAL_CELL_COL}#{total_row}"),
        Google::Apis::SheetsV4::ValueRange.new(values: [[total_friends.to_i]]),
        value_input_option: 'USER_ENTERED'
      )

      # 月別（友だち総数） D列
      service.update_spreadsheet_value(
        spreadsheet_id,
        a1(sheet, "#{TOTAL_CELL_COL}#{month_from}:#{TOTAL_CELL_COL}#{month_to}"),
        Google::Apis::SheetsV4::ValueRange.new(values: (0..11).map { |i| [monthly[i].to_i] }),
        value_input_option: 'USER_ENTERED'
      )

      # （5・6行目の総数/率は本要件では扱わないためここでは未更新）
    end

    # ==== タグ別 月別件数 書き込み（F/H/J/L/N の 10〜21 行） ====================
    def write_tag_monthlies!(service, spreadsheet_id, sheet, tag_monthlies, year_offset)
      month_from = MONTH_START + year_offset
      month_to   = MONTH_END + year_offset

      updates = []
      tag_monthlies.each do |label, arr|
        col = TAG_MONTHLY_COL[label]
        next unless col
        updates << Google::Apis::SheetsV4::ValueRange.new(
          range: a1(sheet, "#{col}#{month_from}:#{col}#{month_to}"),
          values: (0..11).map { |i| [arr[i].to_i] }
        )
      end

      return if updates.empty?
      service.batch_update_values(
        spreadsheet_id,
        Google::Apis::SheetsV4::BatchUpdateValuesRequest.new(
          value_input_option: 'USER_ENTERED',
          data: updates
        )
      )
    end

    # ==== 動画4クリック数（P10〜P21） ==========================================
    def write_video4_counts!(service, spreadsheet_id, sheet, counts, year_offset)
      month_from = MONTH_START + year_offset
      month_to   = MONTH_END + year_offset
      service.update_spreadsheet_value(
        spreadsheet_id,
        a1(sheet, "P#{month_from}:P#{month_to}"),
        Google::Apis::SheetsV4::ValueRange.new(values: (0..11).map { |i| [counts[i].to_i] }),
        value_input_option: 'USER_ENTERED'
      )
    end

    # ==== 流入経路 件数（25〜36行） ============================================
    def write_referrer_counts!(service, spreadsheet_id, sheet, counts_hash, year_offset)
      row_from = REF_ROW_START + year_offset
      row_to   = REF_ROW_END   + year_offset
      updates = []

      REFERRER_OUT_COL.each do |ref, col|
        arr = counts_hash[ref] || Array.new(12, 0)
        updates << Google::Apis::SheetsV4::ValueRange.new(
          range: a1(sheet, "#{col}#{row_from}:#{col}#{row_to}"),
          values: (0..11).map { |i| [arr[i].to_i] }
        )
      end

      return if updates.empty?
      service.batch_update_values(
        spreadsheet_id,
        Google::Apis::SheetsV4::BatchUpdateValuesRequest.new(
          value_input_option: 'USER_ENTERED',
          data: updates
        )
      )
    end

    # ==== 流入経路ブロックの D25〜36 合計式（E+G+I+K+M+O+Q+S） ==================
    def write_ref_total_row_formulas!(service, spreadsheet_id, sheet, year_offset)
      row_from = REF_ROW_START + year_offset
      row_to   = REF_ROW_END   + year_offset
      updates = []
      (row_from..row_to).each do |r|
        formula = "=SUM(E#{r},G#{r},I#{r},K#{r},M#{r},O#{r},Q#{r},S#{r})"
        updates << Google::Apis::SheetsV4::ValueRange.new(
          range: a1(sheet, "D#{r}"),
          values: [[formula]]
        )
      end
      return if updates.empty?
      service.batch_update_values(
        spreadsheet_id,
        Google::Apis::SheetsV4::BatchUpdateValuesRequest.new(
          value_input_option: 'USER_ENTERED',
          data: updates
        )
      )
    end

    # ==== 月次の率（10〜21行）: G/I/K/M/O/Q/S/U に IFERROR(分子 / $D, 0) ==========
    def write_monthly_percentage_formulas!(service, spreadsheet_id, sheet, year_offset)
      month_from = MONTH_START + year_offset
      month_to   = MONTH_END + year_offset

      updates = []
      RATE_MONTH_COL_FROM_COUNT.each do |rate_col, num_col|
        (month_from..month_to).each do |r|
          formula = "=IFERROR(#{num_col}#{r}/$#{TOTAL_CELL_COL}#{r},0)"
          updates << Google::Apis::SheetsV4::ValueRange.new(
            range: a1(sheet, "#{rate_col}#{r}"),
            values: [[formula]]
          )
        end
      end

      return if updates.empty?
      service.batch_update_values(
        spreadsheet_id,
        Google::Apis::SheetsV4::BatchUpdateValuesRequest.new(
          value_input_option: 'USER_ENTERED',
          data: updates
        )
      )
    end

    # ==== 流入経路の率（25〜36行）: F/H/J/L/N/P/R に IFERROR(左隣 / $D, 0) =======
    def write_referrer_rate_formulas!(service, spreadsheet_id, sheet, year_offset)
      row_from = REF_ROW_START + year_offset
      row_to   = REF_ROW_END   + year_offset

      updates = []
      REF_RATE_COL_FROM_COUNT.each do |rate_col, count_col|
        (row_from..row_to).each do |r|
          formula = "=IFERROR(#{count_col}#{r}/$#{TOTAL_CELL_COL}#{r},0)"
          updates << Google::Apis::SheetsV4::ValueRange.new(
            range: a1(sheet, "#{rate_col}#{r}"),
            values: [[formula]]
          )
        end
      end

      return if updates.empty?
      service.batch_update_values(
        spreadsheet_id,
        Google::Apis::SheetsV4::BatchUpdateValuesRequest.new(
          value_input_option: 'USER_ENTERED',
          data: updates
        )
      )
    end

    # ==== Q/S/U の 5行目は 0 に固定 ============================================
    def write_qsu_row5_zero!(service, spreadsheet_id, sheet, year_offset)
      row = TOTAL_ROW + year_offset
      updates = %w[Q S U].map do |col|
        Google::Apis::SheetsV4::ValueRange.new(
          range: a1(sheet, "#{col}#{row}"),
          values: [[0]]
        )
      end
      service.batch_update_values(
        spreadsheet_id,
        Google::Apis::SheetsV4::BatchUpdateValuesRequest.new(
          value_input_option: 'USER_ENTERED',
          data: updates
        )
      )
    end

    # ==== 年ブロック存在保証（不足分を InsertDimension で追加） ==================
    def ensure_year_block_exists!(service, spreadsheet_id, sheet_name, year_offset)
      ss = service.get_spreadsheet(spreadsheet_id)
      sheet = ss.sheets.find { |s| s.properties&.title == sheet_name }
      return unless sheet

      sheet_id   = sheet.properties.sheet_id
      row_count  = sheet.properties.grid_properties.row_count

      needed_last_row = [REF_ROW_END, MONTH_END, TOTAL_ROW].max + year_offset
      return if row_count && row_count >= needed_last_row

      to_add = needed_last_row - row_count
      return if to_add <= 0

      insert_req = Google::Apis::SheetsV4::InsertDimensionRequest.new(
        range: Google::Apis::SheetsV4::DimensionRange.new(
          sheet_id: sheet_id,
          dimension: 'ROWS',
          start_index: row_count,         # 0-based
          end_index: row_count + to_add   # 0-based exclusive
        ),
        inherit_from_before: true
      )

      batch = Google::Apis::SheetsV4::BatchUpdateSpreadsheetRequest.new(
        requests: [Google::Apis::SheetsV4::Request.new(insert_dimension: insert_req)]
      )
      service.batch_update_spreadsheet(spreadsheet_id, batch)
    end

    # ==== Sheets 認証 ==========================================================
    def build_sheets_service
      keyfile = ENV['GOOGLE_APPLICATION_CREDENTIALS']
      raise 'ENV GOOGLE_APPLICATION_CREDENTIALS is not set.' if keyfile.blank?
      raise "Service account key not found: #{keyfile}" unless File.exist?(keyfile)

      json = JSON.parse(File.read(keyfile)) rescue nil
      unless json && json['type'] == 'service_account' && json['private_key'] && json['client_email']
        raise 'Invalid service account JSON: missing private_key/client_email/type=service_account'
      end

      auth = Google::Auth::ServiceAccountCredentials.make_creds(
        json_key_io: File.open(keyfile),
        scope: GOOGLE_SCOPE
      )
      auth.fetch_access_token!

      service = Google::Apis::SheetsV4::SheetsService.new
      service.client_options.application_name = 'LME Line Counts Uploader'
      service.authorization = auth
      service
    end

    # ==== シート探索 / 存在保証 ================================================
    def resolve_source_sheet_title(service, spreadsheet_id)
      prefer = 'Line流入者'
      ss = service.get_spreadsheet(spreadsheet_id)
      titles = ss.sheets.map { |s| s.properties&.title }.compact

      exact = titles.find { |t| t == prefer }
      return exact if exact

      partial = titles.find { |t| t.include?(prefer) }
      return partial if partial

      env = ENV['LME_SOURCE_SHEET_NAME'].presence || ENV['LME_SHEET_NAME'].presence
      return env if env.present? && titles.include?(env)

      titles.include?('Line流入者') ? 'Line流入者' : titles.first
    end

    def ensure_sheet_exists!(service, spreadsheet_id, sheet_name)
      ss = service.get_spreadsheet(spreadsheet_id)
      exists = ss.sheets.any? { |s| s.properties&.title == sheet_name }
      return if exists

      add_req = Google::Apis::SheetsV4::AddSheetRequest.new(
        properties: Google::Apis::SheetsV4::SheetProperties.new(title: sheet_name)
      )
      batch = Google::Apis::SheetsV4::BatchUpdateSpreadsheetRequest.new(
        requests: [Google::Apis::SheetsV4::Request.new(add_sheet: add_req)]
      )
      service.batch_update_spreadsheet(spreadsheet_id, batch)
    end

    # ==== ヘッダ解析（ゆれ吸収） ==============================================
    def build_header_index_map(header_row)
      map = {}
      header_row.each_with_index do |label, i|
        next if label.to_s.strip.empty?
        map[label.to_s.strip] = i
      end
      map
    end

    def index_for(hash, names)
      Array(names).find { |n| hash.key?(n) }.then { |n| n ? hash[n] : nil }
    end

    def index_for_followed_at(hash)
      # 表記ゆれ: 友達/友だち, 日時/時刻, 日
      index_for(hash, ['友達追加時刻', '友達追加日時', '友だち追加時刻', '友だち追加日時', '友達追加日', '友だち追加日'])
    end

    # ==== パース/ユーティリティ ===============================================
    def parse_followed_at(s)
      str = s.to_s.strip
      return nil if str.empty?

      # 例: "2025年9月26日 14時05分"
      if str =~ /\A(\d{4})年(\d{1,2})月(\d{1,2})日(?:\s+(\d{1,2})時(\d{1,2})分)?/
        y = Regexp.last_match(1).to_i
        m = Regexp.last_match(2).to_i
        d = Regexp.last_match(3).to_i
        hh = (Regexp.last_match(4) || '0').to_i
        mm = (Regexp.last_match(5) || '0').to_i
        return Time.zone.local(y, m, d, hh, mm)
      end

      # 例: "2025/09/26 14:05", "2025-09-26", ISOなど
      Time.zone.parse(str) || Time.parse(str)
    rescue
      nil
    end

    def normalize_name(name)
      name.to_s.strip
    end

    def banned_name?(name)
      return false if name.blank?
      n = name.gsub(/\s+/, '')
      return true if n.include?('西野たかや') || n.include?('西野タカヤ')
      return true if n.include?('加藤皇貴')
      false
    end

    def safe_at(row, idx)
      return nil unless row && idx.is_a?(Integer)
      row[idx]
    end

    # A1: `'シート名'!B2` 形式（日本語/空白に対応）
    def a1(sheet_name, inner_range)
      "'#{sheet_name.to_s.gsub("'", "''")}'!#{inner_range}"
    end

    # 1=A, 2=B ...
    def a1_col(n)
      s = +''
      while n && n > 0
        n, r = (n - 1).divmod(26)
        s.prepend((65 + r).chr)
      end
      s
    end

    # 'B'=0, 'C'=1 ... 相対index（ヘッダ配列がB起点のため）
    def col_index_from_letter(letter)
      letter_to_number(letter) - letter_to_number('B')
    end

    def letter_to_number(letter)
      l = letter.to_s.upcase
      sum = 0
      l.each_byte do |b|
        sum = sum * 26 + (b - 64) # 'A'=65 → 1
      end
      sum
    end

    # ==== Spreadsheet ID 解決 ==================================================
    def resolve_spreadsheet_id_from_env!
      id = ENV['LME_SPREADSHEET_ID'].presence || ENV['ONCLASS_SPREADSHEET_ID'].presence
      return id if id.present?

      url = ENV['LME_SPREADSHEET_URL'].presence || ENV['ONCLASS_SPREADSHEET_URL'].presence
      if url.to_s =~ %r{\Ahttps?://docs\.google\.com/spreadsheets/d/([A-Za-z0-9_-]+)}
        return Regexp.last_match(1)
      end

      raise 'Spreadsheet ID not provided. Set LME_SPREADSHEET_ID or LME_SPREADSHEET_URL (or pass as perform arg).'
    end

    # ==== デフォルト（0埋め） ===================================================
    def default_tag_monthlies
      Hash[TAG_MONTHLY_COL.keys.map { |k| [k, Array.new(12, 0)] }]
    end
  end
end