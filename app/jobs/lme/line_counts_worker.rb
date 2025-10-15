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
    BASE_YEAR = 2025
    BLOCK_HEIGHT = 13 # 1年分で下に追加する行数
    TOTAL_ROW = 5     # 総数の行
    MONTH_START = 10  # 1月行
    MONTH_END = 21    # 12月行
    REF_ROW_START = 25 # 流入経路 1月行
    REF_ROW_END = 36   # 流入経路 12月行

    # 特別集計（月次 1〜12月の行）
    SPECIAL_ROW_START   = 40
    SPECIAL_ROW_END     = 51
    BOTH_TAGS_ROW_START = 55
    BOTH_TAGS_ROW_END   = 66

    TOTAL_CELL_COL  = 'D'
    TOTAL_CELL_BASE = 'D5' # 2025年の総数セル

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
      '動画1' => 'H',            # ← I は率列
      '動画2' => 'J',            # ← K は率列
      '動画3' => 'L',            # ← M は率列
      '動画4' => 'N'             # ← O は率列
    }.freeze

    # 月次の率を書き込む列（10〜21行）: 「率列 => 分子（件数）列」
    RATE_MONTH_COL_FROM_COUNT = {
      'G' => 'F', # 動画1ダイジェスト 率 = F/D
      'I' => 'H', # 動画1 率 = H/D
      'K' => 'J', # 動画2 率 = J/D
      'M' => 'L', # 動画3 率 = L/D
      'O' => 'N', # 動画4 率 = N/D
      'Q' => 'P', # 動画4クリック 率 = P/D
      'S' => 'R', # 個別相談 率 = R/D
      'U' => 'T'  # 成約 率 = T/D
    }.freeze

    # 流入経路 → ダッシュボードの件数列（25〜36行）
    REFERRER_OUT_COL = {
      '小松'             => 'E',
      '西野'             => 'G',
      '加藤'             => 'I',
      '西野 ショート'    => 'M',
      'YouTube概要欄'    => 'O',
      'YouTube概要欄 Top'=> 'Q'
    }.freeze

    # 流入経路 率（25〜36行）: 「率列 => 分子（件数）列」（左隣など）
    REF_RATE_COL_FROM_COUNT = {
      'F' => 'E',
      'H' => 'G',
      'J' => 'I',
      'L' => 'K',
      'N' => 'M',
      'P' => 'O',
      'R' => 'Q'
    }.freeze

    # ==== エントリポイント =====================================================
    def perform(spreadsheet_id = nil, source_sheet_name = nil, dashboard_sheet_name = nil, target_year = nil)
      Time.zone = 'Asia/Tokyo'

      spreadsheet_id      ||= resolve_spreadsheet_id_from_env!
      dashboard_sheet_name ||= ENV['LME_DASHBOARD_SHEET_NAME'].presence || 'ダッシュボード'
      target_year          = (target_year.presence || BASE_YEAR).to_i
      year_offset          = (target_year - BASE_YEAR) * BLOCK_HEIGHT

      service = build_sheets_service

      # --- 読み元シート名を決定（"Line流入者" を最優先） --------------------------
      source_sheet_name ||= resolve_source_sheet_title(service, spreadsheet_id)

      # --- ヘッダ取得（B6:ZZ6） --------------------------------------------------
      header_range = a1(source_sheet_name, 'B6:ZZ6')
      header_vr    = service.get_spreadsheet_values(spreadsheet_id, header_range)
      header       = Array(header_vr.values).first || []

      # 年ブロックの行数確保
      ensure_year_block_exists!(service, spreadsheet_id, dashboard_sheet_name, year_offset)

      if header.blank?
        Rails.logger.info("[LmeLineCountsWorker] header empty at #{header_range}")
        write_dashboard!(service, spreadsheet_id, dashboard_sheet_name, year_offset, 0, Array.new(12, 0), {}, {})
        write_daily_average_e_formulas!(service, spreadsheet_id, dashboard_sheet_name, target_year, year_offset)

        write_tag_monthlies!(service, spreadsheet_id, dashboard_sheet_name, default_tag_monthlies, year_offset)
        write_video4_counts!(service, spreadsheet_id, dashboard_sheet_name, Array.new(12, 0), year_offset)
        write_kobetsu_counts!(service, spreadsheet_id, dashboard_sheet_name, Array.new(12, 0), year_offset)
        write_contract_counts!(service, spreadsheet_id, dashboard_sheet_name, Array.new(12, 0), year_offset)
        write_referrer_counts!(service, spreadsheet_id, dashboard_sheet_name, Hash.new { |h,k| h[k]=Array.new(12,0) }, year_offset)
        write_ref_total_row_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_monthly_percentage_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_referrer_rate_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_row6_summary_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_row5_total_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_row6_additional_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_special_ref_contract_counts!(service, spreadsheet_id, dashboard_sheet_name, [], nil, nil, nil, nil, nil, year_offset)
        write_special_totals_d!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_both_tags_counts!(service, spreadsheet_id, dashboard_sheet_name, Array.new(12,0), year_offset)
        write_labels!(service, spreadsheet_id, dashboard_sheet_name)
        return
      end

      # --- データ範囲取得 --------------------------------------------------------
      last_col_index  = header.size # B 起点の個数
      last_col_letter = a1_col(('B'.ord - 'A'.ord) + last_col_index)
      data_range      = a1(source_sheet_name, "B7:#{last_col_letter}100000")

      vr     = service.get_spreadsheet_values(spreadsheet_id, data_range)
      values = Array(vr.values)
      if values.blank?
        Rails.logger.info("[LmeLineCountsWorker] no rows at #{data_range}")
        write_dashboard!(service, spreadsheet_id, dashboard_sheet_name, year_offset, 0, Array.new(12, 0), {}, {})
        write_daily_average_e_formulas!(service, spreadsheet_id, dashboard_sheet_name, target_year, year_offset)

        write_tag_monthlies!(service, spreadsheet_id, dashboard_sheet_name, default_tag_monthlies, year_offset)
        write_video4_counts!(service, spreadsheet_id, dashboard_sheet_name, Array.new(12, 0), year_offset)
        write_kobetsu_counts!(service, spreadsheet_id, dashboard_sheet_name, Array.new(12, 0), year_offset)
        write_contract_counts!(service, spreadsheet_id, dashboard_sheet_name, Array.new(12, 0), year_offset)
        write_referrer_counts!(service, spreadsheet_id, dashboard_sheet_name, Hash.new { |h,k| h[k]=Array.new(12,0) }, year_offset)
        write_ref_total_row_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_monthly_percentage_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_referrer_rate_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_row6_summary_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_row5_total_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_row6_additional_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_special_ref_contract_counts!(service, spreadsheet_id, dashboard_sheet_name, [], nil, nil, nil, nil, nil, year_offset)
        write_special_totals_d!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
        write_both_tags_counts!(service, spreadsheet_id, dashboard_sheet_name, Array.new(12,0), year_offset)
        write_labels!(service, spreadsheet_id, dashboard_sheet_name)
        return
      end

      # --- 2) ヘッダ名 → インデックス解決 ---------------------------------------
      header_map  = build_header_index_map(header)
      idx_follow  = index_for_followed_at(header_map)
      idx_name    = index_for(header_map, ['名前', '氏名', 'お名前', 'LINE表示名', 'Name']) || col_index_from_letter('F')
      idx_referrer= index_for(header_map, ['流入経路', '流入元', '参照元', '経路']) || col_index_from_letter('D')
      idx_kobetsu = index_for(header_map, ['個別相談'])
      idx_contract= index_for(header_map, ['成約'])
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

      # --- 7) 動画4クリック数（P列）: Q以降の「{月}/{日}参加」で◯カウント ----------
      video4_counts = Array.new(12, 0)
      q_start_idx   = col_index_from_letter('Q') # B6基準
      (q_start_idx...header.size).each do |i|
        h = header[i].to_s
        if h =~ /\A(\d{1,2})\/(\d{1,2})参加\z/
          mm = Regexp.last_match(1).to_i
          next if mm < 5
          values.each do |row|
            name = normalize_name(safe_at(row, idx_name))
            next if banned_name?(name)
            cell = safe_at(row, i)
            video4_counts[mm - 1] += 1 if cell.to_s.strip == '◯'
          end
        end
      end

      # --- 7.5) 個別相談 / 成約 の月次件数 ---------------------------------------
      kobetsu_counts  = Array.new(12, 0)
      contract_counts = Array.new(12, 0)
      rows_target.each do |row|
        t = parse_followed_at(safe_at(row, idx_follow)); next unless t
        m_idx = t.month - 1

        if idx_kobetsu
          v = safe_at(row, idx_kobetsu).to_s.strip
          kobetsu_counts[m_idx] += 1 unless v.empty?
        end
        if idx_contract
          v = safe_at(row, idx_contract).to_s.strip
          contract_counts[m_idx] += 1 unless v.empty?
        end
      end

      # --- 8) 流入経路（25〜36行、各チャンネル件数） -----------------------------
      referrer_counts = Hash.new { |h, k| h[k] = Array.new(12, 0) }
      if idx_referrer
        rows_target.each do |row|
          t = parse_followed_at(safe_at(row, idx_follow)); next unless t
          m_idx = t.month - 1
          v = safe_at(row, idx_referrer).to_s
          REFERRER_OUT_COL.keys.each do |ref_kw|
            referrer_counts[ref_kw][m_idx] += 1 if v.include?(ref_kw)
          end
        end
      end

      # --- 8.5) 特別集計（40〜51行）
      #  E: 小松 × 成約あり
      #  G: 西野 × 成約あり  ←★ご依頼の変更
      #  K: 西野日常 × 成約あり（なければ「西野」含む & 「ショート」含まず）
      #  O: YouTube概要欄（Top を含まない）× 成約あり
      #  Q: YouTube概要欄 Top × 成約あり
      special_e = Array.new(12, 0) # 小松 + 成約
      special_g = Array.new(12, 0) # 西野 + 成約（変更後）
      special_k = Array.new(12, 0) # 西野日常 + 成約（拡張条件あり）
      special_o = Array.new(12, 0) # YouTube概要欄(Top除く) + 成約
      special_q = Array.new(12, 0) # YouTube概要欄 Top + 成約

      rows_target.each do |row|
        t = parse_followed_at(safe_at(row, idx_follow)); next unless t
        m_idx = t.month - 1

        ref = safe_at(row, idx_referrer).to_s
        has_contract = idx_contract ? safe_at(row, idx_contract).to_s.strip.present? : false
        next unless has_contract

        special_e[m_idx] += 1 if ref.include?('小松')
        special_g[m_idx] += 1 if ref.include?('西野')

        if ref.include?('西野日常') || (ref.include?('西野') && !ref.include?('ショート'))
          special_k[m_idx] += 1
        end

        if ref.include?('YouTube概要欄 Top')
          special_q[m_idx] += 1
        elsif ref.include?('YouTube概要欄')
          special_o[m_idx] += 1
        end
      end

      # --- 8.6) 「個別相談 かつ 成約」(R 55〜66) --------------------------------
      both_tags_counts = Array.new(12, 0)
      rows_target.each do |row|
        t = parse_followed_at(safe_at(row, idx_follow)); next unless t
        m_idx = t.month - 1
        has_kobetsu  = idx_kobetsu  ? safe_at(row, idx_kobetsu).to_s.strip.present? : false
        has_contract = idx_contract ? safe_at(row, idx_contract).to_s.strip.present? : false
        both_tags_counts[m_idx] += 1 if has_kobetsu && has_contract
      end

      # --- ログ -----------------------------------------------------------------
      Rails.logger.info("[Counts] year=#{target_year} total=#{total}")
      Rails.logger.info("[Counts] video4 counts=#{video4_counts}")
      Rails.logger.info("[Counts] kobetsu counts(R)=#{kobetsu_counts}")
      Rails.logger.info("[Counts] contract counts(T)=#{contract_counts}")
      Rails.logger.info("[Counts] referrer counts=#{referrer_counts}")
      Rails.logger.info("[Counts] special E(小松)=#{special_e.sum}, G(西野)=#{special_g.sum}, K(日常)=#{special_k.sum}, O(概要欄)=#{special_o.sum}, Q(Top)=#{special_q.sum}")
      Rails.logger.info("[Counts] both_tags_counts(R55-66)=#{both_tags_counts}")

      # --- 9) 書き込み -----------------------------------------------------------
      ensure_sheet_exists!(service, spreadsheet_id, dashboard_sheet_name)

      write_dashboard!(service, spreadsheet_id, dashboard_sheet_name, year_offset, total, monthly_total, tag_totals, tag_rates)
      # 追加：E10〜E21 の日次平均式
      write_daily_average_e_formulas!(service, spreadsheet_id, dashboard_sheet_name, target_year, year_offset)

      write_tag_monthlies!(service, spreadsheet_id, dashboard_sheet_name, tag_monthlies, year_offset)
      write_video4_counts!(service, spreadsheet_id, dashboard_sheet_name, video4_counts, year_offset)
      write_kobetsu_counts!(service, spreadsheet_id, dashboard_sheet_name, kobetsu_counts, year_offset)
      write_contract_counts!(service, spreadsheet_id, dashboard_sheet_name, contract_counts, year_offset)
      write_referrer_counts!(service, spreadsheet_id, dashboard_sheet_name, referrer_counts, year_offset)

      write_labels!(service, spreadsheet_id, dashboard_sheet_name)

      write_ref_total_row_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
      write_monthly_percentage_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
      write_referrer_rate_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)

      write_row6_summary_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
      write_row5_total_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)
      write_row6_additional_formulas!(service, spreadsheet_id, dashboard_sheet_name, year_offset)

      # 特別集計（E/G/K/O/Q の 40〜51 行）+ 合計 D(=SUM(E,G,I,K,M,O,Q,S))
      write_special_ref_contract_counts!(
        service, spreadsheet_id, dashboard_sheet_name,
        special_e, special_g, special_k, special_o, special_q, year_offset
      )
      write_special_totals_d!(service, spreadsheet_id, dashboard_sheet_name, year_offset)

      # 個別相談かつ成約（55〜66 行, R 列）
      write_both_tags_counts!(service, spreadsheet_id, dashboard_sheet_name, both_tags_counts, year_offset)
      copy_to_yamada_sheet!(service, spreadsheet_id, dashboard_sheet_name)
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
    def write_dashboard!(service, spreadsheet_id, sheet, year_offset, total_friends, monthly, _tag_totals, _tag_rates)
      total_row  = TOTAL_ROW + year_offset
      month_from = MONTH_START + year_offset
      month_to   = MONTH_END + year_offset

      service.update_spreadsheet_value(
        spreadsheet_id,
        a1(sheet, "#{TOTAL_CELL_COL}#{total_row}"),
        Google::Apis::SheetsV4::ValueRange.new(values: [[total_friends.to_i]]),
        value_input_option: 'USER_ENTERED'
      )

      service.update_spreadsheet_value(
        spreadsheet_id,
        a1(sheet, "#{TOTAL_CELL_COL}#{month_from}:#{TOTAL_CELL_COL}#{month_to}"),
        Google::Apis::SheetsV4::ValueRange.new(values: (0..11).map { |i| [monthly[i].to_i] }),
        value_input_option: 'USER_ENTERED'
      )
    end

    # ==== タグ別 月別件数（F/H/J/L/N の 10〜21 行） =============================
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

    # ==== 個別相談 件数（R10〜R21） ============================================
    def write_kobetsu_counts!(service, spreadsheet_id, sheet, counts, year_offset)
      month_from = MONTH_START + year_offset
      month_to   = MONTH_END + year_offset
      service.update_spreadsheet_value(
        spreadsheet_id,
        a1(sheet, "R#{month_from}:R#{month_to}"),
        Google::Apis::SheetsV4::ValueRange.new(values: (0..11).map { |i| [counts[i].to_i] }),
        value_input_option: 'USER_ENTERED'
      )
    end

    def write_contract_counts!(service, spreadsheet_id, sheet, counts, year_offset)
      month_from = MONTH_START + year_offset
      month_to   = MONTH_END + year_offset
      service.update_spreadsheet_value(
        spreadsheet_id,
        a1(sheet, "T#{month_from}:T#{month_to}"),
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

    # ==== 流入経路ブロック D25〜36 合計式（E+G+I+K+M+O+Q+S） ====================
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

    # ==== 月次の率（10〜21行）: IFERROR(分子 / $D, 0) ===========================
    def write_monthly_percentage_formulas!(service, spreadsheet_id, sheet, year_offset)
      month_from = MONTH_START + year_offset
      month_to   = MONTH_END   + year_offset

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

    # ==== E10〜E21：日次平均（D行 ÷ その月の日数 / 当月は D行 ÷ DAY(TODAY())） ====
    def write_daily_average_e_formulas!(service, spreadsheet_id, sheet, target_year, year_offset)
      month_from = MONTH_START + year_offset # 10
      today      = Time.zone.today
      curr_y     = today.year
      curr_m     = today.month

      updates = []
      12.times do |i|
        row = month_from + i       # 10..21
        mon = i + 1                # 1..12

        formula =
          if target_year == curr_y && mon == curr_m
            # 当月：本日までの日数で割る
            "=IFERROR($D#{row}/DAY(TODAY()),0)"
          else
            # それ以外：その月の最終日の日数で割る（うるう年対応）
            "=IFERROR($D#{row}/DAY(EOMONTH(DATE(#{target_year},#{mon},1),0)),0)"
          end

        updates << Google::Apis::SheetsV4::ValueRange.new(
          range: a1(sheet, "E#{row}"),
          values: [[formula]]
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

    # ==== 6行目サマリ率（K6/M6/O6/Q6）=========================================
    def write_row6_summary_formulas!(service, spreadsheet_id, sheet, year_offset)
      row5 = TOTAL_ROW + year_offset
      row6 = row5 + 1

      specs = [
        { dest: 'K', num: 'K', den: 'I', iferror: false },
        { dest: 'M', num: 'M', den: 'K', iferror: false },
        { dest: 'O', num: 'O', den: 'M', iferror: false },
        { dest: 'Q', num: 'Q', den: 'O', iferror: true }
      ]

      updates = specs.map do |sp|
        formula = sp[:iferror] ? "=IFERROR(#{sp[:num]}#{row5}/#{sp[:den]}#{row5},0)" :
                                 "=#{sp[:num]}#{row5}/#{sp[:den]}#{row5}"
        Google::Apis::SheetsV4::ValueRange.new(
          range: a1(sheet, "#{sp[:dest]}#{row6}"),
          values: [[formula]]
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

    # ==== 5行目の合計式（Q5/S5/U5） =============================================
    def write_row5_total_formulas!(service, spreadsheet_id, sheet, year_offset)
      row5      = TOTAL_ROW + year_offset
      month_from= MONTH_START + year_offset
      month_to  = MONTH_END   + year_offset

      updates = [
        { cell: "Q#{row5}", formula: "=SUM(P#{month_from}:P#{month_to})" },
        { cell: "S#{row5}", formula: "=SUM(R#{month_from}:R#{month_to})" },
        { cell: "U#{row5}", formula: "=SUM(T#{month_from}:T#{month_to})" }
      ].map do |sp|
        Google::Apis::SheetsV4::ValueRange.new(range: a1(sheet, sp[:cell]), values: [[sp[:formula]]])
      end

      service.batch_update_values(
        spreadsheet_id,
        Google::Apis::SheetsV4::BatchUpdateValuesRequest.new(value_input_option: 'USER_ENTERED', data: updates)
      )
    end

    # ==== 6行目の率式（S6/U6） ==================================================
    def write_row6_additional_formulas!(service, spreadsheet_id, sheet, year_offset)
      row5 = TOTAL_ROW + year_offset
      row6 = row5 + 1

      updates = [
        { cell: "S#{row6}", formula: "=Q#{row5}/D#{row5}" },
        { cell: "U#{row6}", formula: "=S#{row5}/D#{row5}" }
      ].map do |sp|
        Google::Apis::SheetsV4::ValueRange.new(range: a1(sheet, sp[:cell]), values: [[sp[:formula]]])
      end

      service.batch_update_values(
        spreadsheet_id,
        Google::Apis::SheetsV4::BatchUpdateValuesRequest.new(value_input_option: 'USER_ENTERED', data: updates)
      )
    end

    # ==== 特別集計（流入×成約）E/G/K/O/Q の 40〜51 行 ==========================
    def write_special_ref_contract_counts!(service, spreadsheet_id, sheet,
                                           counts_e, counts_g, counts_k, counts_o, counts_q, year_offset)
      row_from = SPECIAL_ROW_START + year_offset
      row_to   = SPECIAL_ROW_END   + year_offset

      updates = []
      { 'E' => counts_e, 'G' => counts_g, 'K' => counts_k, 'O' => counts_o, 'Q' => counts_q }.each do |col, arr|
        arr ||= Array.new(12, 0)
        updates << Google::Apis::SheetsV4::ValueRange.new(
          range: a1(sheet, "#{col}#{row_from}:#{col}#{row_to}"),
          values: (0..11).map { |i| [arr[i].to_i] }
        )
      end

      return if updates.empty?
      service.batch_update_values(
        spreadsheet_id,
        Google::Apis::SheetsV4::BatchUpdateValuesRequest.new(value_input_option: 'USER_ENTERED', data: updates)
      )
    end

    # ==== 特別集計 合計（D列 = SUM(E,G,I,K,M,O,Q,S) の 40〜51 行） ===============
    def write_special_totals_d!(service, spreadsheet_id, sheet, year_offset)
      row_from = SPECIAL_ROW_START + year_offset
      row_to   = SPECIAL_ROW_END   + year_offset

      updates = (row_from..row_to).map do |r|
        Google::Apis::SheetsV4::ValueRange.new(
          range: a1(sheet, "D#{r}"),
          values: [[ "=SUM(E#{r},G#{r},I#{r},K#{r},M#{r},O#{r},Q#{r},S#{r})" ]]
        )
      end

      service.batch_update_values(
        spreadsheet_id,
        Google::Apis::SheetsV4::BatchUpdateValuesRequest.new(value_input_option: 'USER_ENTERED', data: updates)
      )
    end

    # ==== 「個別相談 かつ 成約」R列の 55〜66 行 =================================
    def write_both_tags_counts!(service, spreadsheet_id, sheet, counts, year_offset)
      row_from = BOTH_TAGS_ROW_START + year_offset
      row_to   = BOTH_TAGS_ROW_END   + year_offset
      arr = counts || Array.new(12, 0)
      service.update_spreadsheet_value(
        spreadsheet_id,
        a1(sheet, "R#{row_from}:R#{row_to}"),
        Google::Apis::SheetsV4::ValueRange.new(values: (0..11).map { |i| [arr[i].to_i] }),
        value_input_option: 'USER_ENTERED'
      )
    end

    # ==== 年ブロック存在保証（不足分を InsertDimension で追加） ==================
    def ensure_year_block_exists!(service, spreadsheet_id, sheet_name, year_offset)
      ss    = service.get_spreadsheet(spreadsheet_id)
      sheet = ss.sheets.find { |s| s.properties&.title == sheet_name }
      return unless sheet

      sheet_id  = sheet.properties.sheet_id
      row_count = sheet.properties.grid_properties.row_count

      needed_last_row = [REF_ROW_END, MONTH_END, TOTAL_ROW, SPECIAL_ROW_END, BOTH_TAGS_ROW_END].max + year_offset
      return if row_count && row_count >= needed_last_row

      to_add = needed_last_row - row_count
      return if to_add <= 0

      insert_req = Google::Apis::SheetsV4::InsertDimensionRequest.new(
        range: Google::Apis::SheetsV4::DimensionRange.new(
          sheet_id: sheet_id,
          dimension: 'ROWS',
          start_index: row_count,
          end_index: row_count + to_add
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
      ss     = service.get_spreadsheet(spreadsheet_id)
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
      index_for(hash, ['友達追加時刻', '友達追加日時', '友だち追加時刻', '友だち追加日時', '友達追加日', '友だち追加日'])
    end

    # ==== パース/ユーティリティ ===============================================
    def parse_followed_at(s)
      str = s.to_s.strip
      return nil if str.empty?

      if str =~ /\A(\d{4})年(\d{1,2})月(\d{1,2})日(?:\s+(\d{1,2})時(\d{1,2})分)?/
        y = Regexp.last_match(1).to_i
        m = Regexp.last_match(2).to_i
        d = Regexp.last_match(3).to_i
        hh = (Regexp.last_match(4) || '0').to_i
        mm = (Regexp.last_match(5) || '0').to_i
        return Time.zone.local(y, m, d, hh, mm)
      end

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
      l.each_byte { |b| sum = sum * 26 + (b - 64) }
      sum
    end

    # ==== Spreadsheet ID 解決 ==================================================
    def resolve_spreadsheet_id_from_env!
      # まずは、環境変数LME_SPREADSHEET_IDまたはONCLASS_SPREADSHEET_IDを優先して使用
      id = ENV['LME_SPREADSHEET_ID'].presence || ENV['ONCLASS_SPREADSHEET_ID'].presence
      return id if id.present?

      # 次に、URLからスプレッドシートIDを抽出する
      url = ENV['LME_SPREADSHEET_URL'].presence || ENV['ONCLASS_SPREADSHEET_URL'].presence || ENV['LME_YAMADA_COUNT_SPREADSHEET_URL'].presence
      if url.to_s =~ %r{\Ahttps?://docs\.google\.com/spreadsheets/d/([A-Za-z0-9_-]+)}
        return Regexp.last_match(1)  # URLからIDを取り出す
      end

      raise 'Spreadsheet ID not provided. Set LME_SPREADSHEET_ID, ONCLASS_SPREADSHEET_ID, LME_SPREADSHEET_URL, or LME_YAMADA_COUNT_SPREADSHEET_URL.'
    end

    # ==== デフォルト（0埋め） ===================================================
    def default_tag_monthlies
      Hash[TAG_MONTHLY_COL.keys.map { |k| [k, Array.new(12, 0)] }]
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
        Google::Apis::SheetsV4::BatchUpdateValuesRequest.new(value_input_option: 'USER_ENTERED', data: updates)
      )
    end

    def copy_to_yamada_sheet!(service, spreadsheet_id, dashboard_sheet_name)
      dashboard_range = "'#{dashboard_sheet_name}'!A1:Z100"  # ここでシート名と範囲を指定

      # ダッシュボードシートのデータを取得
      dashboard_data = service.get_spreadsheet_values(spreadsheet_id, dashboard_range)

      # 環境変数からスプレッドシートIDを取得
      target_spreadsheet_id = ENV['LME_YAMADA_COUNT_SPREADSHEET_ID']  # 環境変数からコピー先のIDを取得

      # 環境変数からシート名を取得
      target_sheet_name = ENV['LME_YAMADA_COUNT_SPREADSHEET_NAME']  # 環境変数からシート名を取得

      # コピー先のシートと範囲を指定
      target_range = "'#{target_sheet_name}'!A1:Z100"  # シート名と範囲を動的に指定

      # データをコピー先シートに書き込み
      service.update_spreadsheet_value(
        target_spreadsheet_id,
        target_range,
        Google::Apis::SheetsV4::ValueRange.new(values: dashboard_data.values),
        value_input_option: 'USER_ENTERED'
      )
    end
  end
end
