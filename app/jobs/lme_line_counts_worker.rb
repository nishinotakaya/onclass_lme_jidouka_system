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
class LmeLineCountsWorker
  include Sidekiq::Worker
  sidekiq_options queue: 'lme_line_counts', retry: 3

  GOOGLE_SCOPE = [Google::Apis::SheetsV4::AUTH_SPREADSHEETS].freeze

  # ==== 書き込み定義（ダッシュボード） =======================================
  TOTAL_CELL    = 'D5'.freeze
  MONTHLY_RANGE = 'D10:D21'.freeze # 1〜12月 縦並び（友だち総数）

  # 出力先のラベル（4行目）
  TAG_LABEL_MAP = {
    'G4' => '動画1ダイジェスト', # ソース: H列（動画①_ダイジェスト）
    'I4' => '動画1',             # ソース: I列（プロアカ_動画①）
    'K4' => '動画2',             # ソース: K列（プロアカ_動画②）
    'M4' => '動画3',             # ソース: M列（プロアカ_動画③）
    'O4' => '動画4'              # ソース: O列（プロアカ_動画④）
  }.freeze

  TAG_TOTAL_ROWS = 5 # 各タグ総数
  TAG_RATE_ROWS  = 6 # 各タグ移行率(%)

  # タグ別「月別件数」の書き込み先列（10〜21行）
  TAG_MONTHLY_COL = {
    '動画1ダイジェスト' => 'F', # ← G列ラベルの左
    '動画1'             => 'H',
    '動画2'             => 'J',
    '動画3'             => 'L',
    '動画4'             => 'N'
  }.freeze

  # ==== エントリポイント =====================================================
  def perform(spreadsheet_id = nil, source_sheet_name = nil, dashboard_sheet_name = nil, target_year = nil)
    Time.zone = 'Asia/Tokyo'

    spreadsheet_id       ||= resolve_spreadsheet_id_from_env!
    dashboard_sheet_name ||= ENV['LME_DASHBOARD_SHEET_NAME'].presence || 'ダッシュボード'
    target_year = (target_year.presence || 2025).to_i

    service = build_sheets_service

    # --- 読み元シート名を決定（"Line流入者" を最優先で find） -----------------
    source_sheet_name ||= resolve_source_sheet_title(service, spreadsheet_id)

    # --- 1) ヘッダ行だけを安全に取得（B6:ZZ6） --------------------------------
    header_range = a1(source_sheet_name, 'B6:ZZ6')
    header_vr = service.get_spreadsheet_values(spreadsheet_id, header_range)
    header = Array(header_vr.values).first || []
    if header.blank?
      Rails.logger.info("[LmeLineCountsWorker] header empty at #{header_range}")
      write_dashboard!(service, spreadsheet_id, dashboard_sheet_name, target_year, 0, Array.new(12, 0), {}, {})
      write_tag_monthlies!(service, spreadsheet_id, dashboard_sheet_name, default_tag_monthlies)
      return
    end

    # 動的にデータ範囲決定（ヘッダの実カラム数ぶん）
    last_col_index  = header.size # B 起点の個数
    last_col_letter = a1_col(('B'.ord - 'A'.ord) + last_col_index) # B から header.size 分
    data_range = a1(source_sheet_name, "B7:#{last_col_letter}100000")

    vr = service.get_spreadsheet_values(spreadsheet_id, data_range)
    values = Array(vr.values)
    if values.blank?
      Rails.logger.info("[LmeLineCountsWorker] no rows at #{data_range}")
      write_dashboard!(service, spreadsheet_id, dashboard_sheet_name, target_year, 0, Array.new(12, 0), {}, {})
      write_tag_monthlies!(service, spreadsheet_id, dashboard_sheet_name, default_tag_monthlies)
      return
    end

    # --- 2) ヘッダ名 → インデックス解決（ゆれ対応） ---------------------------
    header_map = build_header_index_map(header)

    idx_follow = index_for_followed_at(header_map)
    if idx_follow.nil?
      raise "Source header not found: 友達追加時刻/日時 @ #{source_sheet_name}!B6"
    end

    # タグ列インデックス（① / 1 のゆれ吸収）
    idx_tags = {
      '動画1ダイジェスト' => index_for(header_map, ['動画①_ダイジェスト', '動画1_ダイジェスト', '動画1ダイジェスト']),
      '動画1'             => index_for(header_map, ['プロアカ_動画①', 'プロアカ_動画1', '動画①', '動画1']),
      '動画2'             => index_for(header_map, ['プロアカ_動画②', 'プロアカ_動画2', '動画②', '動画2']),
      '動画3'             => index_for(header_map, ['プロアカ_動画③', 'プロアカ_動画3', '動画③', '動画3']),
      '動画4'             => index_for(header_map, ['プロアカ_動画④', 'プロアカ_動画4', '動画④', '動画4'])
    }

    # --- 3) 対象年の行だけ抽出 -----------------------------------------------
    rows_target_year = []
    values.each do |row|
      fa = safe_at(row, idx_follow)
      t  = parse_followed_at(fa)
      next unless t && t.year == target_year
      rows_target_year << row
    end

    total = rows_target_year.size

    # --- 4) 友だち「月別」総数 ------------------------------------------------
    monthly_total = Array.new(12, 0)
    rows_target_year.each do |row|
      t = parse_followed_at(safe_at(row, idx_follow))
      next unless t
      monthly_total[t.month - 1] += 1
    end

    # --- 5) タグ別 総数 & 移行率 ---------------------------------------------
    tag_totals = {}
    tag_rates  = {}
    idx_tags.each do |out_label, idx|
      cnt = 0
      if idx
        rows_target_year.each do |row|
          v = safe_at(row, idx)
          cnt += 1 if v.to_s.include?('タグあり')
        end
      end
      tag_totals[out_label] = cnt
      tag_rates[out_label]  = total.positive? ? ((cnt.to_f / total) * 100).round(1) : ''
    end

    # --- 6) タグ別「月別」件数 ------------------------------------------------
    tag_monthlies = default_tag_monthlies
    rows_target_year.each do |row|
      t = parse_followed_at(safe_at(row, idx_follow))
      next unless t
      m_idx = t.month - 1
      idx_tags.each do |out_label, idx|
        next unless idx
        v = safe_at(row, idx)
        tag_monthlies[out_label][m_idx] += 1 if v.to_s.include?('タグあり')
      end
    end

    # --- 7) 動画4クリック数（P列）集計 ----------------------------------------
    video4_counts = Array.new(12, 0)
    q_start_idx = header_map['Q'] # Q列以降から
    rows_target_year.each do |row|
      t = parse_followed_at(safe_at(row, idx_follow))
      next unless t
      m_idx = t.month - 1
      q_columns = row[q_start_idx..] # Q列以降
      q_columns.each do |value|
        video4_counts[m_idx] += 1 if value.to_s.include?("参加")
      end
    end

    # --- 8) 流入経路（D列の数）集計 -------------------------------------------
    referrers = {
      '小松' => 'E', '西野' => 'G', '加藤' => 'I', '西野 ショート' => 'M',
      'YouTube概要欄' => 'O', 'YouTubeTop' => 'Q'
    }

    referrer_counts = Hash.new { |h, k| h[k] = Array.new(12, 0) }
    idx_referrer = header_map['流入経路'] # D列のインデックス（流入経路）
    rows_target_year.each do |row|
      t = parse_followed_at(safe_at(row, idx_follow))
      next unless t
      m_idx = t.month - 1
      referrer_value = safe_at(row, idx_referrer)
      referrers.each do |referrer, col|
        referrer_counts[referrer][m_idx] += 1 if referrer_value.to_s.include?(referrer)
      end
    end

    # --- ログ -----------------------------------------------------------------
    Rails.logger.info("[Counts] year=#{target_year} total=#{total}")
    Rails.logger.info("[Counts] monthly total=#{monthly_total.each_with_index.map { |c,i| "#{i+1}月:#{c}" }.join(', ')}")
    Rails.logger.info("[Counts] tag totals=#{tag_totals}")
    Rails.logger.info("[Counts] tag rates(%)=#{tag_rates}")
    Rails.logger.info("[Counts] video4 counts=#{video4_counts}")
    Rails.logger.info("[Counts] referrer counts=#{referrer_counts}")

    # --- 9) 書き込み（指定セルのみ） ------------------------------------------
    ensure_sheet_exists!(service, spreadsheet_id, dashboard_sheet_name)
    write_dashboard!(service, spreadsheet_id, dashboard_sheet_name, target_year, total, monthly_total, tag_totals, tag_rates)
    write_tag_monthlies!(service, spreadsheet_id, dashboard_sheet_name, tag_monthlies)
    write_video4_counts!(service, spreadsheet_id, dashboard_sheet_name, video4_counts)
    write_referrer_counts!(service, spreadsheet_id, dashboard_sheet_name, referrer_counts)
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

    titles.include?('Lme集計') ? 'Lme集計' : titles.first
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

  # ==== ダッシュボード書き込み（総数・月別・ラベル・総数/率） ==================
  def write_dashboard!(service, spreadsheet_id, dashboard_sheet_name, _target_year, total_friends, monthly, tag_totals, tag_rates = nil)
    # 総数
    service.update_spreadsheet_value(
      spreadsheet_id,
      a1(dashboard_sheet_name, TOTAL_CELL),
      Google::Apis::SheetsV4::ValueRange.new(values: [[total_friends.to_i]]),
      value_input_option: 'USER_ENTERED'
    )

    # 月別（友だち総数）
    service.update_spreadsheet_value(
      spreadsheet_id,
      a1(dashboard_sheet_name, MONTHLY_RANGE),
      Google::Apis::SheetsV4::ValueRange.new(values: monthly.map { |v| [v.to_i] }),
      value_input_option: 'USER_ENTERED'
    )

    # ラベル（4行目）
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

    # 総数（5行目）・率（6行目）
    total_updates = []
    rate_updates  = []
    TAG_LABEL_MAP.each do |cell, out_label|
      col = cell.gsub(/\d+/, '')
      total_cell = "#{col}#{TAG_TOTAL_ROWS}"
      rate_cell  = "#{col}#{TAG_RATE_ROWS}"

      total_val = tag_totals[out_label].to_i
      rate_val  = tag_rates[out_label]
      rate_val  = '' if rate_val.nil?

      total_updates << Google::Apis::SheetsV4::ValueRange.new(
        range: a1(dashboard_sheet_name, total_cell),
        values: [[total_val]]
      )
      rate_updates << Google::Apis::SheetsV4::ValueRange.new(
        range: a1(dashboard_sheet_name, rate_cell),
        values: [[rate_val]]
      )
    end
    service.batch_update_values(
      spreadsheet_id,
      Google::Apis::SheetsV4::BatchUpdateValuesRequest.new(
        value_input_option: 'USER_ENTERED',
        data: total_updates
      )
    )
    service.batch_update_values(
      spreadsheet_id,
      Google::Apis::SheetsV4::BatchUpdateValuesRequest.new(
        value_input_option: 'USER_ENTERED',
        data: rate_updates
      )
    )
  end

  # ==== タグ別 月別件数 書き込み（F/H/J/L/N の 10〜21 行） ====================
  def write_tag_monthlies!(service, spreadsheet_id, dashboard_sheet_name, tag_monthlies)
    updates = []

    tag_monthlies.each do |label, arr|
      col = TAG_MONTHLY_COL[label]
      next unless col
      range = a1(dashboard_sheet_name, "#{col}10:#{col}21")
      updates << Google::Apis::SheetsV4::ValueRange.new(
        range: range,
        values: arr.map { |v| [v.to_i] }
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

  # ==== デフォルト（0埋め） ===================================================
  def default_tag_monthlies
    Hash[
      TAG_MONTHLY_COL.keys.map { |k| [k, Array.new(12, 0)] }
    ]
  end

  # ==== 動画4クリック数 書き込み -------------------------------------------
  def write_video4_counts!(service, spreadsheet_id, dashboard_sheet_name, counts)
    service.update_spreadsheet_value(
      spreadsheet_id,
      a1(dashboard_sheet_name, 'P10:P21'),
      Google::Apis::SheetsV4::ValueRange.new(values: counts.map { |v| [v.to_i] }),
      value_input_option: 'USER_ENTERED'
    )
  end

  # ==== 流入経路 書き込み -------------------------------------------
  def write_referrer_counts!(service, spreadsheet_id, dashboard_sheet_name, counts)
    referrers = {
      '小松' => 'E', '西野' => 'G', '加藤' => 'I', '西野 ショート' => 'M',
      'YouTube概要欄' => 'O', 'YouTubeTop' => 'Q'
    }

    counts.each do |referrer, count|
      service.update_spreadsheet_value(
        spreadsheet_id,
        a1(dashboard_sheet_name, "#{referrers[referrer]}10:#{referrers[referrer]}21"),
        Google::Apis::SheetsV4::ValueRange.new(values: count.map { |v| [v.to_i] }),
        value_input_option: 'USER_ENTERED'
      )
    end
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
end
