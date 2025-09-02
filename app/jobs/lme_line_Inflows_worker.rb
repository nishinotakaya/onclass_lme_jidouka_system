# app/workers/lme_line_inflows_worker.rb
# frozen_string_literal: true

require 'json'
require 'cgi'
require 'uri'
require 'set'
require 'faraday'
require 'active_support'
require 'active_support/core_ext'
require 'google/apis/sheets_v4'
require 'googleauth'

class LmeLineInflowsWorker
  include Sidekiq::Worker
  sidekiq_options queue: 'lme_line_inflows', retry: 3

  GOOGLE_SCOPE = [Google::Apis::SheetsV4::AUTH_SPREADSHEETS].freeze

  PROAKA_CATEGORY_ID = 5_180_568
  CUM_SINCE          = ENV['LME_CUM_SINCE'].presence || '2025-05-01' # 累計の起点

  # メイン動画タグ（ID固定で拾う）
  PROAKA_TAGS = {
    v1: 1_394_734, # プロアカ_動画①
    v2: 1_394_736, # プロアカ_動画②
    v3: 1_394_737, # プロアカ_動画③
    v4: 1_394_738  # プロアカ_動画④
  }.freeze

  # ダイジェストは “名前一致” 優先
  PROAKA_DIGEST_NAMES = {
    dv1: '動画①_ダイジェスト',
    dv2: '動画②_ダイジェスト',
    dv3: '動画③_ダイジェスト'
  }.freeze

  # ==== Entry point ==========================================================
  # start_date/end_date: "YYYY-MM-DD"
  # 省略時: ENV['LME_DEFAULT_START_DATE'] (既定 "2025-01-01") 〜 本日（JST）
  def perform(start_date = nil, end_date = nil)
    Time.zone = 'Asia/Tokyo'

    # cookie ウォームアップ（Redisに有効cookieがないと例外）
    LmeSessionWarmupWorker.new.perform
    auth = LmeAuthClient.new

    start_on = (start_date.presence || default_start_on)
    end_on   = (end_date.presence   || Time.zone.today.to_s)

    conn = auth.conn

    # 1) 期間サマリ → 増加があった日を抽出
    overview = fetch_friend_overview(conn, start_on, end_on, auth: auth)
    days =
      case overview
      when Hash
        overview.each_with_object([]) { |(date, stats), acc| acc << date if stats.to_h['followed'].to_i > 0 }.sort
      when Array
        overview.filter_map { |row|
          next unless row.is_a?(Hash)
          (row['followed'] || row[:followed]).to_i > 0 ? (row['date'] || row[:date]).to_s : nil
        }.sort
      else
        []
      end

    Rails.logger.info("[LME] days_need_detail=#{days.inspect}")

    # 2) 詳細取得
    rows = []
    days.each do |date|   # "YYYY-MM-DD"
      next if date.blank?
      detail = fetch_day_details(conn, date, auth: auth)
      Array(detail).each do |r|
        rec = r.respond_to?(:with_indifferent_access) ? r.with_indifferent_access : r
        lu  = (rec['line_user'] || {})
        lu  = lu.with_indifferent_access if lu.respond_to?(:with_indifferent_access)

        rows << {
          'date'         => date,
          'followed_at'  => rec['followed_at'],
          'landing_name' => rec['landing_name'],
          'name'         => lu['name'],
          'line_user_id' => rec['line_user_id'],
          'line_id'      => lu['line_id'],
          'is_blocked'   => (rec['is_blocked'] || 0).to_i
        }
      end
    end

    # 3) 重複除去
    rows.uniq! { |r| [r['line_user_id'], r['followed_at']] }

    # 3.5) タグ有無キャッシュ
    tags_cache = build_proaka_tags_cache(conn, rows.map { |r| r['line_user_id'] }.compact.uniq, auth: auth)

    # 4) シート更新
    spreadsheet_id = ENV.fetch('ONCLASS_SPREADSHEET_ID')
    sheet_name     = ENV.fetch('LME_SHEET_NAME', 'Line流入者')
    anchor_name    = ENV.fetch('ONCLASS_SHEET_NAME', '受講生自動化')

    service = build_sheets_service
    ensure_sheet_exists_adjacent!(service, spreadsheet_id, sheet_name, anchor_name)
    upload_to_gsheets!(
      service: service,
      rows: rows,
      spreadsheet_id: spreadsheet_id,
      sheet_name: sheet_name,
      tags_cache: tags_cache,
      end_on: end_on
    )

    Rails.logger.info("[LmeLineInflowsWorker] wrote #{rows.size} rows to #{sheet_name}")
    { count: rows.size, sheet: sheet_name, range: [start_on, end_on] }
  rescue Faraday::Error => e
    Rails.logger.error("[LmeLineInflowsWorker] HTTP error: #{e.class} #{e.message}")
    raise
  rescue => e
    Rails.logger.error("[LmeLineInflowsWorker] unexpected error: #{e.class} #{e.message}")
    raise
  end

  # ==== LME API =============================================================

  # 期間サマリ
  def fetch_friend_overview(conn, start_on, end_on, auth: nil)
    if auth
      conn.headers['Cookie'] = auth.cookie
      if (xsrf = extract_cookie(auth.cookie, 'XSRF-TOKEN'))
        conn.headers['X-XSRF-TOKEN'] = CGI.unescape(xsrf)
      end
    end
    body = { data: { start: start_on, end: end_on }.to_json }
    resp = conn.post('/ajax/init-data-history-add-friend', body.to_json)
    auth&.refresh_from_response_cookies!(resp.headers)

    json = safe_json(resp.body)
    arr = json['data'] || json['result'] || json['records'] || []
    arr = arr['data'] if arr.is_a?(Hash) && arr['data'].is_a?(Array)
    arr
  end

  # 日別詳細
  def fetch_day_details(conn, ymd, auth: nil)
    if auth
      conn.headers['Cookie'] = auth.cookie
      if (xsrf = extract_cookie(auth.cookie, 'XSRF-TOKEN'))
        conn.headers['X-XSRF-TOKEN'] = CGI.unescape(xsrf)
      end
    end

    body = { date: ymd, tab: 1 }
    resp = conn.post('/ajax/init-data-history-add-friend-by-date', body.to_json)
    auth&.refresh_from_response_cookies!(resp.headers)

    json = safe_json(resp.body)
    rv = json['result'] || json['data'] || json
    rv.is_a?(Array) ? rv : Array(rv)
  end

  # タグ一覧（ユーザー単位）
  def fetch_user_categories_tags(conn, line_user_id, auth: nil, bot_id: nil, is_all_tag: 0)
    if auth
      conn.headers['Cookie'] = auth.cookie
      if (xsrf = extract_cookie(auth.cookie, 'XSRF-TOKEN'))
        conn.headers['X-XSRF-TOKEN'] = CGI.unescape(xsrf)
      end
    end

    bot_id ||= (ENV['LME_BOT_ID'].presence || '17106').to_s
    form = URI.encode_www_form(
      line_user_id: line_user_id,
      is_all_tag: is_all_tag,
      botIdCurrent: bot_id
    )

    resp = conn.post('/basic/chat/get-categories-tags') do |req|
      req.headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8'
      req.body = form
    end
    auth&.refresh_from_response_cookies!(resp.headers)

    json = safe_json(resp.body)
    json['data'].is_a?(Array) ? json['data'] : []
  rescue => e
    Rails.logger.warn("[LME] fetch_user_categories_tags error for #{line_user_id}: #{e.class} #{e.message}")
    []
  end

  def safe_json(str) JSON.parse(str) rescue {} end

  # ==== Google Sheets =======================================================

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
    service.client_options.application_name = 'LME Line Inflows Uploader'
    service.authorization = auth
    service
  end

  # 「after_sheet_name」の直後に new_sheet_name を配置（存在すれば移動）
  def ensure_sheet_exists_adjacent!(service, spreadsheet_id, new_sheet_name, after_sheet_name)
    ss      = service.get_spreadsheet(spreadsheet_id)
    sheets  = ss.sheets
    target  = sheets.find { |s| s.properties&.title == new_sheet_name }
    anchor  = sheets.find { |s| s.properties&.title == after_sheet_name }

    if anchor.nil?
      return ensure_sheet_exists!(service, spreadsheet_id, new_sheet_name)
    end

    desired_index = anchor.properties.index.to_i + 1

    if target.nil?
      add_req = Google::Apis::SheetsV4::AddSheetRequest.new(
        properties: Google::Apis::SheetsV4::SheetProperties.new(
          title: new_sheet_name,
          index: desired_index
        )
      )
      batch = Google::Apis::SheetsV4::BatchUpdateSpreadsheetRequest.new(
        requests: [Google::Apis::SheetsV4::Request.new(add_sheet: add_req)]
      )
      service.batch_update_spreadsheet(spreadsheet_id, batch)
    else
      cur = target.properties.index.to_i
      if cur != desired_index
        update_req = Google::Apis::SheetsV4::UpdateSheetPropertiesRequest.new(
          properties: Google::Apis::SheetsV4::SheetProperties.new(
            sheet_id: target.properties.sheet_id,
            index: desired_index
          ),
          fields: 'index'
        )
        batch = Google::Apis::SheetsV4::BatchUpdateSpreadsheetRequest.new(
          requests: [Google::Apis::SheetsV4::Request.new(update_sheet_properties: update_req)]
        )
        service.batch_update_spreadsheet(spreadsheet_id, batch)
      end
    end
  end

  # フォールバックの単純 ensure
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

  # === クリア→統計(2-4行)→ヘッダー(5行)→データ(6行〜) =======================

  def upload_to_gsheets!(service:, rows:, spreadsheet_id:, sheet_name:, tags_cache:, end_on:)
    # クリア
    clear_req = Google::Apis::SheetsV4::ClearValuesRequest.new
    service.clear_values(spreadsheet_id, "#{sheet_name}!B2:Z", clear_req)
    service.clear_values(spreadsheet_id, "#{sheet_name}!B3:Z", clear_req)
    service.clear_values(spreadsheet_id, "#{sheet_name}!B4:Z", clear_req)
    service.clear_values(spreadsheet_id, "#{sheet_name}!B5:Z", clear_req)
    service.clear_values(spreadsheet_id, "#{sheet_name}!B6:Z", clear_req)
    service.clear_values(spreadsheet_id, "#{sheet_name}!B7:Z", clear_req)

    # 更新日時（B2）
    meta_values = [['更新日時', jp_timestamp]]
    service.update_spreadsheet_value(
      spreadsheet_id,
      "#{sheet_name}!B2",
      Google::Apis::SheetsV4::ValueRange.new(values: meta_values),
      value_input_option: 'USER_ENTERED'
    )

    # 当月 / 前月 / 累計（2025-07-01〜）の%を I/K/M/O に出力
    end_d   = (parse_date(end_on).to_date rescue Date.today)
    this_m  = end_d.strftime('%Y-%m')
    prev_m  = end_d.prev_month.strftime('%Y-%m')

    # 当月＆累計
    monthly_rates, cumulative_rates = calc_rates(rows, tags_cache, month: this_m, since: CUM_SINCE)
    # 前月
    prev_month_rates = month_rates(rows, tags_cache, month: prev_m)

    # B..O の14列配列（必要セルのみ書く）
    row3 = Array.new(14, '')
    row4 = Array.new(14, '')
    row5 = Array.new(14, '')

    row3[0] = "各月%（#{this_m}）"                                  # B3
    row4[0] = "前月%（#{prev_m}）"                                   # B4
    row5[0] = "累計%（#{Date.parse(CUM_SINCE).strftime('%Y/%-m')}〜）" rescue row5[0] = "累計%" # B5

    # I/K/M/O は B起点配列で 8,10,12,14 → 0-based: 7,9,11,13
    put_percentages!(row3, monthly_rates)
    put_percentages!(row4, prev_month_rates)
    put_percentages!(row5, cumulative_rates)

    service.update_spreadsheet_value(
      spreadsheet_id, "#{sheet_name}!B3",
      Google::Apis::SheetsV4::ValueRange.new(values: [row3]),
      value_input_option: 'USER_ENTERED'
    )
    service.update_spreadsheet_value(
      spreadsheet_id, "#{sheet_name}!B4",
      Google::Apis::SheetsV4::ValueRange.new(values: [row4]),
      value_input_option: 'USER_ENTERED'
    )
    service.update_spreadsheet_value(
      spreadsheet_id, "#{sheet_name}!B5",
      Google::Apis::SheetsV4::ValueRange.new(values: [row5]),
      value_input_option: 'USER_ENTERED'
    )

    # ヘッダー（B..O）を6行目へ
    headers = [
      '追加時刻', '流入元', 'line_user_id', '名前', 'LINE_ID', 'ブロック?',
      '動画①_ダイジェスト', 'プロアカ_動画①',
      '動画②_ダイジェスト', 'プロアカ_動画②',
      '動画③_ダイジェスト', 'プロアカ_動画③',
      '', 'プロアカ_動画④'
    ]
    header_range = "#{sheet_name}!B6:#{a1_col(1 + headers.size)}6" # -> O6
    service.update_spreadsheet_value(
      spreadsheet_id, header_range,
      Google::Apis::SheetsV4::ValueRange.new(values: [headers]),
      value_input_option: 'USER_ENTERED'
    )

    # 並び替え: 日付 + 追加時刻 DESC
    sorted = Array(rows).sort_by do |r|
      [ r['date'].to_s, (Time.zone.parse(r['followed_at'].to_s) rescue r['followed_at'].to_s) ]
    end.reverse

    # データ（B..O の 14 列ぶん）を7行目から
    data_values = sorted.map do |r|
      t = tags_cache[r['line_user_id']] || {}
      [
        to_jp_ymdhm(r['followed_at']),            # B
        r['landing_name'],                        # C
        r['line_user_id'],                        # D
        hyperlink_line_user(r['line_user_id'], r['name']), # E
        r['line_id'],                             # F
        r['is_blocked'].to_i,                     # G
        (t[:dv1] ? 'タグあり' : ''),              # H
        (t[:v1]  ? 'タグあり' : ''),              # I
        (t[:dv2] ? 'タグあり' : ''),              # J
        (t[:v2]  ? 'タグあり' : ''),              # K
        (t[:dv3] ? 'タグあり' : ''),              # L
        (t[:v3]  ? 'タグあり' : ''),              # M
        '',                                       # N (予備)
        (t[:v4]  ? 'タグあり' : '')               # O
      ]
    end

    if data_values.any?
      data_range = "#{sheet_name}!B7"
      service.update_spreadsheet_value(
        spreadsheet_id, data_range,
        Google::Apis::SheetsV4::ValueRange.new(values: data_values),
        value_input_option: 'USER_ENTERED'
      )
    end
  end

  # ==== Helpers: 集計 =======================================================

  # rows と tags_cache から、指定月の各%と累計%を返す
  def calc_rates(rows, tags_cache, month:, since:)
    month_rows = Array(rows).select { |r| month_key(r['date']) == month }
    cum_rows   = Array(rows).select { |r| r['date'].to_s >= since.to_s }

    monthly = {
      v1: pct_for(month_rows, tags_cache, :v1),
      v2: pct_for(month_rows, tags_cache, :v2),
      v3: pct_for(month_rows, tags_cache, :v3),
      v4: pct_for(month_rows, tags_cache, :v4)
    }
    cumulative = {
      v1: pct_for(cum_rows, tags_cache, :v1),
      v2: pct_for(cum_rows, tags_cache, :v2),
      v3: pct_for(cum_rows, tags_cache, :v3),
      v4: pct_for(cum_rows, tags_cache, :v4)
    }
    [monthly, cumulative]
  end

  # 指定キー(:v1..:v4)の“タグあり”割合（% 小数1桁）
  def pct_for(rows, tags_cache, key)
    denom = rows.size
    return nil if denom.zero?
    numer = rows.count { |r| (tags_cache[r['line_user_id']] || {})[key] }
    ((numer.to_f / denom) * 100).round(1)
  end

  def month_key(ymd_str)
    Date.parse(ymd_str.to_s).strftime('%Y-%m') rescue nil
  end

  # I/K/M/O に%を入れる（B基準0-indexで 7/9/11/13）
  def put_percentages!(row_array, rates)
    idx_map = { v1: 7, v2: 9, v3: 11, v4: 13 }
    idx_map.each do |k, idx|
      v = rates[k]
      row_array[idx] = v.nil? ? '' : "#{v}%"
    end
  end

  # ==== Helpers: タグ抽出/書式 =================================================
  # 
  ## 指定「月」(:YYYY-MM) の“タグあり”割合（% 小数1桁）を返す
  def month_rates(rows, tags_cache, month:)
    month_rows = Array(rows).select { |r| month_key(r['date']) == month }
    {
      v1: pct_for(month_rows, tags_cache, :v1),
      v2: pct_for(month_rows, tags_cache, :v2),
      v3: pct_for(month_rows, tags_cache, :v3),
      v4: pct_for(month_rows, tags_cache, :v4)
    }
  end


  def build_proaka_tags_cache(conn, line_user_ids, auth:)
    bot_id = (ENV['LME_BOT_ID'].presence || '17106').to_s
    cache = {}
    line_user_ids.each do |uid|
      cats = fetch_user_categories_tags(conn, uid, auth: auth, bot_id: bot_id)
      cache[uid] = proaka_flags_from_categories(cats)
    end
    cache
  end

  def proaka_flags_from_categories(categories)
    target = Array(categories).find { |c| (c['id'] || c[:id]).to_i == PROAKA_CATEGORY_ID }
    return { v1: false, v2: false, v3: false, v4: false, dv1: false, dv2: false, dv3: false } unless target

    tag_list  = Array(target['tags'] || target[:tags])
    tag_ids   = tag_list.map  { |t| (t['tag_id'] || t[:tag_id]).to_i }.to_set
    tag_names = tag_list.map { |t| (t['name']   || t[:name]).to_s }.to_set

    {
      v1:  tag_ids.include?(PROAKA_TAGS[:v1]),
      v2:  tag_ids.include?(PROAKA_TAGS[:v2]),
      v3:  tag_ids.include?(PROAKA_TAGS[:v3]),
      v4:  tag_ids.include?(PROAKA_TAGS[:v4]),
      dv1: tag_names.include?(PROAKA_DIGEST_NAMES[:dv1]),
      dv2: tag_names.include?(PROAKA_DIGEST_NAMES[:dv2]),
      dv3: tag_names.include?(PROAKA_DIGEST_NAMES[:dv3])
    }
  end

  def hyperlink_line_user(id, name)
    return name.to_s if id.to_s.strip.empty?
    label = name.to_s.gsub('"', '""')
    url   = "https://step.lme.jp/basic/friendlist/my_page/#{id}"
    %Q(=HYPERLINK("#{url}","#{label}"))
  end

  def jp_timestamp
    Time.now.in_time_zone('Asia/Tokyo').strftime('%Y年%-m月%-d日 %H時%M分')
  end

  def parse_date(str)
    Time.zone.parse(str.to_s) || Time.parse(str.to_s)
  end

  # "2025-08-29 00:45:36" → "2025年8月29日 00時45分"
  def to_jp_ymdhm(str)
    return '' if str.blank?
    t = (Time.zone.parse(str.to_s) rescue Time.parse(str.to_s) rescue nil)
    return '' unless t
    t.in_time_zone('Asia/Tokyo').strftime('%Y年%-m月%-d日 %H時%M分')
  end

  # 1-based index → A1列名
  def a1_col(n)
    s = String.new
    while n && n > 0
      n, r = (n - 1).divmod(26)
      s.prepend((65 + r).chr)
    end
    s
  end

  # Cookie 文字列から特定キーの値を抜く（XSRF更新用）
  def extract_cookie(cookie_str, key)
    return nil if cookie_str.blank?
    cookie_str.split(';').map(&:strip).each do |pair|
      k, v = pair.split('=', 2)
      return v if k == key
    end
    nil
  end

  private

  def default_start_on
    raw = ENV['LME_DEFAULT_START_DATE'].presence || '2025-01-01'
    Date.parse(raw).strftime('%F')
  rescue
    '2024-01-01'
  end
end
