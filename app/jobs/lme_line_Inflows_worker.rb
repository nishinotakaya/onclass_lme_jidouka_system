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
  PROAKA_TAGS = {
    v1: 1_394_734, # プロアカ_動画① -> J
    v2: 1_394_736, # プロアカ_動画② -> K
    v3: 1_394_737, # プロアカ_動画③ -> M
    v4: 1_394_738  # プロアカ_動画④ -> O
  }.freeze

  # ==== Entry point ==========================================================
  # start_date/end_date: "YYYY-MM-DD"
  # 省略時: ENV['LME_DEFAULT_START_DATE'] (既定 "2025-01-01") 〜 本日（JST）
  def perform(start_date = nil, end_date = nil)
    Time.zone = 'Asia/Tokyo'

    # Redisにcookieがあり有効かをウォームアップ（無ければ例外）
    LmeSessionWarmupWorker.new.perform
    auth = LmeAuthClient.new

    start_on = (start_date.presence || default_start_on)
    end_on   = (end_date.presence   || Time.zone.today.to_s)

    # LME接続（ベースヘッダは LmeAuthClient で付与）
    conn = auth.conn

    # 1) 期間の「友達履歴サマリ」を取得
    overview = fetch_friend_overview(conn, start_on, end_on, auth: auth)

    days =
      case overview
      when Hash
        overview.each_with_object([]) do |(date, stats), acc|
          acc << date if stats.to_h['followed'].to_i > 0
        end.sort
      when Array
        overview.filter_map { |row|
          next unless row.is_a?(Hash)
          (row['followed'] || row[:followed]).to_i > 0 ? (row['date'] || row[:date]).to_s : nil
        }.sort
      else
        []
      end

    Rails.logger.info("[LME] days_need_detail=#{days.inspect}")

    # 2) 増加があった日の詳細を深掘り
    rows = []
    days.each do |date|   # date は "YYYY-MM-DD"
      next if date.blank?

      detail = fetch_day_details(conn, date, auth: auth) # Arrayを返す前提
      Array(detail).each do |r|
        rec = r.respond_to?(:with_indifferent_access) ? r.with_indifferent_access : r
        lu  = (rec['line_user'] || {})
        lu  = lu.with_indifferent_access if lu.respond_to?(:with_indifferent_access)

        line_user_id = rec['line_user_id']
        rows << {
          'date'         => date,
          'followed_at'  => rec['followed_at'],
          'landing_name' => rec['landing_name'],
          'name'         => lu['name'],
          'line_user_id' => line_user_id,
          'line_id'      => lu['line_id'],
          'is_blocked'   => (rec['is_blocked'] || 0).to_i
        }
      end
    end

    # 3) 重複除去（line_user_id + followed_at）
    rows.uniq! { |r| [r['line_user_id'], r['followed_at']] }

    # 3.5) 各 line_user_id ごとに「プロアカ動画タグ」有無を取得（キャッシュ）
    tags_cache = build_proaka_tags_cache(conn, rows.map { |r| r['line_user_id'] }.compact.uniq, auth: auth)

    # 4) スプレッドシートへ書き込み（アンカー「受講生自動化」の隣）
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
      tags_cache: tags_cache
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
  # POST /ajax/init-data-history-add-friend
  def fetch_friend_overview(conn, start_on, end_on, auth: nil)
    # 毎回、最新CookieとXSRFを反映
    if auth
      conn.headers['Cookie'] = auth.cookie
      if (xsrf = extract_cookie(auth.cookie, 'XSRF-TOKEN'))
        conn.headers['X-XSRF-TOKEN'] = CGI.unescape(xsrf)
      end
    end

    body = { data: { start: start_on, end: end_on }.to_json }
    resp = conn.post('/ajax/init-data-history-add-friend', body.to_json)

    # Set-Cookie 取り込み
    auth&.refresh_from_response_cookies!(resp.headers)

    json = safe_json(resp.body)
    arr = json['data'] || json['result'] || json['records'] || []
    arr = arr['data'] if arr.is_a?(Hash) && arr['data'].is_a?(Array)
    arr
  end

  # 日別詳細
  # POST /ajax/init-data-history-add-friend-by-date {date:"YYYY-MM-DD", tab:1}
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
  # POST /basic/chat/get-categories-tags (x-www-form-urlencoded)
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

  def safe_json(str)
    JSON.parse(str) rescue {}
  end

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

  # === 更新（クリア→全書き） + タグ列 (J/K/M/O) ===========================

  def upload_to_gsheets!(service:, rows:, spreadsheet_id:, sheet_name:, tags_cache:)
    # クリア
    clear_req = Google::Apis::SheetsV4::ClearValuesRequest.new
    service.clear_values(spreadsheet_id, "#{sheet_name}!B2:Z", clear_req)
    service.clear_values(spreadsheet_id, "#{sheet_name}!B3:Z", clear_req)
    service.clear_values(spreadsheet_id, "#{sheet_name}!B4:Z", clear_req)

    # 更新日時（B2）
    meta_values = [['更新日時', jp_timestamp]]
    service.update_spreadsheet_value(
      spreadsheet_id,
      "#{sheet_name}!B2",
      Google::Apis::SheetsV4::ValueRange.new(values: meta_values),
      value_input_option: 'USER_ENTERED'
    )

    # ヘッダー（B..O）
    # B:追加時刻, C:流入元, D:line_user_id, E:名前, F:LINE_ID, G:ブロック?
    # H:空, I:空, J:プロアカ_動画①, K:プロアカ_動画②, L:空, M:プロアカ_動画③, N:空, O:プロアカ_動画④
    headers = ['追加時刻', '流入元', 'line_user_id', '名前', 'LINE_ID', 'ブロック?', '', '', 'プロアカ_動画①', 'プロアカ_動画②', '', 'プロアカ_動画③', '', 'プロアカ_動画④']
    header_range = "#{sheet_name}!B3:#{a1_col(1 + headers.size)}3" # -> O列まで
    service.update_spreadsheet_value(
      spreadsheet_id,
      header_range,
      Google::Apis::SheetsV4::ValueRange.new(values: [headers]),
      value_input_option: 'USER_ENTERED'
    )

    # 並び替え: 日付 + 追加時刻 DESC
    sorted = Array(rows).sort_by do |r|
      [
        r['date'].to_s,
        (Time.zone.parse(r['followed_at'].to_s) rescue r['followed_at'].to_s)
      ]
    end.reverse

    # データ（B..O の 14 列ぶん）
    data_values = sorted.map do |r|
      t = tags_cache[r['line_user_id']] || {}
      [
        to_jp_ymdhm(r['followed_at']), # B
        r['landing_name'],            # C
        r['line_user_id'],            # D
        hyperlink_line_user(r['line_user_id'], r['name']), # E
        r['line_id'],                 # F
        r['is_blocked'].to_i,         # G
        '',                           # H
        '',                           # I
        (t[:v1] ? '1' : ''),          # J: 動画①
        (t[:v2] ? '1' : ''),          # K: 動画②
        '',                           # L
        (t[:v3] ? '1' : ''),          # M: 動画③
        '',                           # N
        (t[:v4] ? '1' : '')           # O: 動画④
      ]
    end

    if data_values.any?
      data_range = "#{sheet_name}!B4"
      service.update_spreadsheet_value(
        spreadsheet_id,
        data_range,
        Google::Apis::SheetsV4::ValueRange.new(values: data_values),
        value_input_option: 'USER_ENTERED'
      )
    end
  end

  # ==== Helpers =============================================================

  # line_user_id 一覧に対して、プロアカ動画タグの有無をまとめて取得
  def build_proaka_tags_cache(conn, line_user_ids, auth:)
    bot_id = (ENV['LME_BOT_ID'].presence || '17106').to_s
    cache = {}

    line_user_ids.each do |uid|
      cats = fetch_user_categories_tags(conn, uid, auth: auth, bot_id: bot_id)
      cache[uid] = proaka_flags_from_categories(cats)
    end

    cache
  end

  # categories(JSON) から、プロアカ動画①〜④の有無を抽出
  def proaka_flags_from_categories(categories)
    target = Array(categories).find { |c| (c['id'] || c[:id]).to_i == PROAKA_CATEGORY_ID }
    return { v1: false, v2: false, v3: false, v4: false } unless target

    tag_list = Array(target['tags'] || target[:tags])
    tag_ids  = tag_list.map { |t| (t['tag_id'] || t[:tag_id]).to_i }.to_set

    {
      v1: tag_ids.include?(PROAKA_TAGS[:v1]),
      v2: tag_ids.include?(PROAKA_TAGS[:v2]),
      v3: tag_ids.include?(PROAKA_TAGS[:v3]),
      v4: tag_ids.include?(PROAKA_TAGS[:v4])
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

  # デフォルト開始日（ENV優先、なければ 2025-01-01）
  def default_start_on
    raw = ENV['LME_DEFAULT_START_DATE'].presence || '2025-01-01'
    Date.parse(raw).strftime('%F')
  rescue
    '2025-01-01'
  end
end
