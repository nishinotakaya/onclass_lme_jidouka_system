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

  PROAKA_TAGS = {
    v1: 1_394_734, v2: 1_394_736, v3: 1_394_737, v4: 1_394_738
  }.freeze

  PROAKA_DIGEST_NAMES = {
    dv1: '動画①_ダイジェスト', dv2: '動画②_ダイジェスト', dv3: '動画③_ダイジェスト'
  }.freeze

  RICHMENU_SELECT_NAMES = [
    '月収40万円のエンジニアになれる方法を知りたい',
    'プログラミング無料体験したい',
    '現役エンジニアに質問したい'
  ].freeze

  # ==== Entry point ==========================================================
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
    days.each do |date|
      next if date.blank?
      detail = fetch_day_details(conn, date, auth: auth)
      Rails.logger.info("[LME] details for #{date}: #{detail.size} records")
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

  # 認証ヘッダーを設定する共通処理
  def apply_auth_headers!(conn, auth)
    return unless auth

    conn.headers['Cookie'] = auth.cookie
    if (xsrf = extract_cookie(auth.cookie, 'XSRF-TOKEN'))
      token = CGI.unescape(xsrf)
      conn.headers['x-csrf-token']     = token
      conn.headers['X-CSRF-TOKEN']     = token
      conn.headers['X-XSRF-TOKEN']     = token
      conn.headers['x-xsrf-token']     = token
      conn.headers['X-Requested-With'] = 'XMLHttpRequest'
    end
  end


  # 期間サマリ
  # 置換: fetch_friend_overview
  def fetch_friend_overview(conn, start_on, end_on, auth: nil)
    apply_auth_headers!(conn, auth)
    conn.headers['Referer'] = "#{LmeAuthClient::BASE_URL}/basic/friendlist/friend-history"
    conn.headers['Content-Type'] = 'application/json;charset=UTF-8'
    conn.headers['Accept'] = 'application/json, text/plain, */*'

    body = { data: { start: start_on, end: end_on }.to_json }
    resp = conn.post('/ajax/init-data-history-add-friend', body.to_json)
    auth&.refresh_from_response_cookies!(resp.headers)

    json = safe_json(resp.body)
    arr  = json['data'] || json['result'] || json['records'] || []
    arr  = arr['data'] if arr.is_a?(Hash) && arr['data'].is_a?(Array)
    arr
  rescue Faraday::Error => e
    Rails.logger.warn("[LME] overview failed: #{e.class} #{e.message}")
    []
  end

  # 日別詳細
  def fetch_day_details(conn, ymd, auth: nil)
    apply_auth_headers!(conn, auth)
    conn.headers['Referer'] = "#{LmeAuthClient::BASE_URL}/basic/overview/friendlist/view-date?date=#{ymd}"
    conn.headers['Content-Type'] = 'application/json;charset=UTF-8'
    conn.headers['Accept'] = 'application/json, text/plain, */*'

    body = { date: ymd, tab: 1 }
    resp = conn.post('/ajax/init-data-history-add-friend-by-date', body.to_json)
    auth&.refresh_from_response_cookies!(resp.headers)

    json = safe_json(resp.body)
    rv = json['result'] || json['data'] || json
    rv.is_a?(Array) ? rv : Array(rv)
  rescue Faraday::Error => e
    Rails.logger.warn("[LME] day_details failed for #{ymd}: #{e.class} #{e.message}")
    []
  end


  # タグ一覧（ユーザー単位）
  # 置換: タグ一覧（ユーザー単位）
  # タグ一覧（ユーザー単位）
  def fetch_user_categories_tags(conn, line_user_id, auth: nil, bot_id: nil, is_all_tag: 0)
    raise "auth required" unless auth

    # 1) chat-v3 を開いて meta csrf-token を取得
    referer_path = "/basic/chat-v3?lastTimeUpdateFriend=0"
    csrf = auth.csrf_token_for(referer_path)
    # フォールバック（最悪 Cookie の XSRF を URLデコードしたもの）
    csrf ||= CGI.unescape(extract_cookie(auth.cookie, 'XSRF-TOKEN').to_s)

    # 2) cURL と同じヘッダーを厳密に付与
    conn.headers["Cookie"]           = auth.cookie
    conn.headers["Accept"]           = "*/*"
    conn.headers["x-requested-with"] = "XMLHttpRequest"
    conn.headers["x-csrf-token"]     = csrf
    conn.headers["Referer"]          = "#{LmeAuthClient::BASE_URL}#{referer_path}"

    # 3) form-urlencoded で送る（キー名を cURL と同一に）
    form = URI.encode_www_form(
      line_user_id: line_user_id,
      is_all_tag:   is_all_tag,
      botIdCurrent: (bot_id || ENV['LME_BOT_ID'] || '17106').to_s
    )

    resp = conn.post('/basic/chat/get-categories-tags') do |req|
      req.headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8'
      req.body = form
    end
    auth.refresh_from_response_cookies!(resp.headers)

    json = safe_json(resp.body)
    arr  = json['data'] || json['result'] || []
    arr.is_a?(Array) ? arr : []
  rescue Faraday::Error => e
    Rails.logger.warn("[LME] fetch_user_categories_tags 404/err for #{line_user_id}: #{e.class} #{e.message}")
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
          title: new_sheet_name, index: desired_index
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
            sheet_id: target.properties.sheet_id, index: desired_index
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

  # === クリア→統計→ヘッダー→データ ==========================================
  def upload_to_gsheets!(service:, rows:, spreadsheet_id:, sheet_name:, tags_cache:, end_on:)
    clear_req = Google::Apis::SheetsV4::ClearValuesRequest.new
    %w[B2 B3 B4 B5 B6 B7].each do |r|
      service.clear_values(spreadsheet_id, "#{sheet_name}!#{r}:Z", clear_req)
    end

    meta_values = [['バッチ実行タイミング', jp_timestamp]]
    service.update_spreadsheet_value(
      spreadsheet_id, "#{sheet_name}!B2",
      Google::Apis::SheetsV4::ValueRange.new(values: meta_values),
      value_input_option: 'USER_ENTERED'
    )

    end_d   = (parse_date(end_on).to_date rescue Date.today)
    this_m  = end_d.strftime('%Y-%m')
    prev_m  = end_d.prev_month.strftime('%Y-%m')

    monthly_rates, cumulative_rates = calc_rates(rows, tags_cache, month: this_m, since: CUM_SINCE)
    prev_month_rates = month_rates(rows, tags_cache, month: prev_m)

    headers = [
      '友達追加時刻', '流入元', 'line_user_id', '名前', 'LINE_ID', 'ブロック?',
      '動画①_ダイジェスト', 'プロアカ_動画①',
      '動画②_ダイジェスト', 'プロアカ_動画②',
      '動画③_ダイジェスト', 'プロアカ_動画③',
      '', 'プロアカ_動画④', '選択肢'
    ]
    cols = headers.size

    row3 = Array.new(cols, ''); row4 = Array.new(cols, ''); row5 = Array.new(cols, '')
    row3[0] = "今月%（#{this_m}）"
    row4[0] = "前月%（#{prev_m}）"
    row5[0] = "累計%（#{Date.parse(CUM_SINCE).strftime('%Y/%-m')}〜）" rescue row5[0] = "累計%"
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

    header_range = "#{sheet_name}!B6:#{a1_col(1 + headers.size)}6"
    service.update_spreadsheet_value(
      spreadsheet_id, header_range,
      Google::Apis::SheetsV4::ValueRange.new(values: [headers]),
      value_input_option: 'USER_ENTERED'
    )

    sorted = Array(rows).sort_by do |r|
      [ r['date'].to_s, (Time.zone.parse(r['followed_at'].to_s) rescue r['followed_at'].to_s) ]
    end.reverse

    data_values = sorted.map do |r|
      t = tags_cache[r['line_user_id']] || {}
      [
        to_jp_ymdhm(r['followed_at']),
        r['landing_name'],
        r['line_user_id'],
        hyperlink_line_user(r['line_user_id'], r['name']),
        r['line_id'],
        r['is_blocked'].to_i,
        (t[:dv1] ? 'タグあり' : ''),
        (t[:v1]  ? 'タグあり' : ''),
        (t[:dv2] ? 'タグあり' : ''),
        (t[:v2]  ? 'タグあり' : ''),
        (t[:dv3] ? 'タグあり' : ''),
        (t[:v3]  ? 'タグあり' : ''),
        '',
        (t[:v4]  ? 'タグあり' : ''),
        (t[:select] || '')
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

  # ==== Helpers: タグ抽出/書式 ==============================================
  def month_rates(rows, tags_cache, month:)
    month_rows = Array(rows).select { |r| month_key(r['date']) == month }
    {
      v1: pct_for(month_rows, tags_cache, :v1),
      v2: pct_for(month_rows, tags_cache, :v2),
      v3: pct_for(month_rows, tags_cache, :v3),
      v4: pct_for(month_rows, tags_cache, :v4)
    }
  end

  def build_proaka_tags_cache(_conn, line_user_ids, auth:)
    ids = Array(line_user_ids).uniq
    cache = {}
    q = Queue.new
    ids.each { |id| q << id }

    workers = Integer(ENV['LME_TAG_THREADS'] || 6)
    threads = Array.new(workers) do
      Thread.new do
        local = auth.conn # スレッド専用 conn
        while (uid = q.pop(true) rescue nil)
          cats  = fetch_user_categories_tags(local, uid, auth: auth, bot_id: (ENV['LME_BOT_ID'] || '17106'))
          flags = proaka_flags_from_categories(cats)
          Thread.current[:count] = (Thread.current[:count] || 0) + 1
          cache[uid] = flags
        end
      rescue ThreadError
        # queue empty
      end
    end
    threads.each(&:join)
    cache
  end

  def proaka_flags_from_categories(categories)
    Rails.logger.info("[LME] proaka_flags_from_categories: input=#{categories}")
    
    target = Array(categories).find { |c| (c['id'] || c[:id]).to_i == PROAKA_CATEGORY_ID }
    Rails.logger.info("[LME] proaka_flags_from_categories: target category=#{target}")
    
    return { v1: false, v2: false, v3: false, v4: false, dv1: false, dv2: false, dv3: false, select: nil } unless target

    tag_list  = Array(target['tags'] || target[:tags])
    tag_ids   = tag_list.map  { |t| (t['tag_id'] || t[:tag_id]).to_i }.to_set
    tag_names = tag_list.map { |t| (t['name']   || t[:name]).to_s }.to_set

    Rails.logger.info("[LME] proaka_flags_from_categories: tag_ids=#{tag_ids}, tag_names=#{tag_names}")

    selected = RICHMENU_SELECT_NAMES.find { |nm| tag_names.include?(nm) }

    result = {
      v1:  tag_ids.include?(PROAKA_TAGS[:v1]),
      v2:  tag_ids.include?(PROAKA_TAGS[:v2]),
      v3:  tag_ids.include?(PROAKA_TAGS[:v3]),
      v4:  tag_ids.include?(PROAKA_TAGS[:v4]),
      dv1: tag_names.include?(PROAKA_DIGEST_NAMES[:dv1]),
      dv2: tag_names.include?(PROAKA_DIGEST_NAMES[:dv2]),
      dv3: tag_names.include?(PROAKA_DIGEST_NAMES[:dv3]),
      select: selected
    }
    
    Rails.logger.info("[LME] proaka_flags_from_categories: result=#{result}")
    result
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
