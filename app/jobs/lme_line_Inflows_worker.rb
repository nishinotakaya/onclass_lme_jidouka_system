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
require "selenium-webdriver"
require "net/http"
require "selenium/devtools"
require "playwright"

class LmeLineInflowsWorker
  include Sidekiq::Worker
  sidekiq_options queue: 'lme_line_inflows', retry: 3

  GOOGLE_SCOPE = [Google::Apis::SheetsV4::AUTH_SPREADSHEETS].freeze
  CUM_SINCE    = ENV['LME_CUM_SINCE'].presence || '2025-05-01'
  UA           = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
  ORIGIN       = 'https://step.lme.jp'

  # =========================================================
  # メイン
  # =========================================================
  def perform(start_date = nil, end_date = nil)
    # 1) Seleniumでログイン → Cookie取得（サービスは既存のまま）
    login_service = LmeLoginUserService.new(
      email:    ENV["GOOGLE_EMAIL"],
      password: ENV["GOOGLE_PASSWORD"],
      api_key:  ENV["API2CAPTCHA_KEY"]
    )
    login_result = login_service.fetch_friend_history

    auth_client = LmeAuthClient.new(login_result[:driver])
    auth_client.manual_set!(login_result[:cookie_str], login_result[:cookies])

    start_on = (start_date.presence || default_start_on)
    end_on   = (end_date.presence   || Time.zone.today.to_s)
    bot_id   = (ENV['LME_BOT_ID'].presence || '17106').to_s

    # 2) Playwright: /basic 側Cookie育成 & XSRF 取得
    cookie_header = nil
    xsrf         = nil

    Playwright.create(playwright_cli_executable_path: "npx playwright") do |pw|
      launched_browser = pw.chromium.launch(
        headless: true,
        args: ["--no-sandbox", "--disable-dev-shm-usage"]
      )

      connected_browser = nil
      begin
        connected_browser = pw.chromium.connect_over_cdp("http://localhost:9222")
      rescue
      end

      context =
        if connected_browser && !connected_browser.contexts.empty?
          connected_browser.contexts.first
        else
          launched_browser.new_context
        end

      begin
        # Selenium の Cookie を注入（バージョン差分に両対応）
        add_cookies_to_context!(context, login_result[:cookies])

        page = context.new_page
        pw_add_init_script(page, "window.open = (url, target) => { location.href = url; }")

        # 管理TOPへ
        page.goto("#{ORIGIN}/admin/home")
        pw_wait_networkidle(page)

        # /basic を踏んで cookie & XSRF 生やす
        begin
          page.goto("#{ORIGIN}/basic/overview?botIdCurrent=#{bot_id}&isOtherBot=1")
          pw_wait_networkidle(page)
        rescue => e
          Rails.logger.debug("[BOT] skip basic/overview goto: #{e.class} #{e.message}")
        end

        begin
          page.goto("#{ORIGIN}/basic/friendlist/friend-history?botIdCurrent=#{bot_id}&isOtherBot=1")
          pw_wait_for_url(page, %r{/basic/friendlist/friend-history}, 15_000)
          pw_wait_networkidle(page)
        rescue => e
          Rails.logger.debug("[BOT] friend-history goto warn: #{e.class} #{e.message}")
        end

        # Playwright 側の Cookie を抜き出し → Header/XSRF 生成
        pl_cookies = ctx_cookies(context, "step.lme.jp")
        names = pl_cookies.map { |c| (c["name"] || c[:name]).to_s }
        Rails.logger.debug("[PW] cookie names: #{names.join(', ')}")

        unless names.include?("laravel_session") && names.include?("XSRF-TOKEN")
          raise "Playwright に laravel_session / XSRF-TOKEN が載っていません（未ログイン）"
        end

        cookie_header = pl_cookies.map { |c|
          "#{(c["name"]||c[:name])}=#{(c["value"]||c[:value])}"
        }.join("; ")

        xsrf_cookie = pl_cookies.find { |c| (c["name"] || c[:name]) == "XSRF-TOKEN" }
        xsrf_raw    = xsrf_cookie && (xsrf_cookie["value"] || xsrf_cookie[:value])
        xsrf        = xsrf_raw && CGI.unescape(xsrf_raw.to_s)
      ensure
        context&.close rescue nil
        connected_browser&.close rescue nil
        launched_browser&.close rescue nil
      end
    end

    # 3) Faraday へセッション適用（既存メソッド）
    auth_client.apply_session!(cookie_header, xsrf,
      referer: "#{LmeAuthClient::BASE_URL}/basic/friendlist/friend-history"
    )
    conn = auth_client.conn

    # 4) <<< ここが「curl」の完全再現 >>> 重要ヘッダだけ厳守 + ログ追加

    # (a) /basic/overview — 404でも無視
    begin
      res = conn.post("/basic/overview") do |req|
        req.headers["accept"]            = "application/json, text/plain, */*"
        req.headers["content-type"]      = "application/json;charset=utf-8"
        req.headers["x-csrf-token"]      = xsrf.to_s
        req.headers["x-requested-with"]  = "XMLHttpRequest"
        req.headers["user-agent"]        = UA
        req.headers["origin"]            = ORIGIN
        req.headers["referer"]           = "#{ORIGIN}/basic/overview"
        req.body = nil
      end
      Rails.logger.debug("[curl-basic-overview] status=#{res.status} body=#{safe_head(res.body)}")
    rescue => e
      Rails.logger.debug("[curl-basic-overview] #{e.class}: #{e.message}")
    end

    # (b) /ajax/get-bot-data — 空ボディ、Referer は /basic/friendlist
    begin
      friendlist_referer = "#{ORIGIN}/basic/friendlist?followed_from=#{start_on}&followed_to=#{end_on}"
      res = conn.post("/ajax/get-bot-data") do |req|
        req.headers["accept"]            = "application/json, text/javascript, */*; q=0.01"
        req.headers["x-requested-with"]  = "XMLHttpRequest"
        req.headers["user-agent"]        = UA
        req.headers["origin"]            = ORIGIN
        req.headers["referer"]           = friendlist_referer
        req.headers["x-csrf-token"]      = xsrf.to_s
        req.headers["x-xsrf-token"]      = xsrf.to_s # 念のため両方
        req.body = nil
      end
      Rails.logger.debug("[curl-get-bot-data] status=#{res.status} body=#{safe_head(res.body)}")
    rescue => e
      Rails.logger.debug("[curl-get-bot-data] #{e.class}: #{e.message}")
    end

    # (c) /ajax/init-data-history-add-friend — JSON ボディ {"data":"{\"start\":\"..\",\"end\":\"..\"}"}
    begin
      payload = { data: { start: start_on, end: end_on }.to_json }.to_json

      res = conn.post("/ajax/init-data-history-add-friend") do |req|
        req.headers["accept"]            = "application/json, text/plain, */*"
        req.headers["content-type"]      = "application/json;charset=UTF-8"
        req.headers["x-requested-with"]  = "XMLHttpRequest"
        req.headers["user-agent"]        = UA
        req.headers["origin"]            = ORIGIN
        req.headers["referer"]           = "#{ORIGIN}/basic/friendlist/friend-history"
        # XSRF はどちら表記でも受けるケースがあるため両方付与
        req.headers["x-csrf-token"]      = xsrf.to_s
        req.headers["x-xsrf-token"]      = xsrf.to_s
        req.body = payload
      end
      Rails.logger.debug("[curl-init-data-history-add-friend] status=#{res.status} body=#{safe_head(res.body)}")
    rescue => e
      Rails.logger.debug("[curl-init-data-history-add-friend] #{e.class}: #{e.message}")
    end
    # (d) 必要に応じ “予熱” で post-advance-filter-v2 を 1 回叩く（レスは捨てる）
    begin
      res = conn.post("/basic/friendlist/post-advance-filter-v2") do |req|
        req.headers["accept"]            = "*/*"
        req.headers["content-type"]      = "application/x-www-form-urlencoded; charset=UTF-8"
        req.headers["x-requested-with"]  = "XMLHttpRequest"
        req.headers["user-agent"]        = UA
        req.headers["origin"]            = ORIGIN
        req.headers["referer"]           = "#{ORIGIN}/basic/friendlist?followed_from=#{start_on}&followed_to=#{end_on}"
        form = {
          item_search: '[]',
          item_search_or: '[]',
          scenario_stop_id: '',
          scenario_id_running: '',
          scenario_unfinish_id: '',
          orderBy: 0,
          sort_followed_at_increase: '',
          sort_last_time_increase: '',
          keyword: '',
          rich_menu_id: '',
          page: 1,
          followed_to: end_on,
          followed_from: start_on,
          connect_db_replicate: 'false',
          line_user_id_deleted: '',
          is_cross: 'false'
        }
        req.body = URI.encode_www_form(form)
      end
      Rails.logger.debug("[post-advance-filter-v2] status=#{res.status} body=#{safe_head(res.body)}")
    rescue => e
      Rails.logger.debug("[post-advance-filter-v2] #{e.class}: #{e.message}")
    end
    debugger
    # 5) ★既存ロジックに戻る（FriendHistoryService は変更なし）
    result = Lme::FriendHistoryService.new(auth: auth_client).overview_with_tags(
      auth_client.conn,
      start_on: start_on,
      end_on:   end_on,
      bot_id:   bot_id
    )

    rows       = result[:rows]
    tags_cache = result[:tags_cache]
    Rails.logger.info("[LmeLineInflowsWorker] ✅ Integrated fetch completed: #{rows.size} rows, #{tags_cache.size} users tagged")

    # 6) GSheets 反映（元のまま）
    spreadsheet_id = ENV.fetch('ONCLASS_SPREADSHEET_ID')
    sheet_name     = ENV.fetch('LME_SHEET_NAME', 'Line流入者')
    anchor_name    = ENV.fetch('ONCLASS_SHEET_NAME', 'フロントコース受講生')

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

  # =========================================================
  # Google Sheets
  # =========================================================
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

    service.update_spreadsheet_value(spreadsheet_id, "#{sheet_name}!B3",
      Google::Apis::SheetsV4::ValueRange.new(values: [row3]), value_input_option: 'USER_ENTERED')
    service.update_spreadsheet_value(spreadsheet_id, "#{sheet_name}!B4",
      Google::Apis::SheetsV4::ValueRange.new(values: [row4]), value_input_option: 'USER_ENTERED')
    service.update_spreadsheet_value(spreadsheet_id, "#{sheet_name}!B5",
      Google::Apis::SheetsV4::ValueRange.new(values: [row5]), value_input_option: 'USER_ENTERED')

    header_range = "#{sheet_name}!B6:#{a1_col(1 + headers.size)}6"
    service.update_spreadsheet_value(spreadsheet_id, header_range,
      Google::Apis::SheetsV4::ValueRange.new(values: [headers]), value_input_option: 'USER_ENTERED')

    sorted = Array(rows).sort_by { |r|
      [ r['date'].to_s, (Time.zone.parse(r['followed_at'].to_s) rescue r['followed_at'].to_s) ]
    }.reverse

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
      service.update_spreadsheet_value(
        spreadsheet_id, "#{sheet_name}!B7",
        Google::Apis::SheetsV4::ValueRange.new(values: data_values),
        value_input_option: 'USER_ENTERED'
      )
    end
  end

  # === 集計ヘルパ（元のまま） ===============================================
  def calc_rates(rows, tags_cache, month:, since:)
    month_rows = Array(rows).select { |r| month_key(r['date']) == month }
    cum_rows   = Array(rows).select { |r| r['date'].to_s >= since.to_s }

    monthly = { v1: pct_for(month_rows, tags_cache, :v1),
                v2: pct_for(month_rows, tags_cache, :v2),
                v3: pct_for(month_rows, tags_cache, :v3),
                v4: pct_for(month_rows, tags_cache, :v4) }
    cumulative = { v1: pct_for(cum_rows, tags_cache, :v1),
                   v2: pct_for(cum_rows, tags_cache, :v2),
                   v3: pct_for(cum_rows, tags_cache, :v3),
                   v4: pct_for(cum_rows, tags_cache, :v4) }
    [monthly, cumulative]
  end

  def pct_for(rows, tags_cache, key)
    denom = rows.size
    return nil if denom.zero?
    numer = rows.count { |r| (tags_cache[r['line_user_id']] || {})[key] }
    ((numer.to_f / denom) * 100).round(1)
  end

  def month_key(ymd_str) Date.parse(ymd_str.to_s).strftime('%Y-%m') rescue nil end
  def put_percentages!(row_array, rates)
    { v1: 7, v2: 9, v3: 11, v4: 13 }.each do |k, idx|
      v = rates[k]; row_array[idx] = v.nil? ? '' : "#{v}%"
    end
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

  def parse_date(str) Time.zone.parse(str.to_s) || Time.parse(str.to_s) end
  def to_jp_ymdhm(str)
    return '' if str.blank?
    t = (Time.zone.parse(str.to_s) rescue Time.parse(str.to_s) rescue nil)
    return '' unless t
    t.in_time_zone('Asia/Tokyo').strftime('%Y年%-m月%-d日 %H時%M分')
  end

  def a1_col(n)
    s = String.new
    while n && n > 0
      n, r = (n - 1).divmod(26)
      s.prepend((65 + r).chr)
    end
    s
  end

  private

  def default_start_on
    raw = ENV['LME_DEFAULT_START_DATE'].presence || '2025-01-01'
    Date.parse(raw).strftime('%F')
  rescue
    '2024-01-01'
  end

  # 指定「月」(:YYYY-MM) の“タグあり”割合（% 小数1桁）を返す
  def month_rates(rows, tags_cache, month:)
    month_rows = Array(rows).select { |r| month_key(r['date']) == month }
    {
      v1: pct_for(month_rows, tags_cache, :v1),
      v2: pct_for(month_rows, tags_cache, :v2),
      v3: pct_for(month_rows, tags_cache, :v3),
      v4: pct_for(month_rows, tags_cache, :v4)
    }
  end

  # =========================================================
  # Playwright ユーティリティ（互換対応）
  # =========================================================
  def add_cookies_to_context!(context, raw_cookies, default_domain: "step.lme.jp")
    normalized = Array(raw_cookies).map do |c|
      h = c.respond_to?(:to_h) ? c.to_h : c
      cookie = {
        name:     (h[:name]  || h["name"]).to_s,
        value:    (h[:value] || h["value"]).to_s,
        domain:   (h[:domain] || h["domain"] || default_domain).to_s,
        path:     (h[:path]   || h["path"]   || "/").to_s,
        httpOnly: !!(h[:http_only] || h["http_only"] || h[:httponly] || h["httponly"] || h["httpOnly"]),
        secure:   true
      }
      exp = (h[:expires] || h["expires"] || h[:expiry] || h["expiry"])
      cookie[:expires] =
        case exp
        when Time      then exp.to_i
        when Integer   then exp
        when Float     then exp.to_i
        when String    then (Time.parse(exp).to_i rescue nil)
        else nil
        end
      cookie.compact
    end

    begin
      context.add_cookies(normalized)            # 旧: 位置引数
    rescue ArgumentError, Playwright::Error
      context.add_cookies(cookies: normalized)   # 新: キーワード引数
    end
  end
  private :add_cookies_to_context!

  def ctx_cookies(context, domain = nil)
    cookies = begin
      context.cookies                             # 旧API
    rescue ArgumentError, NoMethodError
      context.cookies(["https://step.lme.jp"])    # 新API
    end
    return cookies unless domain

    Array(cookies).select do |c|
      d = (c["domain"] || c[:domain] || "")
      d.include?(domain)
    end
  end
  private :ctx_cookies

  def pw_wait_networkidle(page)
    page.wait_for_load_state(state: "networkidle")
  rescue Playwright::TimeoutError, ArgumentError, NoMethodError
    sleep 1
  end
  private :pw_wait_networkidle

  def pw_wait_for_url(page, pattern, timeout_ms = 15_000)
    page.wait_for_url(pattern, timeout: timeout_ms)
    true
  rescue Playwright::TimeoutError, ArgumentError, NoMethodError
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout_ms.to_f/1000
    loop do
      return true if page.url.to_s.match?(pattern)
      break if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.2
    end
    false
  end
  private :pw_wait_for_url

  def pw_eval(page, func_str, arg=nil)
    arg.nil? ? page.evaluate(func_str) : page.evaluate(func_str, arg)
  rescue ArgumentError
    page.evaluate(func_str)
  end
  private :pw_eval

  def pw_add_init_script(page, code)
    page.add_init_script(script: code)
  rescue ArgumentError, NoMethodError
    page.add_init_script(code)
  end
  private :pw_add_init_script

  # 短いログ用
  def safe_head(s, n = 200)
    str = s.to_s
    str.bytesize > n ? str.byteslice(0, n) + "...(trunc)" : str
  end
end
