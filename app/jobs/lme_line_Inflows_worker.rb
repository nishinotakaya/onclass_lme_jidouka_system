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
require 'selenium-webdriver'
require 'net/http'
require 'selenium/devtools'
require 'playwright'

class LmeLineInflowsWorker
  include Sidekiq::Worker
  sidekiq_options queue: 'lme_line_inflows', retry: 3

  GOOGLE_SCOPE = [Google::Apis::SheetsV4::AUTH_SPREADSHEETS].freeze
  CUM_SINCE    = ENV['LME_CUM_SINCE'].presence || '2025-09-20'
  ORIGIN       = 'https://step.lme.jp'
  UA           = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
  ACCEPT_LANG  = 'ja,en-US;q=0.9,en;q=0.8'
  CH_UA        = %Q("Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140")

  # =========================================================
  # メイン
  # =========================================================
  def perform(start_date = nil, end_date = nil)
    # 1) ログイン → Cookie/XSRF 入手
    login_service = LmeLoginUserService.new(
      email:    ENV['GOOGLE_EMAIL'],
      password: ENV['GOOGLE_PASSWORD'],
      api_key:  ENV['API2CAPTCHA_KEY']
    )
    login_result = login_service.fetch_friend_history

    # === 修正: fallback を禁止して sanitize 済みだけ使う ===
    cookie_header = login_result[:basic_cookie_header].to_s.strip
    xsrf_cookie  = login_result[:basic_xsrf].to_s.strip
    raise 'basic_cookie_header missing' if cookie_header.blank?
    raise 'basic_xsrf missing' if xsrf_cookie.blank?

    # 2) meta の CSRF トークンを取得（HTTP → 取れなければ Playwright DOM で取得）
    csrf_meta, meta_src = fetch_csrf_meta_with_cookies(cookie_header, [
      '/basic/friendlist',
      '/basic/overview',
      '/basic',
      '/admin/home',
      '/'
    ])
    unless csrf_meta.present?
      Rails.logger.warn('[csrf-meta] HTTPで取得できず → Playwright で DOM から取得フォールバック')
      csrf_meta, cookie_header, xsrf_cookie, meta_src =
        playwright_fetch_meta_csrf!(login_result[:cookies], ['/basic/friendlist', '/basic/overview', '/basic'])
    end
    if csrf_meta.present?
      Rails.logger.debug("[csrf-meta] found at #{meta_src}: #{csrf_meta[0,8]}...(masked)")
    else
      Rails.logger.warn('[csrf-meta] not found. FORM は x-xsrf-token 併用のみで試行します')
    end

    # 3) 期間・bot
    start_on = (start_date.presence || default_start_on)
    end_on   = (end_date.presence   || Time.zone.today.to_s)
    bot_id   = (ENV['LME_BOT_ID'].presence || '17106').to_s
    Rails.logger.debug("start_on=#{start_on} end_on=#{end_on} cookie_header#{cookie_header} xsrf_cookie#{xsrf_cookie}")
    # 4) 前座（JSON）: /ajax/init-data-history-add-friend  ※cURLどおり x-xsrf-token を使う
    Rails.logger.info('[Inflows] warmup: init-data-history-add-friend …')
    begin
      _ = curl_post_json(
        path: '/ajax/init-data-history-add-friend',
        json_body: { data: { start: start_on, end: end_on }.to_json }.to_json,
        cookie: cookie_header,
        xsrf: xsrf_cookie,
        referer: "#{ORIGIN}/basic/friendlist/friend-history",
        extra_headers: { 'x-server' => 'ovh' }
      )
    rescue => e
      Rails.logger.debug("[warmup:init-data] #{e.class}: #{e.message}")
    end

    # 5) 本体（FORM）: /basic/friendlist/post-advance-filter-v2
    Rails.logger.info('[Inflows] Fetching friendlist (post-advance-filter-v2)…')
    raw_rows = []
    begin
      page_no = 1
      loop do
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
          page: page_no,
          followed_to: end_on,
          followed_from: start_on,
          connect_db_replicate: 'false',
          line_user_id_deleted: '',
          is_cross: 'false'
        }

        Rails.logger.debug(form.inspect)

        res_body = curl_post_form(
          path: '/basic/friendlist/post-advance-filter-v2',
          form: form,
          cookie: cookie_header,
          csrf_meta: csrf_meta,
          xsrf_cookie: xsrf_cookie,
          referer: "#{ORIGIN}/basic/friendlist"
        )

        body = JSON.parse(res_body) rescue {}
        data = body.dig('data', 'data') || []
        # === ここで必ずログ ===
        
        filtered = data.select do |row|
          blocked  = row['is_blocked'].to_i == 1
          followed = row['followed_at'].present? &&
          Date.parse(row['followed_at']) >= Date.parse(start_on) &&
          Date.parse(row['followed_at']) <= Date.parse(end_on)
          blocked || followed
        end
        Rails.logger.debug("[post-advance-filter-v2] body=#{body} page=#{page_no} data_count=#{data.size} filtered=#{filtered.size}")
        raw_rows.concat(filtered)
        break if data.empty? || body.dig('data', 'current_page') >= body.dig('data', 'last_page')
        page_no += 1
        sleep 0.15
      end
    rescue => e
      Rails.logger.debug("[post-advance-filter-v2] fetch error: #{e.class}: #{e.message}")
    end

    rows = raw_rows.select do |r|
      r['followed_at'].present? &&
        Date.parse(r['followed_at']) >= Date.parse('2025-09-20')
    end
    rows.uniq! { |r| [r['line_user_id'], r['followed_at']] }
    Rails.logger.debug("[rows-filter] raw_rows=#{raw_rows.size} → rows=#{rows.size}")
    Rails.logger.debug("[rows-sample] #{rows.first.inspect}") if rows.present?
    # 7) タグ取得の前に、チャット画面でCookie/XSRFしつつmetaも再確認
    if rows.present?
      sample_uid = rows.first['line_user_id']
      chat_cookie_header, chat_xsrf_cookie = playwright_bake_chat_cookies!(login_result[:cookies], sample_uid, bot_id)
      cookie_header = (chat_cookie_header.presence || cookie_header)
      xsrf_cookie  = (chat_xsrf_cookie.presence  || xsrf_cookie)

      # chat-v3 の DOM から meta 再取得（保険）
      meta2, cookie_header2, xsrf2, src2 =
        playwright_fetch_meta_csrf!(login_result[:cookies], "/basic/chat-v3?friend_id=#{sample_uid}")
      csrf_meta    = (meta2.presence || csrf_meta)
      cookie_header = (cookie_header2.presence || cookie_header)
      xsrf_cookie   = (xsrf2.presence || xsrf_cookie)
      Rails.logger.debug("[csrf-meta] chat-v3 DOM re-fetch: #{src2 || 'none'} #{csrf_meta.present? ? '(ok)' : '(miss)'}")
    end

    # 8) タグ取得（FORM）
    Rails.logger.info("[Inflows] Fetching tags inline for #{rows.size} rows…")
    tags_cache = {}
    rows.each_with_index do |r, i|
      uid = r['line_user_id']
      begin
        form = { line_user_id: uid, is_all_tag: 0 }
        res_body = curl_post_form(
          path: '/basic/chat/get-categories-tags',
          form: form,
          cookie: cookie_header,
          csrf_meta: csrf_meta,
          xsrf_cookie: xsrf_cookie,
          referer: "#{ORIGIN}/basic/chat-v3?friend_id=#{uid}"
        )
        flags = extract_tag_flags_from_payload(res_body)
        if flags.present?
          tags_cache[uid] = flags
          r['tags_flags'] = flags
        end
      rescue => e
        Rails.logger.debug("[get-categories-tags] uid=#{uid} #{e.class}: #{e.message}")
      end
      sleep 0.12
      Rails.logger.debug("[tags-inline] #{i+1}/#{rows.size} uid=#{uid} #{tags_cache.key?(uid) ? 'OK' : 'skip'}") if (i % 200).zero?
    end

    Rails.logger.info("[Inflows] ✅ Integrated fetch completed: #{rows.size} rows, #{tags_cache.size} users tagged")

    # 9) GSheets 反映（既存）
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
  # POST: x-www-form-urlencoded（meta: x-csrf-token / cookie: x-xsrf-token）
  # =========================================================
  def curl_post_form(path:, form:, cookie:, csrf_meta:, xsrf_cookie:, referer:)
    conn = Faraday.new(url: ORIGIN) do |f|
      f.request :url_encoded
      f.adapter Faraday.default_adapter
    end
    res = conn.post(path) do |req|
      req.headers['accept']             = '*/*'
      req.headers['accept-language']    = ACCEPT_LANG
      req.headers['content-type']       = 'application/x-www-form-urlencoded; charset=UTF-8'
      req.headers['cookie']             = cookie.to_s
      req.headers['origin']             = ORIGIN
      req.headers['priority']           = 'u=1, i'
      req.headers['referer']            = referer
      req.headers['sec-ch-ua']          = CH_UA
      req.headers['sec-ch-ua-mobile']   = '?0'
      req.headers['sec-ch-ua-platform'] = %Q("macOS")
      req.headers['sec-fetch-dest']     = 'empty'
      req.headers['sec-fetch-mode']     = 'cors'
      req.headers['sec-fetch-site']     = 'same-origin'
      req.headers['user-agent']         = UA
      req.headers['x-requested-with']   = 'XMLHttpRequest'
      req.headers['x-csrf-token']       = csrf_meta.to_s       if csrf_meta.present?          # meta 由来
      req.headers['x-xsrf-token']       = xsrf_cookie.to_s      if xsrf_cookie.present?        # Cookie（デコード済み）
      req.body = URI.encode_www_form(form)
    end
    res.body.to_s
  end
  private :curl_post_form

  # =========================================================
  # POST: application/json（JSON は x-xsrf-token のみで OK）
  # =========================================================
  def curl_post_json(path:, json_body:, cookie:, xsrf:, referer:, extra_headers: {})
    conn = Faraday.new(url: ORIGIN) { |f| f.adapter Faraday.default_adapter }
    res = conn.post(path) do |req|
      req.headers['accept']             = 'application/json, text/plain, */*'
      req.headers['accept-language']    = ACCEPT_LANG
      req.headers['content-type']       = 'application/json;charset=UTF-8'
      req.headers['cookie']             = cookie.to_s
      req.headers['origin']             = ORIGIN
      req.headers['priority']           = 'u=1, i'
      req.headers['referer']            = referer
      req.headers['sec-ch-ua']          = CH_UA
      req.headers['sec-ch-ua-mobile']   = '?0'
      req.headers['sec-ch-ua-platform'] = %Q("macOS")
      req.headers['sec-fetch-dest']     = 'empty'
      req.headers['sec-fetch-mode']     = 'cors'
      req.headers['sec-fetch-site']     = 'same-origin'
      req.headers['user-agent']         = UA
      req.headers['x-xsrf-token']       = xsrf.to_s
      extra_headers.each { |k, v| req.headers[k] = v }
      req.body = json_body.to_s
    end
    res.body.to_s
  end
  private :curl_post_json

  # =========================================================
  # （1）まず Cookie 付き GET で meta CSRF を探す
  # =========================================================
  def fetch_csrf_meta_with_cookies(cookie_header, paths = '/')
    try_paths = Array(paths).compact_blank
    try_paths = ['/'] if try_paths.empty?

    try_paths.each do |p|
      html, final_url = get_with_cookies(cookie_header, p)
      token = extract_meta_csrf(html)
      return [token, final_url] if token.present?
    end
    [nil, nil]
  end
  private :fetch_csrf_meta_with_cookies

  def get_with_cookies(cookie_header, path)
    url = URI.join(ORIGIN, path).to_s
    5.times do
      res = Faraday.new(url: ORIGIN) { |f| f.adapter Faraday.default_adapter }.get(path) do |req|
        req.headers['accept']                   = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        req.headers['accept-language']          = ACCEPT_LANG
        req.headers['cookie']                   = cookie_header.to_s
        req.headers['user-agent']               = UA
        req.headers['sec-ch-ua']                = CH_UA
        req.headers['sec-ch-ua-mobile']         = '?0'
        req.headers['sec-ch-ua-platform']       = %Q("macOS")
        req.headers['upgrade-insecure-requests']= '1'
        req.headers['cache-control']            = 'no-cache'
        req.headers['pragma']                   = 'no-cache'
        req.headers['referer']                  = ORIGIN
      end
      case res.status
      when 301, 302, 303, 307, 308
        loc = res.headers['location'].to_s
        return [res.body.to_s, url] if loc.blank?
        path = URI.join(ORIGIN, loc).request_uri
        url  = URI.join(ORIGIN, loc).to_s
        next
      else
        return [res.body.to_s, url]
      end
    end
    ['', url]
  rescue => e
    Rails.logger.debug("[csrf-meta GET] #{e.class}: #{e.message}")
    ['', url]
  end
  private :get_with_cookies

  def extract_meta_csrf(html)
    return nil if html.blank?
    token = html[/<meta[^>]+name=["']csrf-token["'][^>]*content=["']([^"']+)["']/i, 1]
    return token if token.present?
    token = html[/csrfToken["']?\s*[:=]\s*["']([^"']+)["']/i, 1]
    return token if token.present?
    nil
  end
  private :extract_meta_csrf

  # =========================================================
  # （2）ダメなら Playwright で実ページの DOM から meta CSRF を取得
  #     ついでに Cookie ヘッダと XSRF（デコード済み）も取り直して返す
  # =========================================================
  def playwright_fetch_meta_csrf!(raw_cookies, paths)
    csrf_meta = nil
    cookie_header = nil
    xsrf_cookie = nil
    src = nil

    Playwright.create(playwright_cli_executable_path: 'npx playwright') do |pw|
      browser = pw.chromium.launch(headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage'])
      context = browser.new_context
      begin
        add_cookies_to_context!(context, raw_cookies)
        page = context.new_page

        Array(paths).compact_blank.each do |p|
          page.goto("#{ORIGIN}#{p}")
          pw_wait_for_url(page, %r{/basic/}, 15_000)
          pw_wait_networkidle(page)

          # 1) meta → 2) window.Laravel.csrfToken → 3) window.csrfToken
          csrf_meta = page.evaluate(<<~JS)
            () => {
              const m = document.querySelector('meta[name="csrf-token"]');
              if (m && m.content) return m.content;
              if (window && window.Laravel && window.Laravel.csrfToken) return window.Laravel.csrfToken;
              if (window && window.csrfToken) return window.csrfToken;
              return null;
            }
          JS
          if csrf_meta.to_s.strip != ''
            src = p
            break
          end
        end

        # Cookie を文字列化 / XSRF（Cookie）をデコードして取り出す
        pl_cookies = ctx_cookies(context, 'step.lme.jp')
        cookie_header = pl_cookies.map { |c| "#{(c['name']||c[:name])}=#{(c['value']||c[:value])}" }.join('; ')
        xsrf_row = pl_cookies.find { |c| (c['name'] || c[:name]) == 'XSRF-TOKEN' }
        if xsrf_row
          raw_val = (xsrf_row['value'] || xsrf_row[:value]).to_s
          xsrf_cookie = CGI.unescape(raw_val) # ← ヘッダ用は“デコード済み”にする
        end
      ensure
        context&.close rescue nil
        browser&.close rescue nil
      end
    end

    [csrf_meta, cookie_header, xsrf_cookie, src]
  rescue => e
    Rails.logger.debug("[playwright_fetch_meta_csrf] #{e.class}: #{e.message}")
    [nil, nil, nil, nil]
  end
  private :playwright_fetch_meta_csrf!

  # =========================================================
  # タグ抽出（既存）
  # =========================================================
  def extract_tag_flags_from_payload(body)
    json = JSON.parse(body) rescue {}
    strings = deep_collect_strings(json)
    return {} if strings.empty?

    select_label = strings.find { |s| s.include?('選択肢') } || strings.find { |s| s =~ /選択/ }

    patterns = {
      dv1: [/動画.?①.*ダイジェスト/i, /ダイジェスト.*1/i, /Digest.*1/i],
      v1:  [/プロアカ.?動画.?①/i, /動画.?①.*(本編|視聴|購入|フル)/i],
      dv2: [/動画.?②.*ダイジェスト/i, /ダイジェスト.*2/i, /Digest.*2/i],
      v2:  [/プロアカ.?動画.?②/i, /動画.?②.*(本編|視聴|購入|フル)/i],
      dv3: [/動画.?③.*ダイジェスト/i, /ダイジェスト.*3/i, /Digest.*3/i],
      v3:  [/プロアカ.?動画.?③/i, /動画.?③.*(本編|視聴|購入|フル)/i],
      v4:  [/プロアカ.?動画.?④/i, /動画.?④/i]
    }

    names = extract_probable_tag_names(json)
    pool  = (names + strings).uniq

    flags = {}
    patterns.each { |k, regs| flags[k] = regs.any? { |re| pool.any? { |s| s =~ re } } }
    flags[:select] = select_label
    flags
  rescue => e
    Rails.logger.debug("[extract_tag_flags] #{e.class}: #{e.message}")
    {}
  end

  def extract_probable_tag_names(obj)
    keys = %w[name tag_name category_name title label text value]
    out  = []
    walk = lambda do |v|
      case v
      when Hash
        v.each do |k, vv|
          out << vv.to_s if keys.include?(k.to_s) && vv.is_a?(String)
          walk.call(vv)
        end
      when Array
        v.each { |e| walk.call(e) }
      end
    end
    walk.call(obj)
    out.compact.map(&:to_s)
  end

  def deep_collect_strings(obj)
    out = []
    walk = lambda do |v|
      case v
      when String
        out << v
      when Hash
        v.each_value { |vv| walk.call(vv) }
      when Array
        v.each { |e| walk.call(e) }
      end
    end
    walk.call(obj)
    out.compact.map(&:to_s)
  end

  # =========================================================
  # Google Sheets（既存）
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
      t = (r['tags_flags'] || tags_cache[r['line_user_id']] || {})
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

  # === 集計ヘルパ ===============================================
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
    numer = rows.count { |r| ((r['tags_flags'] || tags_cache[r['line_user_id']] || {})[key]) }
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
    url   = "#{ORIGIN}/basic/friendlist/my_page/#{id}"
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
    '2025-09-20'
  end

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
  # Playwright ユーティリティ
  # =========================================================
  def add_cookies_to_context!(context, raw_cookies, default_domain: 'step.lme.jp')
    normalized = Array(raw_cookies).map do |c|
      h = c.respond_to?(:to_h) ? c.to_h : c
      http_only_flag = h[:http_only] || h['http_only'] || h[:httponly] || h['httponly'] || false
      cookie = {
        name:     (h[:name]  || h['name']).to_s,
        value:    (h[:value] || h['value']).to_s,
        domain:   (h[:domain] || h['domain'] || default_domain).to_s,
        path:     (h[:path]   || h['path']   || '/').to_s,
        httpOnly: !!http_only_flag,
        secure:   true
      }
      exp = (h[:expires] || h['expires'] || h[:expiry] || h['expiry'])
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
      context.add_cookies(normalized)
    rescue ArgumentError, Playwright::Error
      context.add_cookies(cookies: normalized)
    end
  end
  private :add_cookies_to_context!

  def ctx_cookies(context, domain = nil)
    cookies = Array(context.cookies || [])
    return cookies unless domain
    cookies.select do |c|
      d = (c['domain'] || c[:domain] || (c.respond_to?(:domain) ? c.domain : '') || '').to_s
      d.include?(domain)
    end
  end
  private :ctx_cookies

  def pw_wait_networkidle(page)
    page.wait_for_load_state(state: 'networkidle')
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

  def pw_add_init_script(page, code)
    page.add_init_script(script: code)
  rescue ArgumentError, NoMethodError
    page.add_init_script(code)
  end
  private :pw_add_init_script

  def playwright_bake_chat_cookies!(raw_cookies, sample_uid, bot_id)
    cookie_header = nil
    xsrf          = nil

    Playwright.create(playwright_cli_executable_path: 'npx playwright') do |pw|
      browser = pw.chromium.launch(headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage'])
      context = browser.new_context
      begin
        add_cookies_to_context!(context, raw_cookies)
        page = context.new_page
        pw_add_init_script(page, 'window.open = (url, target) => { location.href = url; }')

        page.goto("#{ORIGIN}/basic/chat-v3?friend_id=#{sample_uid}")
        pw_wait_for_url(page, %r{/basic/chat-v3}, 15_000)
        pw_wait_networkidle(page)

        pl_cookies = ctx_cookies(context, 'step.lme.jp')
        names = pl_cookies.map { |c| (c['name'] || c[:name]).to_s }
        Rails.logger.debug("[PW] cookie names(chat): #{names.join(', ')}")

        cookie_header = pl_cookies.map { |c|
          "#{(c['name']||c[:name])}=#{(c['value']||c[:value])}"
        }.join('; ')

        xsrf_cookie = pl_cookies.find { |c| (c['name'] || c[:name]) == 'XSRF-TOKEN' }
        xsrf_raw    = xsrf_cookie && (xsrf_cookie['value'] || xsrf_cookie[:value])
        xsrf        = xsrf_raw && CGI.unescape(xsrf_raw.to_s)
      ensure
        context&.close rescue nil
        browser&.close rescue nil
      end
    end
    [cookie_header, xsrf]
  end
  private :playwright_bake_chat_cookies!

  # 短いログ用
  def safe_head(s, n = 200)
    str = s.to_s
    str.bytesize > n ? str.byteslice(0, n) + '...(trunc)' : str
  end
end
