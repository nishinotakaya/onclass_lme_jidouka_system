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
  CUM_SINCE    = ENV['LME_CUM_SINCE'].presence || '2025-09-10'
  UA           = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
  ORIGIN       = 'https://step.lme.jp'

  # =========================================================
  # メイン
  # =========================================================
  def perform(start_date = nil, end_date = nil)
    # 1) Seleniumでログイン → Cookie取得
    login_service = LmeLoginUserService.new(
      email:    ENV["GOOGLE_EMAIL"],
      password: ENV["GOOGLE_PASSWORD"],
      api_key:  ENV["API2CAPTCHA_KEY"]
    )
    login_result = login_service.fetch_friend_history

    auth_client = LmeAuthClient.new(login_result[:driver])
    auth_client.manual_set!(login_result[:cookie_str], login_result[:cookies])

    # === テスト：デフォルト開始日は 2025-09-10（引数で上書き可） ===
    start_on = (start_date.presence || default_start_on) # => 2025-09-10 を返す
    end_on   = (end_date.presence   || Time.zone.today.to_s)
    bot_id   = (ENV['LME_BOT_ID'].presence || '17106').to_s

    # 2) Playwright: /basic 側Cookie育成 & XSRF 取得（friendlist用）
    cookie_header, xsrf = playwright_bake_basic_cookies!(login_result[:cookies], bot_id)

    # 3) Faraday へセッション適用
    auth_client.apply_session!(cookie_header, xsrf,
      referer: "#{LmeAuthClient::BASE_URL}/basic/friendlist/friend-history"
    )
    conn = auth_client.conn

    # 4) cURL 準拠の前座叩き
    curl_like_warmups!(conn, xsrf, start_on, end_on)

    # 5) friendlist をページングで収集
    Rails.logger.info("[Inflows] Fetching friendlist (v2, paginated)...")
    raw_rows = []
    begin
      page_no   = 1
      last_page = nil
      loop do
        res = conn.post("/basic/friendlist/post-advance-filter-v2") do |req|
          req.headers["accept"]           = "*/*"
          req.headers["content-type"]     = "application/x-www-form-urlencoded; charset=UTF-8"
          req.headers["x-requested-with"] = "XMLHttpRequest"
          req.headers["user-agent"]       = UA
          req.headers["origin"]           = ORIGIN
          req.headers["referer"]          = "#{ORIGIN}/basic/friendlist?followed_from=#{start_on}&followed_to=#{end_on}"
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
          req.body = URI.encode_www_form(form)
          body = JSON.parse(res.body)
          data = body.dig("data", "data") || []
          debugger
          # ここで抽出
          filtered = data.select do |row|
            row["is_blocked"] == 1 || row["followed_at"].present?
          end
          rows.concat(filtered)
          break if data.empty? || body.dig("data", "current_page") >= body.dig("data", "last_page")
          page_no += 1
          sleep 0.1 # 連打防止
        end

        Rails.logger.debug("[post-advance-filter-v2] page=#{page_no} status=#{res.status} body=#{safe_head(res.body)}")
        break unless res.status.to_i == 200

        json         = JSON.parse(res.body) rescue {}
        data_block   = json.is_a?(Hash) ? json["data"] : nil
        page_items   = data_block.is_a?(Hash) ? (data_block["data"] || []) : []
        current_page = data_block.is_a?(Hash) ? data_block["current_page"].to_i : page_no
        last_page    = data_block.is_a?(Hash) ? (data_block["last_page"] || last_page || (page_items.empty? ? current_page : current_page)) : last_page

        Array(page_items).each do |rec|
          rec = rec.respond_to?(:with_indifferent_access) ? rec.with_indifferent_access : (rec || {})
          lu_link = rec['link_my_page']
          uid     = rec['line_user_id'] || last_id_from_my_page(lu_link) || rec['line_id']
          followed_at = rec['followed_at']
          date_key    = begin
                          Time.zone.parse(followed_at.to_s).to_date.strftime('%Y-%m-%d')
                        rescue
                          (Date.parse(followed_at.to_s) rescue nil)&.strftime('%Y-%m-%d')
                        end
          raw_rows << {
            'date'         => date_key,
            'followed_at'  => rec['followed_at'],
            'landing_name' => rec['landing_name'],
            'name'         => rec['name'],
            'line_user_id' => uid,
            'line_id'      => rec['line_id'],
            'is_blocked'   => (rec['is_blocked'] || 0).to_i
          }
        end

        break if last_page && current_page >= last_page
        break if page_items.blank?
        page_no += 1
        sleep 0.15 # 連打抑制
      end
    rescue => e
      Rails.logger.debug("[post-advance-filter-v2] fetch error: #{e.class}: #{e.message}")
    end

    # 6) rows を限定（テスト: 2025-09-10 以降のみ）& 重複除去
    rows = raw_rows.select { |r| r['date'].to_s >= '2025-09-10' }
    rows.uniq! { |r| [r['line_user_id'], r['followed_at']] }

    # 7) chat-v3 を1回踏んで “チャット用Cookie” を育成 → Faradayを差し替え
    if rows.present?
      sample_uid = rows.first['line_user_id']
      chat_cookie_header, chat_xsrf = playwright_bake_chat_cookies!(login_result[:cookies], sample_uid, bot_id)
      auth_client.apply_session!(chat_cookie_header, chat_xsrf,
        referer: "#{LmeAuthClient::BASE_URL}/basic/chat-v3?friend_id=#{sample_uid}"
      )
      conn = auth_client.conn
      xsrf = chat_xsrf
    end

    # 8) rows 作成中にタグも同時取得
    Rails.logger.info("[Inflows] Fetching tags inline for #{rows.size} rows...")
    tags_cache = {}
    rows.each_with_index do |r, i|
      uid = r['line_user_id']
      # 各 UID に合わせて referer を合わせる
      flags = fetch_tags_for_uid(conn: conn, xsrf: xsrf, bot_id: bot_id, uid: uid)
      if flags.present?
        tags_cache[uid] = flags
        r['tags_flags'] = flags # rowsにも持たせておく
      end
      sleep 0.12
      # 進捗ログ（たまに）
      Rails.logger.debug("[tags-inline] #{i+1}/#{rows.size} uid=#{uid} #{flags ? 'OK' : 'skip'}") if (i % 200).zero?
    end

    Rails.logger.info("[Inflows] ✅ Integrated fetch completed: #{rows.size} rows, #{tags_cache.size} users tagged")

    # 9) GSheets 反映
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
  # タグ取得（404なら即切る）
  # =========================================================
  def fetch_tags_for_uid(conn:, xsrf:, bot_id:, uid:)
    begin
      res = conn.post("/basic/chat/get-categories-tags?friend_id=#{uid}") do |req|
        req.headers["accept"]           = "*/*"
        req.headers["content-type"]     = "application/x-www-form-urlencoded; charset=UTF-8"
        req.headers["x-requested-with"] = "XMLHttpRequest"
        req.headers["x-csrf-token"]     = xsrf.to_s
        req.headers["x-xsrf-token"]     = xsrf.to_s
        req.headers["X-XSRF-TOKEN"]     = xsrf.to_s
        req.headers["user-agent"]       = UA
        req.headers["origin"]           = ORIGIN
        req.headers["referer"]          = "#{ORIGIN}/basic/chat-v3?friend_id=#{uid}"

        form = {
          line_user_id: uid,
          is_all_tag:   0,
          botIdCurrent: bot_id
        }
        req.body = URI.encode_www_form(form)
      end

      case res.status.to_i
      when 200
        Rails.logger.debug("[get-categories-tags] uid=#{uid} status=200 body=#{safe_head(res.body)}")
        extract_tag_flags_from_payload(res.body)
      when 404
        Rails.logger.debug("[get-categories-tags] uid=#{uid} status=404 (cut request)")
        nil # 即切り
      else
        Rails.logger.debug("[get-categories-tags] uid=#{uid} status=#{res.status} body=#{safe_head(res.body)}")
        nil
      end
    rescue Faraday::ResourceNotFound
      Rails.logger.debug("[get-categories-tags] uid=#{uid} 404(ResourceNotFound) (cut)")
      nil
    rescue => e
      Rails.logger.debug("[get-categories-tags] uid=#{uid} #{e.class}: #{e.message}")
      nil
    end
  end
  private :fetch_tags_for_uid

  # =========================================================
  # Cookie 育成ヘルパ
  # =========================================================
  def playwright_bake_basic_cookies!(raw_cookies, bot_id)
    cookie_header = nil
    xsrf          = nil

    Playwright.create(playwright_cli_executable_path: "npx playwright") do |pw|
      browser = pw.chromium.launch(headless: true, args: ["--no-sandbox", "--disable-dev-shm-usage"])
      context = browser.new_context
      begin
        add_cookies_to_context!(context, raw_cookies)
        page = context.new_page
        pw_add_init_script(page, "window.open = (url, target) => { location.href = url; }")

        page.goto("#{ORIGIN}/admin/home")
        pw_wait_networkidle(page)

        # /basic へ（friendlist 系 Cookie と XSRF を生やす）
        begin
          page.goto("#{ORIGIN}/basic/overview?botIdCurrent=#{bot_id}&isOtherBot=1")
          pw_wait_networkidle(page)
        rescue => e
          Rails.logger.debug("[BOT] skip basic/overview: #{e.class} #{e.message}")
        end

        begin
          page.goto("#{ORIGIN}/basic/friendlist/friend-history?botIdCurrent=#{bot_id}&isOtherBot=1")
          pw_wait_for_url(page, %r{/basic/friendlist/friend-history}, 15_000)
          pw_wait_networkidle(page)
        rescue => e
          Rails.logger.debug("[BOT] friend-history warn: #{e.class} #{e.message}")
        end

        pl_cookies = ctx_cookies(context, "step.lme.jp")
        names = pl_cookies.map { |c| (c["name"] || c[:name]).to_s }
        Rails.logger.debug("[PW] cookie names(basic): #{names.join(', ')}")

        cookie_header = pl_cookies.map { |c|
          "#{(c["name"]||c[:name])}=#{(c["value"]||c[:value])}"
        }.join("; ")

        xsrf_cookie = pl_cookies.find { |c| (c["name"] || c[:name]) == "XSRF-TOKEN" }
        xsrf_raw    = xsrf_cookie && (xsrf_cookie["value"] || xsrf_cookie[:value])
        xsrf        = xsrf_raw && CGI.unescape(xsrf_raw.to_s)
      ensure
        context&.close rescue nil
        browser&.close rescue nil
      end
    end
    [cookie_header, xsrf]
  end
  private :playwright_bake_basic_cookies!

  def playwright_bake_chat_cookies!(raw_cookies, sample_uid, bot_id)
    cookie_header = nil
    xsrf          = nil

    Playwright.create(playwright_cli_executable_path: "npx playwright") do |pw|
      browser = pw.chromium.launch(headless: true, args: ["--no-sandbox", "--disable-dev-shm-usage"])
      context = browser.new_context
      begin
        add_cookies_to_context!(context, raw_cookies)
        page = context.new_page
        pw_add_init_script(page, "window.open = (url, target) => { location.href = url; }")

        # chat-v3 を 1 回踏む（追加Cookieが付く個体あり）
        page.goto("#{ORIGIN}/basic/chat-v3?friend_id=#{sample_uid}&botIdCurrent=#{bot_id}&isOtherBot=1")
        pw_wait_for_url(page, %r{/basic/chat-v3}, 15_000)
        pw_wait_networkidle(page)

        pl_cookies = ctx_cookies(context, "step.lme.jp")
        names = pl_cookies.map { |c| (c["name"] || c[:name]).to_s }
        Rails.logger.debug("[PW] cookie names(chat): #{names.join(', ')}")

        cookie_header = pl_cookies.map { |c|
          "#{(c["name"]||c[:name])}=#{(c["value"]||c[:value])}"
        }.join("; ")

        xsrf_cookie = pl_cookies.find { |c| (c["name"] || c[:name]) == "XSRF-TOKEN" }
        xsrf_raw    = xsrf_cookie && (xsrf_cookie["value"] || xsrf_cookie[:value])
        xsrf        = xsrf_raw && CGI.unescape(xsrf_raw.to_s)
      ensure
        context&.close rescue nil
        browser&.close rescue nil
      end
    end
    [cookie_header, xsrf]
  end
  private :playwright_bake_chat_cookies!

  # =========================================================
  # “cURL 準拠”の前座叩き
  # =========================================================
  def curl_like_warmups!(conn, xsrf, start_on, end_on)
    # (a) /basic/overview — 404でも無視
    begin
      res = conn.post("/basic/overview") do |req|
        req.headers["accept"]           = "application/json, text/plain, */*"
        req.headers["content-type"]     = "application/json;charset=utf-8"
        req.headers["x-csrf-token"]     = xsrf.to_s
        req.headers["x-requested-with"] = "XMLHttpRequest"
        req.headers["user-agent"]       = UA
        req.headers["origin"]           = ORIGIN
        req.headers["referer"]          = "#{ORIGIN}/basic/overview"
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
        req.headers["accept"]           = "application/json, text/javascript, */*; q=0.01"
        req.headers["x-requested-with"] = "XMLHttpRequest"
        req.headers["user-agent"]       = UA
        req.headers["origin"]           = ORIGIN
        req.headers["referer"]          = friendlist_referer
        req.headers["x-csrf-token"]     = xsrf.to_s
        req.headers["x-xsrf-token"]     = xsrf.to_s
        req.body = nil
      end
      Rails.logger.debug("[curl-get-bot-data] status=#{res.status} body=#{safe_head(res.body)}")
    rescue => e
      Rails.logger.debug("[curl-get-bot-data] #{e.class}: #{e.message}")
    end

    # (c) /ajax/init-data-history-add-friend
    begin
      payload = { data: { start: start_on, end: end_on }.to_json }.to_json
      res = conn.post("/ajax/init-data-history-add-friend") do |req|
        req.headers["accept"]           = "application/json, text/plain, */*"
        req.headers["content-type"]     = "application/json;charset=UTF-8"
        req.headers["x-requested-with"] = "XMLHttpRequest"
        req.headers["user-agent"]       = UA
        req.headers["origin"]           = ORIGIN
        req.headers["referer"]          = "#{ORIGIN}/basic/friendlist/friend-history"
        req.headers["x-csrf-token"]     = xsrf.to_s
        req.headers["x-xsrf-token"]     = xsrf.to_s
        req.headers["X-XSRF-TOKEN"]     = xsrf.to_s
        req.body = payload
      end
      Rails.logger.debug("[curl-init-data-history-add-friend] status=#{res.status} body=#{safe_head(res.body)}")
    rescue => e
      Rails.logger.debug("[curl-init-data-history-add-friend] #{e.class}: #{e.message}")
    end
  end
  private :curl_like_warmups!

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

  # === テストデフォルト開始日 ===
  def default_start_on
    '2025-09-10'
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
  # Playwright ユーティリティ
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
      context.add_cookies(normalized)
    rescue ArgumentError, Playwright::Error
      context.add_cookies(cookies: normalized)
    end
  end
  private :add_cookies_to_context!

  def ctx_cookies(context, domain = nil)
    cookies = begin
      context.cookies
    rescue ArgumentError, NoMethodError
      context.cookies(["https://step.lme.jp"])
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

  # =========================================================
  # タグ検出ヘルパ（レスポンスを柔軟にパース）
  # =========================================================
  def extract_tag_flags_from_payload(body)
    json = JSON.parse(body) rescue {}
    strings = deep_collect_strings(json)
    return {} if strings.empty?

    select_label = strings.find { |s| s.include?("選択肢") } || strings.find { |s| s =~ /選択/ }

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
    patterns.each do |k, regs|
      flags[k] = regs.any? { |re| pool.any? { |s| s =~ re } }
    end
    flags[:select] = select_label
    flags
  rescue => e
    Rails.logger.debug("[extract_tag_flags] #{e.class}: #{e.message}")
    {}
  end

  # “name/label/title/text/value” といったキーから文字列を拾う
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

  # JSON 全体から文字列を収集
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

  def last_id_from_my_page(link)
    return nil if link.blank?
    link.to_s.split("/").last
  end
end
