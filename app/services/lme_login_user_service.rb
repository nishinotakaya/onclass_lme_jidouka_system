# frozen_string_literal: true

require "selenium-webdriver"
require "net/http"
require "json"
require "playwright"
require "time"
require "cgi"

class LmeLoginUserService
  LOG_PREFIX    = "[LmeLoginUserService]".freeze
  DUMP_ON_ERROR = ENV["LME_LOGIN_DUMP"] == "1"

  def initialize(email:, password:, api_key:)
    @email    = email
    @password = password
    @api_key  = api_key
  end

  # =========================================================
  # Public: セレニウムでログインして Cookie と driver を返す
  # =========================================================
  def login!
    service = Selenium::WebDriver::Chrome::Service.new(path: "/usr/bin/chromedriver")
    options = Selenium::WebDriver::Chrome::Options.new
    options.binary = "/usr/bin/chromium"
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--headless=new")
    tmpdir = Dir.mktmpdir("chrome_")
    options.add_argument("--user-data-dir=#{tmpdir}")
    options.add_argument("--profile-directory=Default")

    driver = Selenium::WebDriver.for(:chrome, service: service, options: options)
    driver.manage.timeouts.implicit_wait = 2
    wait   = Selenium::WebDriver::Wait.new(timeout: 30)

    begin
      log :info, "open / (トップ) へ遷移します"
      driver.navigate.to "https://step.lme.jp/"
      wait_for_ready_state(driver)
      log :debug, "初回: title=#{safe(driver.title)}, url=#{driver.current_url}"

      # 既ログイン疑い（セッションcookie or /admin|/basic の滞在）
      if already_logged_in?(driver)
        log :info, "既にログイン済みの兆候 → /basic/friendlist へ（BOT無し→ダメならBOT有り）"
        ensure_basic_session(driver)
        cookies    = driver.manage.all_cookies
        cookie_str = to_cookie_string(cookies)
        log_cookies_brief!(cookies)
        return { cookies: cookies, cookie_str: cookie_str, driver: driver }
      end

      # --- ログインフォーム操作（フォールバック付き） ---
      log :info, "ログインフォームへ入力"
      email_el = driver.find_elements(id: "email_login").first ||
                 driver.find_elements(css: "input[name='email']").first
      pass_el  = driver.find_elements(id: "password_login").first ||
                 driver.find_elements(css: "input[name='password']").first
      raise "ログインフォーム要素が見つかりません" unless email_el && pass_el

      email_el.send_keys(@email)
      pass_el.send_keys(@password)

      # --- reCAPTCHA v2 があれば 2Captcha（最大3回リトライ） ---
      if driver.find_elements(css: ".g-recaptcha").any?
        sitekey = driver.find_element(css: ".g-recaptcha").attribute("data-sitekey")
        token   = obtain_recaptcha_token_with_retries(sitekey, driver.current_url, tries: 3)
        inject_recaptcha_token!(driver, token)
      else
        log :info, "reCAPTCHA 要素なし → スキップ"
      end

      # --- 送信 ---
      login_btn = wait.until { driver.find_element(css: "button[type=submit]") }
      wait.until { login_btn.enabled? }
      log :info, "ログインボタンをクリック"
      login_btn.click

      # --- ログイン完了検知（一次）---
      log :info, "ログイン完了待ち（URL/cookie検知）"
      ok_once = wait_until(35) do
        !looks_like_login_page?(driver) && has_session_cookie?(driver)
      end
      log :debug, "一次判定=#{ok_once} title=#{safe(driver.title)} url=#{driver.current_url}"

      # --- 最終確認（/admin に居座れるか）---
      confirm_login_or_raise!(driver)

      ensure_basic_session(driver)

      cookies    = driver.manage.all_cookies
      cookie_str = to_cookie_string(cookies)
      log_cookies_brief!(cookies)

      { cookies: cookies, cookie_str: cookie_str, driver: driver }

    rescue => e
      log :error, "error: #{e.class} #{e.message}"
      e.backtrace&.first(8)&.each { |l| log :error, "  at #{l}" }
      dump_page(driver) if DUMP_ON_ERROR rescue nil
      driver.quit rescue nil
      raise
    end
  end

  # =========================================================
  # Public: Playwright に Cookie を引き継ぎ、/basic 側 Cookie & XSRF を安定させる
  # 返却形式は既存の呼び出しに合わせて login! の結果に ajax_result を足す
  # ※ 正しい Cookie/XSRF が取れなければ、reCAPTCHA を含めて 1 回だけ再試行
  # =========================================================
  def fetch_friend_history(loa_label: "プロアカ")
    attempts_left = 2
    last_result   = nil

    while attempts_left > 0
      login_result = login!
      ajax_result  = nil
      basic_cookie_header = nil
      basic_xsrf          = nil
      basic_url           = nil

      Playwright.create(playwright_cli_executable_path: "npx playwright") do |pw|
        log :info, "Playwright 起動（cookie引き継ぎ）"
        browser = pw.chromium.launch(headless: true, args: ["--no-sandbox", "--disable-dev-shm-usage"])
        context = browser.new_context
        begin
          # Cookie 正規化 → 追加
          normalized = normalize_cookies_for_pw(login_result[:cookies])
          begin
            context.add_cookies(normalized)
          rescue ArgumentError, Playwright::Error
            context.add_cookies(cookies: normalized)
          end
          log :debug, "PW: add_cookies=#{normalized.map{_1[:name]}.join(',')}"

          page = context.new_page
          # 別タブ阻止（同タブに遷移させる）
          begin
            page.add_init_script(script: "window.open = (u,t)=>{ location.href=u }")
          rescue ArgumentError, NoMethodError
            page.add_init_script("window.open = (u,t)=>{ location.href=u }")
          end

          # 任意の JSON レスポンス監視（デバッグ用）
          page.on("response", ->(res) {
            begin
              ctype = res.headers["content-type"].to_s.downcase
              next unless ctype.include?("json")
              body = res.body
              hit  = body.force_encoding("UTF-8").include?("プロアカ")
              if hit || res.url.include?("/get-categories-tags") || res.url.include?("/ajax/get-bot-data")
                log :debug, "[NET] #{res.status} #{res.url} プロアカ?=#{hit} HEAD=#{body[0,120]}"
              end
            rescue => e
              log :debug, "[NET] parse error #{e.class}: #{e.message}"
            end
          })

          # /admin → LOA 選択
          log :info, "PW: /admin/home へ"
          page.goto("https://step.lme.jp/admin/home")
          page.wait_for_load_state(state: "networkidle") rescue nil
          log :debug, "PW: admin url=#{page.url}"

          # LOA（例: プロアカ）をクリックして /basic/ へ
          clicked = false
          ["tr:has-text('#{loa_label}') >> a:has-text('#{loa_label}')",
           "a:has-text('#{loa_label}')",
           "text=#{loa_label}"].each do |sel|
            begin
              count = page.locator(sel).count
              if count > 0
                log :info, "PW: クリック '#{sel}' (count=#{count})"
                page.locator(sel).first.click
                clicked = true
                break
              end
            rescue => e
              log :debug, "locator '#{sel}' error: #{e.class} #{e.message}"
            end
          end
          unless clicked
            log :warn, "PW: '#{loa_label}' ロケータ見つからず → 行クリックで代替"
            row = page.locator("tr:has-text('#{loa_label}')").first rescue nil
            row&.click
          end

          reached = pw_reached_basic?(page, timeout_ms: 15_000)
          unless reached
            log :warn, "PW: /basic 判定NG → /basic/friendlist 直遷移を試行"
            page.goto("https://step.lme.jp/basic/friendlist") rescue nil
            begin
              page.wait_for_url(%r{/basic/}, timeout: 10_000)
              reached = true
            rescue
              reached = page.url.include?("/basic/")
            end
          end
          basic_url = page.url.to_s
          log :debug, "PW: after select url=#{basic_url} reached_basic=#{reached}"

          # 友だち履歴を踏んで XSRF 更新を誘発（保険）
          if reached
            begin
              page.goto("https://step.lme.jp/basic/friendlist/friend-history")
              page.wait_for_load_state(state: "networkidle") rescue nil
            rescue => e
              log :debug, "friend-history warn: #{e.class} #{e.message}"
            end
          end

          # Cookie / XSRF 抽出
          pl_cookies = begin
            context.cookies(["https://step.lme.jp"])
          rescue
            context.cookies
          end
          names = Array(pl_cookies).map { |c| (c["name"] || c[:name]).to_s }
          log :debug, "PW cookies(basic?): #{names.join(', ')}"

          basic_cookie_header = Array(pl_cookies).map { |c|
            "#{(c["name"]||c[:name])}=#{(c["value"]||c[:value])}"
          }.join("; ")

          xsrf_cookie = Array(pl_cookies).find { |c| (c["name"] || c[:name]) == "XSRF-TOKEN" }
          xsrf_raw    = xsrf_cookie && (cval = (cval = xsrf_cookie["value"] || xsrf_cookie[:value]))
          basic_xsrf  = xsrf_raw && CGI.unescape(xsrf_raw.to_s)
          # debugger # todo次ここで止めてCookieのとトークン確認
          log :debug, "PW XSRF head=#{basic_xsrf.to_s[0,10]}"

          # 任意スクショ（Playwrightのfull_pageはバージョン依存のため無指定）
          begin
            page.screenshot(path: "/tmp/after_basic.png")
            log :debug, "screenshot: /tmp/after_basic.png"
          rescue => e
            log :debug, "screenshot error: #{e.class} #{e.message}"
          end

        ensure
          context&.close rescue nil
          browser&.close rescue nil
        end
      end

      last_result = {
        **login_result,
        ajax_result: ajax_result,
        basic_cookie_header: basic_cookie_header,
        basic_xsrf: basic_xsrf,
        basic_url: basic_url
      }

      # ここで「正しいCookieを取得できたか」をチェック。NGなら 2Captcha を含めてもう一周。
      if valid_basic_session?(basic_cookie_header, basic_xsrf, basic_url)
        return last_result
      else
        attempts_left -= 1
        log :warn, "basic cookie/xsrf 判定NG → 二段階認証(reCAPTCHA)を含め再試行 (残り#{attempts_left})"
      end
    end

    # 最後に得られたものを返す（呼び出し側でログを確認できるように）
    last_result
  end

  # =========================================================
  # 2Captcha
  # =========================================================
  def solve_recaptcha(sitekey, url)
    api_key = @api_key
    uri = URI("http://2captcha.com/in.php")
    res = Net::HTTP.post_form(uri, {
      "key"       => api_key,
      "method"    => "userrecaptcha",
      "googlekey" => sitekey,
      "pageurl"   => url,
      "json"      => 1
    })
    result = JSON.parse(res.body) rescue {}
    raise "2Captcha error: #{res.body}" unless result["status"] == 1

    request_id = result["request"]
    log :debug, "2Captcha request_id=#{request_id}"
    sleep 20
    loop do
      uri_res = URI("http://2captcha.com/res.php?key=#{api_key}&action=get&id=#{request_id}&json=1")
      res2 = Net::HTTP.get_response(uri_res)
      json = JSON.parse(res2.body) rescue {}
      if json["status"] == 1
        return json["request"]
      elsif json["request"] == "CAPCHA_NOT_READY"
        sleep 5
      else
        raise "2Captcha solve failed: #{res2.body}"
      end
    end
  end

  # =========================================================
  # 内部ヘルパ
  # =========================================================
  private

  # reCAPTCHA 取得を最大 tries 回リトライ（ERROR_CAPTCHA_UNSOLVABLE 対応）
  def obtain_recaptcha_token_with_retries(sitekey, url, tries: 3)
    attempt = 0
    last_err = nil
    while attempt < tries
      attempt += 1
      log :info, "reCAPTCHA 検出 → 2Captcha に投げます (try=#{attempt}/#{tries})"
      begin
        token = solve_recaptcha(sitekey, url)
        log :debug, "captcha token 取得 (length=#{token.to_s.length})"
        return token
      rescue => e
        last_err = e
        msg = e.message.to_s
        if msg.include?("ERROR_CAPTCHA_UNSOLVABLE")
          log :warn, "2Captcha unsolvable → リトライ"
          sleep 5
          next
        else
          log :error, "2Captcha unexpected error: #{e.class} #{e.message}"
          raise
        end
      end
    end
    raise last_err || "2Captcha could not solve after #{tries} tries"
  end

  # 既ログインかどうか厳密判定
  def already_logged_in?(driver)
    if looks_like_login_page?(driver)
      log :debug, "already_logged_in?: login page detected → false"
      return false
    end

    begin
      driver.navigate.to "https://step.lme.jp/admin/home"
      wait_for_ready_state(driver)
      url         = driver.current_url.to_s
      in_basic    = url.include?("/admin") || url.include?("/basic/")
      cookies_ok  = has_session_cookie?(driver)
      log :debug, "LOGIN_PROBE: url=#{url} in_basic=#{in_basic} cookies_ok=#{cookies_ok}"
      (in_basic && cookies_ok)
    rescue => e
      log :warn, "LOGIN_PROBE error: #{e.class} #{e.message}"
      false
    end
  end

  def looks_like_login_page?(driver)
    title = driver.title.to_s
    url   = driver.current_url.to_s
    has_form = driver.find_elements(id: "email_login").any? ||
               driver.find_elements(css: "input[name='email']").any?
    is_login_title = title.include?("ログイン") || title.downcase.include?("login")
    is_login_url   = url.end_with?("/") || url.include?("/login")
    log :debug, "LOGIN_LOOKS: title=#{safe(title)} url=#{url} form=#{has_form} is_login_title=#{is_login_title} is_login_url=#{is_login_url}"
    has_form || is_login_title || is_login_url
  end

  def confirm_login_or_raise!(driver)
    driver.navigate.to "https://step.lme.jp/admin/home"
    wait_for_ready_state(driver)
    url = driver.current_url.to_s
    ok  = (url.include?("/admin") || url.include?("/basic/"))
    log :info, "LOGIN_CONFIRM: url=#{url} ok=#{ok}"
    raise "Login not confirmed (still at #{url})" unless ok
  end

  def ensure_basic_session(driver)
    driver.navigate.to "https://step.lme.jp/basic/friendlist"
    wait_for_ready_state(driver)
    log :debug, "friendlist(無) title=#{safe(driver.title)} url=#{driver.current_url}"
    unless driver.current_url.include?("/basic/")
      bot_url = "https://step.lme.jp/basic/friendlist?botIdCurrent=#{ENV['LME_BOT_ID']}&isOtherBot=1"
      log :warn, "BOT未指定では /basic に入れず → #{bot_url} を試します"
      driver.navigate.to bot_url
      wait_for_ready_state(driver)
      log :debug, "friendlist(有) title=#{safe(driver.title)} url=#{driver.current_url}"
    end
  end

  def has_session_cookie?(driver)
    names = driver.manage.all_cookies.map { |c| c[:name].to_s }
    names.include?("laravel_session") || names.include?("XSRF-TOKEN")
  end

  def inject_recaptcha_token!(driver, token)
    driver.execute_script(<<~JS, token)
      (function(tok){
        let f = document.querySelector('#g-recaptcha-response');
        if(!f){
          f = document.createElement('textarea');
          f.id = 'g-recaptcha-response';
          f.name = 'g-recaptcha-response';
          f.style = 'display:none;';
          document.body.appendChild(f);
        }
        f.value = tok;
        f.dispatchEvent(new Event('change', { bubbles: true }));
      })(arguments[0]);
    JS
    val = driver.execute_script("return document.querySelector('#g-recaptcha-response') && document.querySelector('#g-recaptcha-response').value;")
    log :debug, "g-recaptcha-response length=#{val.to_s.length}"
  end

  def wait_for_ready_state(driver, timeout = 20)
    deadline = Time.now + timeout
    until Time.now > deadline
      state = driver.execute_script("return document.readyState")
      return true if state == "complete"
      sleep 0.2
    end
    false
  end

  def wait_until(sec = 15, &blk)
    deadline = Time.now + sec
    loop do
      return true if blk.call
      return false if Time.now > deadline
      sleep 0.3
    end
  end

  def normalize_cookies_for_pw(raw)
    Array(raw).map do |c|
      h = c.respond_to?(:to_h) ? c.to_h : c
      http_only_flag = h[:http_only] || h["http_only"] || h[:httponly] || h["httponly"] || false
      cookie = {
        name:     (h[:name]  || h["name"]).to_s,
        value:    (h[:value] || h["value"]).to_s,
        domain:   (h[:domain] || h["domain"] || "step.lme.jp").to_s,
        path:     (h[:path]   || h["path"]   || "/").to_s,
        httpOnly: !!http_only_flag,
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
  end

  def to_cookie_string(cookies)
    Array(cookies).map { |c| "#{c[:name]}=#{c[:value]}" }.join("; ")
  end

  def log_cookies_brief!(cookies)
    names   = Array(cookies).map { |c| c[:name] }.join(", ")
    laravel = Array(cookies).find { |c| c[:name] == "laravel_session" }&.dig(:value)
    xsrf    = Array(cookies).find { |c| c[:name] == "XSRF-TOKEN" }&.dig(:value)
    log :debug, "cookies: #{names}"
    log :debug, "laravel_session?=#{!laravel.to_s.empty?} xsrf_head=#{(CGI.unescape(xsrf.to_s)[0,10])}"
  end

  def dump_page(driver)
    ts   = Time.now.strftime("%Y%m%d-%H%M%S")
    png  = "/tmp/lme_login_fail_#{ts}.png"
    html = "/tmp/lme_login_fail_#{ts}.html"
    begin
      driver.save_screenshot(png)
      File.write(html, driver.page_source.to_s)
      log :warn, "Dumped screenshot=#{png} html=#{html} url=#{driver.current_url} title=#{safe(driver.title)}"
    rescue => e
      log :warn, "dump failed: #{e.class} #{e.message}"
    end
  end

  def valid_basic_session?(cookie_header, xsrf, url)
    return false if cookie_header.to_s.empty? || xsrf.to_s.empty?
    has_laravel = cookie_header.include?("laravel_session=")
    has_xsrf    = cookie_header.include?("XSRF-TOKEN=")
    in_basic    = url.to_s.include?("/basic/")
    has_laravel && has_xsrf && in_basic
  end

  def safe(s, n = 60)
    s.to_s[0, n]
  end

  def log(level, msg)
    if defined?(Rails) && Rails.respond_to?(:logger) && Rails.logger
      case level
      when :debug then Rails.logger.debug("#{LOG_PREFIX} #{msg}")
      when :info  then Rails.logger.info("#{LOG_PREFIX} #{msg}")
      when :warn  then Rails.logger.warn("#{LOG_PREFIX} #{msg}")
      when :error then Rails.logger.error("#{LOG_PREFIX} #{msg}")
      else             Rails.logger.info("#{LOG_PREFIX} #{msg}")
      end
    else
      $stdout.puts "#{LOG_PREFIX} #{level.upcase}: #{msg}"
    end
  end

  def pw_reached_basic?(page, timeout_ms: 15_000)
    begin
      page.wait_for_url(%r{/basic/(overview|friendlist|chat-v3)}, timeout: timeout_ms)
    rescue Playwright::TimeoutError
      # URLでダメでも左ナビ文言で判定
    end
    url_ok  = page.url.to_s.include?("/basic/")
    text_ok = begin
      txt = page.evaluate("document.body && document.body.innerText || ''")
      txt.include?("友だちリスト") || txt.include?("チャット管理")
    rescue
      false
    end
    url_ok || text_ok
  end
end
