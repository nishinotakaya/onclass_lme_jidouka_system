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

  # =========================
  # ログイン処理
  # =========================
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

      if already_logged_in?(driver)
        log :info, "既にログイン済みの兆候 → /basic/friendlist"
        ensure_basic_session(driver)
        cookies    = driver.manage.all_cookies
        cookie_str = to_cookie_string(cookies)
        log_cookies_brief!(cookies)
        return { cookies: cookies, cookie_str: cookie_str, driver: driver }
      end

      log :info, "ログインフォームへ入力"
      email_el = driver.find_elements(id: "email_login").first ||
                 driver.find_elements(css: "input[name='email']").first
      pass_el  = driver.find_elements(id: "password_login").first ||
                 driver.find_elements(css: "input[name='password']").first
      raise "ログインフォーム要素が見つかりません" unless email_el && pass_el

      email_el.send_keys(@email)
      pass_el.send_keys(@password)

      if driver.find_elements(css: ".g-recaptcha").any?
        sitekey = driver.find_element(css: ".g-recaptcha").attribute("data-sitekey")
        token   = obtain_recaptcha_token_with_retries(sitekey, driver.current_url, tries: 3)
        inject_recaptcha_token!(driver, token)
      else
        log :info, "reCAPTCHA 要素なし → スキップ"
      end

      login_btn = wait.until { driver.find_element(css: "button[type=submit]") }
      wait.until { login_btn.enabled? }
      log :info, "ログインボタンをクリック"
      login_btn.click

      log :info, "ログイン完了待ち（URL/cookie検知）"
      ok_once = wait_until(35) do
        !looks_like_login_page?(driver) && has_session_cookie?(driver)
      end
      log :debug, "一次判定=#{ok_once} title=#{safe(driver.title)} url=#{driver.current_url}"

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

  # =========================
  # フレンド履歴取得
  # =========================
  def fetch_friend_history(loa_label: "プロアカ")
    attempts_left = 2
    last_result   = nil

    while attempts_left > 0
      login_result = login!
      basic_cookie_header = nil
      basic_xsrf          = nil
      basic_url           = nil
      final_cookies       = nil
      raw_cookie_header   = nil

      Playwright.create(playwright_cli_executable_path: "npx playwright") do |pw|
        log :info, "Playwright 起動（cookie引き継ぎ）"
        browser = pw.chromium.launch(headless: true, args: ["--no-sandbox", "--disable-dev-shm-usage"])
        context = browser.new_context
        begin
          normalized = normalize_cookies_for_pw(login_result[:cookies])
          context.add_cookies(normalized) rescue context.add_cookies(cookies: normalized)

          page = context.new_page
          log :info, "PW: /admin/home へ"
          page.goto("https://step.lme.jp/admin/home")
          page.wait_for_load_state(state: "networkidle") rescue nil
          log :debug, "PW: admin url=#{page.url}"

          ["tr:has-text('#{loa_label}') >> a:has-text('#{loa_label}')",
           "a:has-text('#{loa_label}')",
           "text=#{loa_label}"].each do |sel|
            begin
              if page.locator(sel).count > 0
                log :info, "PW: クリック '#{sel}'"
                page.locator(sel).first.click
                break
              end
            rescue => e
              log :debug, "locator '#{sel}' error: #{e.class} #{e.message}"
            end
          end

          page.goto("https://step.lme.jp/basic/friendlist") rescue nil
          page.wait_for_url(%r{/basic/}, timeout: 10_000) rescue nil
          basic_url = page.url.to_s
          log :debug, "PW: after select url=#{basic_url}"

          # cookies取得（必ず引数なしで呼ぶ！）
          pl_cookies = context.cookies

          if pl_cookies.nil? || pl_cookies.empty?
            log :warn, "PW: cookies 取得できず"
            pl_cookies = []
          end

          final_cookies     = pl_cookies
          raw_cookie_header = pl_cookies.map { |c| "#{c["name"]}=#{c["value"]}" }.join("; ")
          basic_cookie_header = sanitize_cookie_header(raw_cookie_header)

          xsrf_cookie = pl_cookies.find { |c| (c["name"] || c[:name]) == "XSRF-TOKEN" }
          xsrf_raw    = xsrf_cookie && (c = xsrf_cookie["value"] || xsrf_cookie[:value])
          basic_xsrf  = xsrf_raw && CGI.unescape(xsrf_raw.to_s)

          log :debug, "[cookies raw] #{raw_cookie_header.presence || '(none)'}"
          log :debug, "[cookies sanitized] #{basic_cookie_header.presence || '(none)'}"
          log :debug, "PW XSRF head=#{basic_xsrf.to_s[0,10]}"
        ensure
          context&.close rescue nil
          browser&.close rescue nil
        end
      end

      last_result = {
        basic_cookie_header: basic_cookie_header,
        basic_xsrf: basic_xsrf,
        cookies: final_cookies,
        basic_url: basic_url,
        raw_cookie_header: raw_cookie_header
      }

      if valid_basic_session?(basic_cookie_header, basic_xsrf, basic_url)
        return last_result
      else
        attempts_left -= 1
        log :warn, "basic cookie/xsrf 判定NG → 再試行 (残り#{attempts_left})"
      end
    end
    last_result
  end

  # =========================
  # Cookieヘッダー整形
  # =========================
  def sanitize_cookie_header(cookie_header)
    keep = %w[laravel_session XSRF-TOKEN ROUTEID]
    return "" if cookie_header.nil? || cookie_header.strip.empty?
    cookie_header.split(/;\s*/).select { |kv|
      key = kv.split('=').first
      keep.include?(key)
    }.join('; ')
  end

  # =========================
  # 2Captcha
  # =========================
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
        if e.message.include?("ERROR_CAPTCHA_UNSOLVABLE")
          log :warn, "2Captcha unsolvable → リトライ"
          sleep 5
          next
        else
          raise
        end
      end
    end
    raise last_err || "2Captcha could not solve after #{tries} tries"
  end

  def already_logged_in?(driver)
    if looks_like_login_page?(driver)
      return false
    end
    begin
      driver.navigate.to "https://step.lme.jp/admin/home"
      wait_for_ready_state(driver)
      url         = driver.current_url.to_s
      in_basic    = url.include?("/admin") || url.include?("/basic/")
      cookies_ok  = has_session_cookie?(driver)
      (in_basic && cookies_ok)
    rescue
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
    has_form || is_login_title || is_login_url
  end

  def confirm_login_or_raise!(driver)
    driver.navigate.to "https://step.lme.jp/admin/home"
    wait_for_ready_state(driver)
    url = driver.current_url.to_s
    ok  = (url.include?("/admin") || url.include?("/basic/"))
    raise "Login not confirmed (still at #{url})" unless ok
  end

  def ensure_basic_session(driver)
    driver.navigate.to "https://step.lme.jp/basic/friendlist"
    wait_for_ready_state(driver)
    unless driver.current_url.include?("/basic/")
      bot_url = "https://step.lme.jp/basic/friendlist?botIdCurrent=#{ENV['LME_BOT_ID']}&isOtherBot=1"
      driver.navigate.to bot_url
      wait_for_ready_state(driver)
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
      {
        name:     (h[:name]  || h["name"]).to_s,
        value:    (h[:value] || h["value"]).to_s,
        domain:   (h[:domain] || h["domain"] || "step.lme.jp").to_s,
        path:     (h[:path]   || h["path"]   || "/").to_s,
        httpOnly: !!(h[:http_only] || h["http_only"] || h[:httponly] || h["httponly"]),
        secure:   true
      }.compact
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
      Rails.logger.public_send(level, "#{LOG_PREFIX} #{msg}")
    else
      $stdout.puts "#{LOG_PREFIX} #{level.upcase}: #{msg}"
    end
  end
end
