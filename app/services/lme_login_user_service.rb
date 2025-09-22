# app/services/lme_login_user_service.rb
require "selenium-webdriver"
require "net/http"
require "json"
require "playwright"
require "time"

class LmeLoginUserService
  def initialize(email:, password:, api_key:)
    @email    = email
    @password = password
    @api_key  = api_key
  end

  def login!
    service = Selenium::WebDriver::Chrome::Service.new(path: "/usr/bin/chromedriver")
    options = Selenium::WebDriver::Chrome::Options.new
    options.binary = "/usr/bin/chromium"
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--headless=new")
    options.add_argument("--user-data-dir=/tmp/chrome_profile")
    options.add_argument("--profile-directory=Default")
    options.add_argument("--remote-debugging-port=9222")

    driver = Selenium::WebDriver.for(:chrome, service: service, options: options)
    wait   = Selenium::WebDriver::Wait.new(timeout: 30)

    begin
      driver.navigate.to "https://step.lme.jp/"
      if driver.title.include?("LOA選択") || driver.current_url.include?("admin")
        cookies    = driver.manage.all_cookies
        cookie_str = cookies.map { |c| "#{c[:name]}=#{c[:value]}" }.join("; ")
        return { cookies: cookies, cookie_str: cookie_str, driver: driver }
      end

      # --- ID / PASS 入力 ---
      driver.find_element(id: "email_login").send_keys(@email)
      driver.find_element(id: "password_login").send_keys(@password)

      # --- reCAPTCHA sitekey 取得 → 2Captcha ---
      sitekey = driver.find_element(css: ".g-recaptcha").attribute("data-sitekey")
      token   = solve_recaptcha(sitekey, driver.current_url)

      # --- トークン注入 & submit ---
      driver.execute_script(<<~JS, token)
        const responseField = document.querySelector('#g-recaptcha-response');
        if (responseField) {
          responseField.value = arguments[0];
          responseField.dispatchEvent(new Event('change', { bubbles: true }));
        }
      JS
      login_btn = wait.until { driver.find_element(css: "button[type=submit]") }
      wait.until { login_btn.enabled? }
      login_btn.click

      cookies    = driver.manage.all_cookies
      cookie_str = cookies.map { |c| "#{c[:name]}=#{c[:value]}" }.join("; ")
      { cookies: cookies, cookie_str: cookie_str, driver: driver }
    rescue => e
      puts "[LmeLoginUserService] error: #{e.class} #{e.message}"
      driver.quit rescue nil
      raise
    end
  end

  # ログイン済みの Cookie を Playwright に引き継ぎ、/basic セッションを確定
  def fetch_friend_history
    login_result = login!
    ajax_result = nil

    Playwright.create(playwright_cli_executable_path: "npx playwright") do |pw|
      # Fallback 用に自前の headless を起動
      launched_browser = pw.chromium.launch(
        headless: true,
        args: ["--no-sandbox", "--disable-dev-shm-usage"]
      )

      # 既存の Chromium に CDP で接続（起動済みなら使う）
      connected_browser = nil
      begin
        connected_browser = pw.chromium.connect_over_cdp("http://localhost:9222")
      rescue
        # つながらなくても OK（fallback）
      end

      context =
        if connected_browser && !connected_browser.contexts.empty?
          connected_browser.contexts.first
        else
          launched_browser.new_context
        end

      normalized = Array(login_result[:cookies]).map do |c|
        h = c.respond_to?(:to_h) ? c.to_h : c
        cookie = {
          name:     (h[:name]  || h["name"]).to_s,
          value:    (h[:value] || h["value"]).to_s,
          domain:   (h[:domain] || h["domain"] || "step.lme.jp").to_s,
          path:     (h[:path]   || h["path"]   || "/").to_s,
          httpOnly: !!(h[:http_only] || h["http_only"] || h[:httponny] || h["httponly"] || h["httpOnly"]),
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

      # ★ キーワード引数必須
      # context.add_cookies(cookies: normalized)

      page = context.new_page

      # Ajax レスポンス監視（デバッグ）
      page.on("response", ->(res) {
        if res.url.include?("init-data-history-add-friend")
          puts "[CAPTURED] #{res.url} (status: #{res.status})"
          begin
            body = res.body
            ajax_result = JSON.parse(body) rescue nil
          rescue => e
            puts "[Parse Error] #{e.class} #{e.message}"
          end
        end
      })

      # /admin → /basic/overview へ
      page.goto("https://step.lme.jp/admin/home")
      page.goto("https://step.lme.jp/basic/overview")
      page.wait_for_load_state(state: "networkidle") rescue nil

      # ここでは最新 Cookie の育成だけが目的
      launched_browser.close rescue nil
      connected_browser&.close rescue nil
    end

    { **login_result, ajax_result: ajax_result }
  end



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
    result = JSON.parse(res.body)
    raise "2Captcha error: #{result}" unless result["status"] == 1

    request_id = result["request"]
    sleep 20
    loop do
      uri_res = URI("http://2captcha.com/res.php?key=#{api_key}&action=get&id=#{request_id}&json=1")
      res2 = Net::HTTP.get_response(uri_res)
      json = JSON.parse(res2.body)
      if json["status"] == 1
        return json["request"]
      elsif json["request"] == "CAPCHA_NOT_READY"
        sleep 5
      else
        raise "2Captcha solve failed: #{json}"
      end
    end
  end
end
