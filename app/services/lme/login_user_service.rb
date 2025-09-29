# frozen_string_literal: true

require "selenium-webdriver"
require "net/http"
require "json"
require "time"
require "cgi"
require "tmpdir"
require "securerandom"
require "fileutils"

module Lme
  class LoginUserService
    LOG_PREFIX    = "[LmeLoginUserService]".freeze
    DUMP_ON_ERROR = ENV["LME_LOGIN_DUMP"] == "1"

    BASIC_TARGET_PATH    = "/basic/overview".freeze
    BASIC_FALLBACK       = "/basic/friendlist".freeze
    RECAPTCHA_MAX_SOLVES = (ENV["RECAPTCHA_MAX_SOLVES"] || "1").to_i

    def initialize(email:, password:, api_key:)
      @email    = email
      @password = password
      @api_key  = api_key
    end

    # =========================
    # 1) ログイン (Selenium)
    # =========================
    def login!
      # ChromeDriver: Selenium Manager か CHROMEDRIVER_PATH を使用
      service =
        if ENV["CHROMEDRIVER_PATH"].to_s.strip.empty?
          Selenium::WebDriver::Service.chrome
        else
          Selenium::WebDriver::Service.chrome(path: ENV["CHROMEDRIVER_PATH"])
        end

      options = Selenium::WebDriver::Chrome::Options.new

      # CFT/旧buildpack 互換の Chrome バイナリを探索
      chrome_bin = [ENV["GOOGLE_CHROME_SHIM"], ENV["CHROME_BIN"], ENV["GOOGLE_CHROME_BIN"]]
                    .compact.map!(&:to_s).map!(&:strip)
                    .find { |v| !v.empty? && File.exist?(v) }
      options.binary = chrome_bin if chrome_bin

      # 安定起動フラグ
      options.add_argument("--no-sandbox")
      options.add_argument("--disable-dev-shm-usage")
      options.add_argument("--disable-gpu")
      options.add_argument("--headless=new")
      options.add_argument("--disable-features=IsolateOrigins,site-per-process")
      options.add_argument("--window-size=1200,900")

      # セッション分離用プロファイル
      @tmp_profile_dir = Dir.mktmpdir("chrome_")
      options.add_argument("--user-data-dir=#{@tmp_profile_dir}")
      options.add_argument("--profile-directory=Default")

      driver = Selenium::WebDriver.for(:chrome, service: service, options: options)

      # タイムアウト（緩め）
      begin
        driver.manage.timeouts.implicit_wait = 2
        driver.manage.timeouts.page_load = 60
        driver.manage.timeouts.script_timeout = 30
      rescue; end

      begin
        log :info, "open / (トップ) へ遷移します"
        safe_selenium_goto(driver, "#{base_url}/", tries: 3)
        wait_for_ready_state(driver)
        log :debug, "初回: title=#{safe(driver.title)}, url=#{driver.current_url}"

        if already_logged_in?(driver)
          log :info, "既にログイン済みの兆候 → #{BASIC_FALLBACK}"
          ensure_basic_session(driver)
          cookies = driver.manage.all_cookies
          log_cookies_brief!(cookies)
          return { cookies: cookies, driver: driver }
        end

        # 入力
        email_el = driver.find_elements(id: "email_login").first ||
                   driver.find_elements(css: "input[name='email']").first
        pass_el  = driver.find_elements(id: "password_login").first ||
                   driver.find_elements(css: "input[name='password']").first
        raise "ログインフォーム要素が見つかりません" unless email_el && pass_el

        email_el.send_keys(@email)
        pass_el.send_keys(@password)

        # reCAPTCHA
        if driver.find_elements(css: ".g-recaptcha").any?
          sitekey = driver.find_element(css: ".g-recaptcha").attribute("data-sitekey")
          token   = obtain_recaptcha_token_with_retries(sitekey, driver.current_url, tries: RECAPTCHA_MAX_SOLVES)
          inject_recaptcha_token!(driver, token)
        else
          log :info, "reCAPTCHA 要素なし → スキップ"
        end

        # 送信
        btn = wait_until(10) { driver.find_element(css: "button[type=submit]") && driver.find_element(css: "button[type=submit]").enabled? }
        log :info, "ログインボタンをクリック"
        driver.find_element(css: "button[type=submit]").click

        # セッション成立待ち
        ok_once = wait_until(35) { !looks_like_login_page?(driver) && has_session_cookie?(driver) }
        log :debug, "一次判定=#{ok_once} title=#{safe(driver.title)} url=#{driver.current_url}"
        confirm_login_or_raise!(driver)

        ensure_basic_session(driver) # /basic/friendlist を踏む
        cookies = driver.manage.all_cookies
        log_cookies_brief!(cookies)
        { cookies: cookies, driver: driver }
      rescue => e
        log :error, "error: #{e.class} #{e.message}"
        e.backtrace&.first(8)&.each { |l| log :error, "  at #{l}" }
        dump_page(driver)
        driver.quit rescue nil
        FileUtils.remove_entry_secure(@tmp_profile_dir) rescue nil
        raise
      end
    end

    # =========================
    # 2) /basic へ“必ず”入って Cookie/XSRF を確定 (Seleniumのみ)
    # =========================
    def fetch_friend_history(loa_label: "プロアカ")
      login_result = login!
      driver = login_result[:driver]

      basic_cookie_header = nil
      basic_xsrf          = nil
      basic_url           = nil
      final_cookies       = nil
      raw_cookie_header   = nil

      begin
        # admin/home → LOA 確定
        safe_selenium_goto(driver, "#{base_url}/admin/home", tries: 3)
        wait_for_ready_state(driver)
        choose_loa_if_needed_selenium(driver, loa_label)

        # /basic へ遷移（まず friendlist → ダメなら overview）
        bot_id = ENV["LME_BOT_ID"].to_s.strip
        href_friend = build_basic_url(BASIC_FALLBACK,  bot_id: bot_id, ts: now_ms)
        safe_selenium_goto(driver, href_friend, tries: 3)
        wait_for_ready_state(driver)

        unless selenium_reached_basic?(driver)
          href_over = build_basic_url(BASIC_TARGET_PATH, bot_id: bot_id, ts: now_ms)
          safe_selenium_goto(driver, href_over, tries: 3)
          wait_for_ready_state(driver)
        end

        raise "basicエリアに入れませんでした (url=#{driver.current_url})" unless selenium_reached_basic?(driver)

        # 仕上げ（任意）overview をもう一度
        begin
          over = build_basic_url(BASIC_TARGET_PATH, bot_id: bot_id, ts: now_ms)
          safe_selenium_goto(driver, over, tries: 2)
          wait_for_ready_state(driver)
        rescue; end
        basic_url = driver.current_url.to_s

        # Cookie/XSRF 確定
        sel_cookies = driver.manage.all_cookies
        final_cookies     = sel_cookies
        raw_cookie_header = sel_cookies.map { |c| "#{c[:name]}=#{c[:value]}" }.join("; ")
        basic_cookie_header = sanitize_cookie_header(raw_cookie_header)

        xsrf_cookie = sel_cookies.find { |c| c[:name] == "XSRF-TOKEN" }&.dig(:value)
        basic_xsrf  = xsrf_cookie ? CGI.unescape(xsrf_cookie.to_s) : dom_csrf_token_selenium(driver)
        log :debug, "[cookies sanitized] #{basic_cookie_header || '(none)'}"
        log :debug, "XSRF head=#{basic_xsrf.to_s[0,10]}"
      ensure
        # 必要に応じてここでクローズ
        # driver.quit rescue nil
        # FileUtils.remove_entry_secure(@tmp_profile_dir) rescue nil
      end

      result = {
        basic_cookie_header: basic_cookie_header,
        basic_xsrf:          basic_xsrf,
        cookies:             final_cookies,
        basic_url:           basic_url,
        raw_cookie_header:   raw_cookie_header,
      }

      unless valid_basic_session?(result[:basic_cookie_header], result[:basic_xsrf])
        log :error, "basic cookie/xsrf 判定NG（#{result[:basic_url]}）"
        raise "basicへの到達またはCookie/XSRFの確定に失敗しました"
      end
      result
    end

    # =========================
    # ヘルパ（Selenium）
    # =========================

    def choose_loa_if_needed_selenium(driver, loa_label)
      # 出てなければ確定済み
      begin
        elems = driver.find_elements(:xpath, "//*[contains(normalize-space(text()), '#{loa_label}')]")
        return if elems.empty?
      rescue
        return
      end
      # 素直にクリック
      begin
        elems = driver.find_elements(:xpath, "//*[contains(normalize-space(text()), '#{loa_label}')]")
        elems.first&.click
        sleep 0.4
      rescue => e
        log :debug, "LOAクリック失敗: #{e.class} #{e.message}"
      end
    end

    def selenium_reached_basic?(driver)
      url_ok  = driver.current_url.to_s.include?("/basic/")
      text_ok = begin
        html = driver.page_source.to_s
        html.include?("友だちリスト") || html.include?("チャット管理")
      rescue
        false
      end
      url_ok || text_ok
    end

    def safe_selenium_goto(driver, url, tries: 2, base_backoff: 0.5)
      tries.times do |i|
        begin
          driver.navigate.to url
          return true
        rescue => e
          log :debug, "Selenium goto失敗 try=#{i + 1}/#{tries}: #{e.class} #{e.message}"
          sleep(base_backoff * (i + 1))
        end
      end
      false
    end

    def dom_csrf_token_selenium(driver)
      driver.execute_script(<<~JS).to_s
        (function(){
          var m = document.querySelector('meta[name="csrf-token"]');
          return m ? m.getAttribute('content') : '';
        })();
      JS
    rescue
      ""
    end

    # =========================
    # 共通ヘルパ
    # =========================

    def base_url
      ENV["LME_BASE_URL"].presence || "https://step.lme.jp"
    end

    def build_basic_url(path, bot_id:, ts:)
      base = "#{base_url}#{path}"
      q = []
      q << "botIdCurrent=#{CGI.escape(bot_id)}" unless bot_id.to_s.empty?
      q << "isOtherBot=1" unless bot_id.to_s.empty?
      q << "_ts=#{ts}"
      q.empty? ? base : "#{base}?#{q.join('&')}"
    end

    def sanitize_cookie_header(cookie_header)
      keep = %w[laravel_session XSRF-TOKEN ROUTEID]
      return "" if cookie_header.nil? || cookie_header.strip.empty?
      pairs = cookie_header.split(/;\s*/).map { |kv| kv.split("=", 2) }.select { |k,_| k && _ }
      uniq  = {}; pairs.each { |k, v| uniq[k] = v } # 後勝ち
      keep.map { |k| "#{k}=#{uniq[k]}" if uniq[k] }.compact.join("; ")
    end

    # ===== reCAPTCHA =====

    def obtain_recaptcha_token_with_retries(sitekey, url, tries: RECAPTCHA_MAX_SOLVES)
      attempt  = 0
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

    def solve_recaptcha(sitekey, url)
      uri = URI("http://2captcha.com/in.php")
      res = Net::HTTP.post_form(uri, {
        "key"       => @api_key,
        "method"    => "userrecaptcha",
        "googlekey" => sitekey,
        "pageurl"   => url,
        "json"      => 1
      })
      json = JSON.parse(res.body) rescue {}
      raise "2Captcha error: #{res.body}" unless json["status"] == 1

      request_id = json["request"]
      log :debug, "2Captcha request_id=#{request_id}"
      sleep 2
      loop do
        uri_res = URI("http://2captcha.com/res.php?key=#{@api_key}&action=get&id=#{request_id}&json=1")
        res2 = Net::HTTP.get_response(uri_res)
        j2 = JSON.parse(res2.body) rescue {}
        return j2["request"] if j2["status"] == 1
        raise "2Captcha solve failed: #{res2.body}" unless j2["request"] == "CAPCHA_NOT_READY"
        sleep 5
      end
    end

    # ===== 汎用ユーティリティ =====

    def already_logged_in?(driver)
      return false if looks_like_login_page?(driver)
      safe_selenium_goto(driver, "#{base_url}/admin/home", tries: 2)
      wait_for_ready_state(driver)
      url        = driver.current_url.to_s
      cookies_ok = has_session_cookie?(driver)
      (url.include?("/admin") || url.include?("/basic/")) && cookies_ok
    rescue
      false
    end

    def looks_like_login_page?(driver)
      t = driver.title.to_s
      u = driver.current_url.to_s
      has_form = driver.find_elements(id: "email_login").any? ||
                 driver.find_elements(css: "input[name='email']").any?
      is_login_title = t.include?("ログイン") || t.downcase.include?("login")
      is_login_url   = u.end_with?("/") || u.include?("/login")
      has_form || is_login_title || is_login_url
    end

    def confirm_login_or_raise!(driver)
      safe_selenium_goto(driver, "#{base_url}/admin/home", tries: 2)
      wait_for_ready_state(driver)
      url = driver.current_url.to_s
      ok  = (url.include?("/admin") || url.include?("/basic/"))
      raise "Login not confirmed (still at #{url})" unless ok
    end

    def ensure_basic_session(driver)
      safe_selenium_goto(driver, "#{base_url}#{BASIC_FALLBACK}", tries: 2)
      wait_for_ready_state(driver)
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
        begin
          return true if driver.execute_script("return document.readyState") == "complete"
        rescue Selenium::WebDriver::Error::JavascriptError
        end
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
      driver.save_screenshot(png)
      File.write(html, driver.page_source.to_s)
      log :warn, "Dumped screenshot=#{png} html=#{html} url=#{driver.current_url} title=#{safe(driver.title)}"
    rescue => e
      log :warn, "dump failed: #{e.class} #{e.message}"
    end

    def valid_basic_session?(cookie_header, xsrf)
      return false if cookie_header.to_s.empty? || xsrf.to_s.empty?
      cookie_header.include?("laravel_session=") && cookie_header.include?("XSRF-TOKEN=")
    end

    def now_ms
      (Time.now.to_f * 1000).to_i
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

    def heroku?
      ENV["DYNO"].to_s != "" || ENV["HEROKU"].to_s == "1"
    end
  end
end
