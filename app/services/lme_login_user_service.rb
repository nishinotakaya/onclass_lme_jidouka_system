# frozen_string_literal: true

require "selenium-webdriver"
require "net/http"
require "json"
require "playwright"
require "time"
require "cgi"
require "tmpdir"

class LmeLoginUserService
  LOG_PREFIX    = "[LmeLoginUserService]".freeze
  DUMP_ON_ERROR = ENV["LME_LOGIN_DUMP"] == "1"

  BASIC_TARGET_PATH = "/basic/overview".freeze
  BASIC_FALLBACK    = "/basic/friendlist".freeze
  RECAPTCHA_MAX_SOLVES = (ENV["RECAPTCHA_MAX_SOLVES"] || "1").to_i

  NAV_CLICK_JS = <<~JS
    (target) => {
      try {
        const a = document.createElement('a');
        a.href = target;
        a.rel  = 'noopener';
        a.style.display = 'none';
        document.body.appendChild(a);
        const ev = new MouseEvent('click', { bubbles: true, cancelable: true, view: window });
        a.dispatchEvent(ev);
        setTimeout(() => { try { a.remove(); } catch(_){} }, 1000);
      } catch(e) {
        try { window.location.assign(target); } catch(_){}
      }
    }
    JS


  def initialize(email:, password:, api_key:)
    @email    = email
    @password = password
    @api_key  = api_key
  end

  # =========================
  # 1) ログイン (Selenium)
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
      btn = wait.until { driver.find_element(css: "button[type=submit]") }
      wait.until { btn.enabled? }
      log :info, "ログインボタンをクリック"
      btn.click

      # セッション成立待ち
      ok_once = wait_until(35) { !looks_like_login_page?(driver) && has_session_cookie?(driver) }
      log :debug, "一次判定=#{ok_once} title=#{safe(driver.title)} url=#{driver.current_url}"
      confirm_login_or_raise!(driver)

      ensure_basic_session(driver) # /basic/friendlist まで一旦寄せる
      cookies = driver.manage.all_cookies
      log_cookies_brief!(cookies)
      { cookies: cookies, driver: driver }
    rescue => e
      log :error, "error: #{e.class} #{e.message}"
      e.backtrace&.first(8)&.each { |l| log :error, "  at #{l}" }
      dump_page(driver) if DUMP_ON_ERROR rescue nil
      driver.quit rescue nil
      raise
    end
  end

  # =========================
  # 2) /basic へ“必ず”入って Cookie/XSRF を確定 (Playwright)
  # =========================
  def fetch_friend_history(loa_label: "プロアカ")
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
        # Selenium cookie を PW に移植
        normalized = normalize_cookies_for_pw(login_result[:cookies])
        context.add_cookies(normalized) rescue context.add_cookies(cookies: normalized)

        page = context.new_page

        # admin → LOA 確定
        admin_url = "https://step.lme.jp/admin/home"
        safe_goto(page, admin_url, desc: "admin/home", tries: 2)
        page.wait_for_load_state(state: "domcontentloaded") rescue nil
        log :debug, "PW: admin url=#{page.url}"
        choose_loa_if_needed(page, loa_label)

        # /basic へ “クリック遷移”
        bot_id = ENV["LME_BOT_ID"].to_s.strip
        href_friend = build_basic_url(BASIC_FALLBACK, bot_id: bot_id, ts: now_ms)
        page.evaluate(NAV_CLICK_JS, arg: href_friend)
        page.wait_for_load_state(state: "networkidle") rescue nil
        sleep 0.5
        unless pw_reached_basic?(page, timeout_ms: 8_000)
          href_over = build_basic_url(BASIC_TARGET_PATH, bot_id: bot_id, ts: now_ms)
          page.evaluate(NAV_CLICK_JS, arg: href_over)
          page.wait_for_load_state(state: "networkidle") rescue nil
          sleep 0.5
        end

        # ダメ押し（新規タブ）
        unless pw_reached_basic?(page, timeout_ms: 8_000)
          href_over = build_basic_url(BASIC_TARGET_PATH, bot_id: bot_id, ts: now_ms)
          newp = open_in_new_tab(context, href_over + "&_tab=1&_r=#{now_ms}", desc: "overview(new tab)")
          page = newp if newp && pw_reached_basic?(newp, timeout_ms: 8_000)
        end

        # ここで /basic にいなければ中止（admin の Cookie を返さない）
        raise "basicエリアに入れませんでした (url=#{page.url})" unless pw_reached_basic?(page, timeout_ms: 2_000)

        # 見栄えとして overview を1回だけ（任意）
        begin
          over = build_basic_url(BASIC_TARGET_PATH, bot_id: bot_id, ts: now_ms)
          safe_goto(page, over, desc: "overview(final)", tries: 1)
        rescue; end
        basic_url = page.url.to_s

        # Cookie/XSRF 確定
        pl_cookies = context.cookies || []
        final_cookies     = pl_cookies
        raw_cookie_header = pl_cookies.map { |c| "#{(c["name"] || c[:name])}=#{(c["value"] || c[:value])}" }.join("; ")
        basic_cookie_header = sanitize_cookie_header(raw_cookie_header)

        xsrf_cookie = pl_cookies.find { |c| (c["name"] || c[:name]) == "XSRF-TOKEN" }
        xsrf_raw    = xsrf_cookie && (xsrf_cookie["value"] || xsrf_cookie[:value])
        basic_xsrf  = xsrf_raw && CGI.unescape(xsrf_raw.to_s)
        basic_xsrf  = dom_csrf_token(page) if basic_xsrf.to_s.empty?
        log :debug, "[cookies sanitized] #{basic_cookie_header || '(none)'}"
        log :debug, "PW XSRF head=#{basic_xsrf.to_s[0,10]}"
      ensure
        context&.close rescue nil
        browser&.close rescue nil
      end
    end

    result = {
      basic_cookie_header: basic_cookie_header,
      basic_xsrf: basic_xsrf,
      cookies: final_cookies,
      basic_url: basic_url,               # 必ず /basic/* で返す
      raw_cookie_header: raw_cookie_header,
    }

    unless valid_basic_session?(result[:basic_cookie_header], result[:basic_xsrf])
      log :error, "basic cookie/xsrf 判定NG（#{result[:basic_url]}）"
      raise "basicへの到達またはCookie/XSRFの確定に失敗しました"
    end
    result
  end

  # =========================
  # ヘルパ（最小セット）
  # =========================

  def choose_loa_if_needed(page, loa_label)
    # ラベルが出なければ確定済み
    begin
      page.wait_for_selector("text=#{loa_label}", timeout: 8_000)
    rescue Playwright::TimeoutError
      return
    end

    # まずは素直に text= クリック
    begin
      el = page.locator("text=#{loa_label}")
      if el.count > 0
        el.first.click
        page.wait_for_load_state(state: "networkidle") rescue nil
        sleep 0.5
        return
      end
    rescue; end

    # JS総当り：クリック可能な祖先を最大4段たどって click
    begin
      page.evaluate(<<~JS, arg: loa_label)
        (label) => {
          function clickable(e){
            if(!e) return false;
            const s = window.getComputedStyle(e);
            if(s.display==='none' || s.visibility==='hidden') return false;
            return typeof e.click === 'function';
          }
          const els = Array.from(document.querySelectorAll('span,a,button,td,div'));
          for (let i=0; i<els.length; i++){
            const e = els[i];
            const t = (e.innerText || '').trim();
            if(!t) continue;
            if(t.indexOf(label) !== -1){
              let cur = e;
              for (let j=0; j<4 && cur; j++){
                if (clickable(cur)) { cur.click(); return true; }
                cur = cur.parentElement;
              }
              e.dispatchEvent(new MouseEvent('click', {bubbles:true, cancelable:true, view:window}));
              return true;
            }
          }
          return false;
        }
      JS
      page.wait_for_load_state(state: "networkidle") rescue nil
      sleep 0.5
    rescue => e
      log :debug, "LOAクリック(JS)失敗: #{e.class} #{e.message}"
    end
  end

  def pw_reached_basic?(page, timeout_ms: 8_000)
    begin
      page.wait_for_url(%r{/basic/(overview|friendlist|chat-v3)}, timeout: timeout_ms)
    rescue Playwright::TimeoutError
      # URLが変わらなくても本文で判定
    end
    url_ok  = page.url.to_s.include?("/basic/")
    text_ok = begin
      txt = page.evaluate("document.body && document.body.innerText || ''").to_s
      txt.include?("友だちリスト") || txt.include?("チャット管理")
    rescue
      false
    end
    url_ok || text_ok
  end

  def safe_goto(page, url, desc:, tries: 2)
    tries.times do |i|
      begin
        page.goto(url, timeout: 30_000, referer: "https://step.lme.jp/admin/home")
        page.wait_for_load_state(state: "domcontentloaded") rescue nil
        return true if page.url.to_s.start_with?("https://")
      rescue => e
        log :debug, "goto失敗(#{desc} try=#{i + 1}/#{tries}): #{e.class} #{e.message}"
      end
      # assign フォールバック
      begin
        page.evaluate("u => { try { window.location.assign(u) } catch(e) {} }", arg: url)
        page.wait_for_url(%r{^https://}, timeout: 10_000) rescue nil
        return true if page.url.to_s.start_with?("https://")
      rescue => e
        log :debug, "assign失敗(#{desc} try=#{i + 1}/#{tries}): #{e.class} #{e.message}"
      end
      sleep 0.3
    end
    false
  end

  def open_in_new_tab(context, url, desc:)
    newp = context.new_page
    begin
      newp.goto(url, timeout: 30_000, referer: "https://step.lme.jp/admin/home")
      newp.wait_for_load_state(state: "domcontentloaded") rescue nil
      return newp if newp.url.to_s.start_with?("https://")
    rescue => e
      log :debug, "new tab 失敗(#{desc}): #{e.class} #{e.message}"
    end
    newp.close rescue nil
    nil
  end

  def build_basic_url(path, bot_id:, ts:)
    base = "https://step.lme.jp#{path}"
    q = []
    q << "botIdCurrent=#{CGI.escape(bot_id)}" unless bot_id.empty?
    q << "isOtherBot=1" unless bot_id.empty?
    q << "_ts=#{ts}"
    q.empty? ? base : "#{base}?#{q.join('&')}"
  end

  def dom_csrf_token(page)
    page.evaluate(<<~JS).to_s
      (function(){
        var m = document.querySelector('meta[name="csrf-token"]');
        return m ? m.getAttribute('content') : '';
      })();
    JS
  rescue
    ""
  end

  def sanitize_cookie_header(cookie_header)
    keep = %w[laravel_session XSRF-TOKEN ROUTEID]
    return "" if cookie_header.nil? || cookie_header.strip.empty?
    pairs = cookie_header.split(/;\s*/).map { |kv| kv.split("=", 2) }.select { |k,_| k && _ }
    uniq  = {}; pairs.each { |k, v| uniq[k] = v } # 後勝ち
    keep.map { |k| "#{k}=#{uniq[k]}" if uniq[k] }.compact.join("; ")
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
      }
    end
  end

  # 内部ヘルパ
  private

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

  # ---- ここから下は Selenium 用の小ヘルパ ----

  def already_logged_in?(driver)
    return false if looks_like_login_page?(driver)
    driver.navigate.to "https://step.lme.jp/admin/home"
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
    driver.navigate.to "https://step.lme.jp/admin/home"
    wait_for_ready_state(driver)
    url = driver.current_url.to_s
    ok  = (url.include?("/admin") || url.include?("/basic/"))
    raise "Login not confirmed (still at #{url})" unless ok
  end

  def ensure_basic_session(driver)
    driver.navigate.to "https://step.lme.jp#{BASIC_FALLBACK}"
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
      return true if driver.execute_script("return document.readyState") == "complete"
      sleep 0.2
    end
    false
  end

  def wait_until(sec = 15)
    deadline = Time.now + sec
    loop do
      return true if yield
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

  # URLに依存せず Cookie+XSRF があればOK（adminのまま返さないよう /basic 到達を別で厳守）
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
end
