require 'cgi'
require 'uri'
require 'time'
require 'faraday'
require 'selenium-webdriver'

module Lme
  class CookieContext
    def initialize(origin:, ua:, accept_lang:, ch_ua:, logger: Rails.logger)
      @origin = origin
      @ua = ua
      @accept_lang = accept_lang
      @ch_ua = ch_ua
      @logger = logger
    end

    # /admin/home → /basic/overview を踏んで Cookie を安定化（Selenium）
    def ensure_basic_context!(raw_cookies, driver:)
      cookie_header = nil
      xsrf = nil
      raise 'ensure_basic_context! requires Selenium driver' unless driver

      add_cookies_to_driver!(driver, raw_cookies)

      navigate_and_wait(driver, "#{@origin}/admin/home")
      navigate_and_wait(driver, "#{@origin}/basic/overview")

      all = normalize_driver_cookies(driver)
      cookie_header = all.map { |c| "#{c[:name]}=#{c[:value]}" }.join('; ')
      xsrf_row = all.find { |c| c[:name] == 'XSRF-TOKEN' }
      xsrf_raw = xsrf_row && xsrf_row[:value]
      xsrf = xsrf_raw && CGI.unescape(xsrf_raw.to_s)

      [cookie_header, xsrf]
    rescue => e
      @logger.warn("[ensure_basic_context!(selenium)] #{e.class}: #{e.message}")
      [header_from_pairs(raw_cookies), extract_cookie(header_from_pairs(raw_cookies), 'XSRF-TOKEN')]
    end

    # HTTP で <meta name="csrf-token" ...> を探す
    def fetch_csrf_meta_with_cookies(cookie_header, paths = '/')
      Array(paths).compact_blank.each do |p|
        html, final_url = get_with_cookies(cookie_header, p)
        token = extract_meta_csrf(html)
        return [token, final_url] if token.present?
      end
      [nil, nil]
    end

    # Selenium で DOM から csrf-token を読む（HTTP失敗時の最後の砦）
    # ついでに driver の Cookie を拾って Cookie ヘッダ/XSRF も更新する
    def selenium_fetch_meta_csrf!(driver, raw_cookies, paths, origin:)
      add_cookies_to_driver!(driver, raw_cookies) if raw_cookies.present?

      csrf_meta     = nil
      cookie_header = nil
      xsrf_cookie   = nil
      src           = nil

      Array(paths).compact_blank.each do |p|
        # p が絶対URLならそのまま、相対なら origin と結合
        dest = p.to_s.start_with?('http') ? p.to_s : join(origin, p)
        navigate_and_wait(driver, dest)

        csrf_meta = eval_csrf_via_js(driver)
        if csrf_meta.to_s.strip != ''
          src = p
          break
        end
      end

      all = normalize_driver_cookies(driver)
      cookie_header = all.map { |c| "#{c[:name]}=#{c[:value]}" }.join('; ')
      xsrf_row = all.find { |c| c[:name] == 'XSRF-TOKEN' }
      xsrf_cookie = xsrf_row && CGI.unescape(xsrf_row[:value].to_s)

      [csrf_meta, cookie_header, xsrf_cookie, src]
    rescue => e
      @logger.debug("[selenium_fetch_meta_csrf!] #{e.class}: #{e.message}")
      [nil, nil, nil, nil]
    end

    # ===== HTTPユーティリティ =====

    def get_with_cookies(cookie_header, path)
      url = join(@origin, path)
      conn = Faraday.new(url: @origin) { |f| f.adapter Faraday.default_adapter }
      res = conn.get(URI(url).request_uri) do |req|
        req.headers['accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        req.headers['accept-language'] = @accept_lang
        req.headers['cookie'] = cookie_header.to_s
        req.headers['user-agent'] = @ua
        req.headers['sec-ch-ua'] = @ch_ua
        req.headers['sec-ch-ua-mobile'] = '?0'
        req.headers['sec-ch-ua-platform'] = %Q("macOS")
        req.headers['upgrade-insecure-requests']= '1'
        req.headers['cache-control'] = 'no-cache'
        req.headers['pragma'] = 'no-cache'
        req.headers['referer'] = @origin
      end
      [res.body.to_s, url]
    rescue => e
      @logger.debug("[csrf-meta GET] #{e.class}: #{e.message}")
      ['', url]
    end

    def extract_meta_csrf(html)
      return nil if html.blank?
      html[/<meta[^>]+name=["']csrf-token["'][^>]*content=["']([^"']+)["']/i, 1] ||
        html[/csrfToken["']?\s*[:=]\s*["']([^"']+)["']/i, 1]
    end

    # ===== Seleniumユーティリティ =====

    def add_cookies_to_driver!(driver, raw_cookies)
      domain = URI(@origin).host
      Array(raw_cookies).each do |c|
        h = c.respond_to?(:to_h) ? c.to_h : c
        next if h[:name].to_s.empty?
        cookie = {
          name:  (h[:name]  || h['name']).to_s,
          value: (h[:value] || h['value']).to_s,
          path:  (h[:path]  || h['path']  || '/').to_s,
          domain:(h[:domain]|| h['domain']|| domain).to_s,
          secure: true
        }
        if (exp = (h[:expires] || h['expires'] || h[:expiry] || h['expiry']))
          cookie[:expires] =
            case exp
            when Time    then exp
            when Integer then Time.at(exp)
            when Float   then Time.at(exp.to_i)
            when String  then (Time.parse(exp) rescue nil)
            end
        end
        begin
          navigate_if_necessary_for_cookie!(driver)
          driver.manage.add_cookie(cookie.compact)
        rescue Selenium::WebDriver::Error::InvalidCookieDomainError
          # ドメイン不一致時は無視
        rescue => e
          @logger.debug("[add_cookie] #{cookie[:name]} #{e.class}: #{e.message}")
        end
      end
    end

    def navigate_if_necessary_for_cookie!(driver)
      cur = (driver.current_url rescue '')
      return if cur.start_with?(@origin)
      driver.navigate.to(@origin)
      short_wait
    rescue => e
      @logger.debug("[navigate_if_necessary] #{e.class}: #{e.message}")
    end

    def navigate_and_wait(driver, url)
      driver.navigate.to(url)
      Selenium::WebDriver::Wait.new(timeout: 10).until { driver.execute_script('return document.readyState') == 'complete' }
    rescue => e
      @logger.debug("[navigate_and_wait] #{url} #{e.class}: #{e.message}")
      short_wait
    end

    def eval_csrf_via_js(driver)
      driver.execute_script(<<~JS)
        (function(){
          var m = document.querySelector('meta[name="csrf-token"]');
          if (m && m.content) return m.content;
          if (window && window.Laravel && window.Laravel.csrfToken) return window.Laravel.csrfToken;
          if (window && window.csrfToken) return window.csrfToken;
          return null;
        })();
      JS
    rescue => e
      @logger.debug("[eval_csrf_via_js] #{e.class}: #{e.message}")
      nil
    end

    def normalize_driver_cookies(driver)
      Array(driver.manage.all_cookies).map do |c|
        {
          name: (c[:name] || c['name']).to_s,
          value: (c[:value] || c['value']).to_s,
          domain: (c[:domain] || c['domain']).to_s,
          path: (c[:path] || c['path'] || '/').to_s,
          expires: (c[:expires] || c['expires']),
          http_only: (c[:httpOnly] || c['httpOnly'] || c[:http_only] || c['http_only'] || false),
          secure: (c[:secure] || c['secure'] || true)
        }.compact
      end
    rescue => e
      @logger.debug("[normalize_driver_cookies] #{e.class}: #{e.message}")
      []
    end

    # ===== 単純Cookie/文字列ユーティリティ =====

    def extract_cookie(cookie_str, key)
      return nil if cookie_str.blank?
      cookie_str.split(';').map(&:strip).each do |pair|
        k, v = pair.split('=', 2)
        return v if k == key
      end
      nil
    end

    def decode_xsrf(v)
      s = v.to_s
      s.include?('%') ? CGI.unescape(s) : s
    end

    def extract_cookie_from_pairs(pairs, name)
      Array(pairs).each do |c|
        h = c.respond_to?(:to_h) ? c.to_h : c
        return h[:value] || h['value'] if (h[:name] || h['name']).to_s == name.to_s
      end
      nil
    end

    def header_from_pairs(pairs)
      Array(pairs).map { |c| h = (c.respond_to?(:to_h) ? c.to_h : c); "#{h[:name] || h['name']}=#{h[:value] || h['value']}" }.join('; ')
    end

    def join(base, path)
      URI.join(base, path.to_s).to_s
    end

    def short_wait
      sleep 0.3
    end
  end
end
