# frozen_string_literal: true

# Cookie/Playwright/CSRFメタ取得などセッション周りを担当
# 依存：Playwright, Faraday, CGI, Rails.logger

module Lme
  class CookieContext
    def initialize(origin:, ua:, accept_lang:, ch_ua:, logger: Rails.logger)
      @origin = origin
      @ua = ua
      @accept_lang = accept_lang
      @ch_ua = ch_ua
      @logger = logger
    end

    # 旧 ensure_basic_context!
    def ensure_basic_context!(raw_cookies, fallback_cookie: nil)
      cookie_header = nil
      xsrf = nil

      Playwright.create(playwright_cli_executable_path: 'npx playwright') do |pw|
        browser = pw.chromium.launch(headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage'])
        context = browser.new_context
        begin
          if raw_cookies.present?
            add_cookies_to_context!(context, raw_cookies)
          elsif fallback_cookie.present?
            pairs = fallback_cookie.split(';').map { |p| k, v = p.strip.split('=', 2); { name: k, value: v } }
            add_cookies_to_context!(context, pairs)
          end
          page = context.new_page
          page.goto("#{@origin}/admin/home")
          pw_wait_networkidle(page)
          page.goto("#{@origin}/basic/overview")
          pw_wait_networkidle(page)

          pl = ctx_cookies(context, domain_base)
          cookie_header = pl.map { |c| "#{(c['name']||c[:name])}=#{(c['value']||c[:value])}" }.join('; ')
          xsrf_row = pl.find { |c| (c['name'] || c[:name]) == 'XSRF-TOKEN' }
          xsrf_raw = xsrf_row && (xsrf_row['value'] || xsrf_row[:value])
          xsrf = xsrf_raw && CGI.unescape(xsrf_raw.to_s)
        ensure
          context&.close rescue nil
          browser&.close rescue nil
        end
      end
      [cookie_header, xsrf]
    rescue => e
      @logger.warn("[ensure_basic_context!] #{e.class}: #{e.message}")
      [fallback_cookie, extract_cookie(fallback_cookie, 'XSRF-TOKEN')]
    end

    # 旧 fetch_csrf_meta_with_cookies
    def fetch_csrf_meta_with_cookies(cookie_header, paths = '/')
      Array(paths).compact_blank.each do |p|
        html, final_url = get_with_cookies(cookie_header, p)
        token = extract_meta_csrf(html)
        return [token, final_url] if token.present?
      end
      [nil, nil]
    end

    # 旧 get_with_cookies（HTML取得）
    def get_with_cookies(cookie_header, path)
      url = URI.join(@origin, path).to_s
      res = Faraday.new(url: @origin) { |f| f.adapter Faraday.default_adapter }.get(path) do |req|
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

    # 旧 extract_meta_csrf
    def extract_meta_csrf(html)
      return nil if html.blank?
      html[/<meta[^>]+name=["']csrf-token["'][^>]*content=["']([^"']+)["']/i, 1] ||
        html[/csrfToken["']?\s*[:=]\s*["']([^"']+)["']/i, 1]
    end

    # 旧 playwright_fetch_meta_csrf!
    def playwright_fetch_meta_csrf!(raw_cookies, paths)
      csrf_meta    = nil
      cookie_header = nil
      xsrf_cookie  = nil
      src          = nil

      Playwright.create(playwright_cli_executable_path: 'npx playwright') do |pw|
        browser = pw.chromium.launch(headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage'])
        context = browser.new_context
        begin
          add_cookies_to_context!(context, raw_cookies)
          page = context.new_page

          Array(paths).compact_blank.each do |p|
            page.goto("#{@origin}#{p}")
            pw_wait_for_url(page, %r{/basic/}, 15_000)
            pw_wait_networkidle(page)

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

          pl_cookies = ctx_cookies(context, domain_base)
          cookie_header = pl_cookies.map { |c| "#{(c['name']||c[:name])}=#{(c['value']||c[:value])}" }.join('; ')
          xsrf_row = pl_cookies.find { |c| (c['name'] || c[:name]) == 'XSRF-TOKEN' }
          if xsrf_row
            raw_val = (xsrf_row['value'] || xsrf_row[:value]).to_s
            xsrf_cookie = CGI.unescape(raw_val)
          end
        ensure
          context&.close rescue nil
          browser&.close rescue nil
        end
      end

      [csrf_meta, cookie_header, xsrf_cookie, src]
    rescue => e
      @logger.debug("[playwright_fetch_meta_csrf] #{e.class}: #{e.message}")
      [nil, nil, nil, nil]
    end

    # 旧 playwright_bake_chat_cookies!
    def playwright_bake_chat_cookies!(raw_cookies, sample_uid, _bot_id)
      cookie_header = nil
      xsrf = nil

      Playwright.create(playwright_cli_executable_path: 'npx playwright') do |pw|
        browser = pw.chromium.launch(headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage'])
        context = browser.new_context
        begin
          add_cookies_to_context!(context, raw_cookies)
          page = context.new_page
          pw_add_init_script(page, 'window.open = (url, target) => { location.href = url; }')
          page.goto("#{@origin}/basic/chat-v3?friend_id=#{sample_uid}")
          pw_wait_for_url(page, %r{/basic/chat-v3}, 15_000)
          pw_wait_networkidle(page)

          pl_cookies = ctx_cookies(context, domain_base)
          cookie_header = pl_cookies.map { |c| "#{(c['name']||c[:name])}=#{(c['value']||c[:value])}" }.join('; ')
          xsrf_cookie = pl_cookies.find { |c| (c['name'] || c[:name]) == 'XSRF-TOKEN' }
          xsrf_raw = xsrf_cookie && (xsrf_cookie['value'] || xsrf_cookie[:value])
          xsrf = xsrf_raw && CGI.unescape(xsrf_raw.to_s)
        ensure
          context&.close rescue nil
          browser&.close rescue nil
        end
      end
      [cookie_header, xsrf]
    end

    # ===== Cookie helpers / Playwright utils =====

    def add_cookies_to_context!(context, raw_cookies, default_domain: domain_base)
      normalized = Array(raw_cookies).map do |c|
        h = c.respond_to?(:to_h) ? c.to_h : c
        http_only_flag = h[:http_only] || h['http_only'] || h[:httponly] || h['httponly'] || false
        cookie = {
          name: (h[:name] || h['name']).to_s,
          value: (h[:value] || h['value']).to_s,
          domain: (h[:domain] || h['domain'] || default_domain).to_s,
          path: (h[:path] || h['path'] || '/').to_s,
          httpOnly: !!http_only_flag,
          secure: true
        }
        exp = (h[:expires] || h['expires'] || h[:expiry] || h['expiry'])
        cookie[:expires] =
          case exp
          when Time    then exp.to_i
          when Integer then exp
          when Float   then exp.to_i
          when String  then (Time.parse(exp).to_i rescue nil)
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

    def ctx_cookies(context, domain = nil)
      cookies = Array(context.cookies || [])
      return cookies unless domain
      cookies.select do |c|
        d = (c['domain'] || c[:domain] || (c.respond_to?(:domain) ? c.domain : '') || '').to_s
        d.include?(domain)
      end
    end

    def pw_wait_networkidle(page)
      page.wait_for_load_state(state: 'networkidle')
    rescue Playwright::TimeoutError, ArgumentError, NoMethodError
      sleep 1
    end

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

    def pw_add_init_script(page, code)
      page.add_init_script(script: code)
    rescue ArgumentError, NoMethodError
      page.add_init_script(code)
    end

    # ===== 単純Cookieユーティリティ =====
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

    private

    def domain_base
      # ORIGIN が https://step.lme.jp を想定
      URI(@origin).host || 'step.lme.jp'
    end
  end
end
