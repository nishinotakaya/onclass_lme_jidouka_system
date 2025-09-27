# frozen_string_literal: true

# HTTP(Post Form / Get JSON) と LOA リトライだけを担当
# 依存：Faraday / Rails.logger
# Cookie周りは CookieContext に委譲します

module Lme
  class HttpClient
    def initialize(origin:, ua:, accept_lang:, ch_ua:, cookie_service:, logger: Rails.logger)
      @origin = origin
      @ua = ua
      @accept_lang = accept_lang
      @ch_ua = ch_ua
      @cookie_service = cookie_service
      @logger = logger
    end

    # 旧 curl_post_form
    def post_form(path:, form:, cookie:, csrf_meta:, xsrf_cookie:, referer:)
      conn = Faraday.new(url: @origin) { |f| f.request :url_encoded; f.adapter Faraday.default_adapter }
      res = conn.post(path) do |req|
        req.headers['accept'] = '*/*'
        req.headers['accept-language'] = @accept_lang
        req.headers['content-type'] = 'application/x-www-form-urlencoded; charset=UTF-8'
        req.headers['cookie'] = cookie.to_s
        req.headers['origin'] = @origin
        req.headers['referer'] = referer
        req.headers['sec-ch-ua'] = @ch_ua
        req.headers['sec-ch-ua-mobile'] = '?0'
        req.headers['sec-ch-ua-platform'] = %Q("macOS")
        req.headers['sec-fetch-dest'] = 'empty'
        req.headers['sec-fetch-mode'] = 'cors'
        req.headers['sec-fetch-site'] = 'same-origin'
        req.headers['user-agent'] = @ua
        req.headers['x-requested-with'] = 'XMLHttpRequest'
        req.headers['x-csrf-token'] = csrf_meta.to_s if csrf_meta.present?
        req.headers['x-xsrf-token'] = xsrf_cookie.to_s if xsrf_cookie.present?
        req.body = URI.encode_www_form(form)
      end
      raise Faraday::Error, "HTTP #{res.status}" if res.status >= 500
      res.body.to_s
    end

    # 旧 curl_get_json
    def get_json(path:, cookie:, referer:)
      conn = Faraday.new(url: @origin) { |f| f.adapter Faraday.default_adapter }
      res = conn.get(path) do |req|
        req.headers['accept'] = 'application/json, text/javascript, */*; q=0.01'
        req.headers['accept-language'] = @accept_lang
        req.headers['cookie'] = cookie.to_s
        req.headers['referer'] = referer
        req.headers['sec-ch-ua'] = @ch_ua
        req.headers['sec-ch-ua-mobile'] = '?0'
        req.headers['sec-ch-ua-platform'] = %Q("macOS")
        req.headers['sec-fetch-dest'] = 'empty'
        req.headers['sec-fetch-mode'] = 'cors'
        req.headers['sec-fetch-site'] = 'same-origin'
        req.headers['user-agent'] = @ua
        req.headers['x-requested-with'] = 'XMLHttpRequest'
        req.headers['cache-control'] = 'no-cache'
        req.headers['pragma'] = 'no-cache'
      end
      JSON.parse(res.body.to_s) rescue {}
    end

    # 旧 get_with_cookies（Cookie付でHTMLを取る）
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

    # 旧 with_loa_retry
    # cookie_header/xsrf_header は String 想定（replaceで中身を差し替え）
    def with_loa_retry(cookie_header, xsrf_header)
      tries = 0
      begin
        tries += 1
        body = yield
        if body.is_a?(String) && body.size < 256 && body =~ /(csrf|419|expired)/i
          raise Faraday::ClientError.new('419 CSRF mismatch')
        end
        body
      rescue Faraday::ClientError => e
        code = e.message[/\b(\d{3})\b/, 1].to_i
        if tries == 1 && [404, 401, 419].include?(code) || tries == 1
          @logger.info("[LOA retry] #{code.zero? ? e.class : code} → ensure_basic_context! → retry")
          new_cookie, new_xsrf = @cookie_service.ensure_basic_context!(nil, fallback_cookie: cookie_header)
          cookie_header.replace(new_cookie) if new_cookie.present?
          xsrf_header.replace(new_xsrf)     if new_xsrf.present?
          retry
        end
        raise
      end
    end
  end
end
