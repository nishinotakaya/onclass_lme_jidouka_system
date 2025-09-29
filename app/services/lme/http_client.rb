# frozen_string_literal: true

# HTTP(JSON/FORM) と LOA リトライを担当（Faraday / Rails.logger 依存）
# Cookie/XSRF/CSRF の実体は ApiContext から参照する（@ctx を注入）
module Lme
  class HttpClient
    attr_accessor :ctx

    def initialize(origin:, ua:, accept_lang:, ch_ua:, cookie_service:, logger: Rails.logger)
      @origin = origin
      @ua = ua
      @accept_lang = accept_lang
      @ch_ua = ch_ua
      @cookie_service = cookie_service
      @logger = logger
    end

    # ===================== 共通ヘッダ =====================
    def json_headers(referer:)
      cookie = ctx&.cookie_header.to_s
      xsrf   = ctx&.xsrf_header.to_s
      csrf   = ctx&.csrf_meta.to_s
      ref    = referer.presence || ctx_basic_url || @origin
      {
        'accept'                 => 'application/json, text/plain, */*',
        'accept-language'        => @accept_lang,
        'user-agent'             => @ua,
        'sec-ch-ua'              => @ch_ua,
        'sec-ch-ua-mobile'       => '?0',
        'sec-ch-ua-platform'     => %Q("macOS"),
        'x-requested-with'       => 'XMLHttpRequest',
        'cookie'                 => cookie,
        # Laravel 対策：両方付与（GET でも）
        'x-csrf-token'           => csrf,
        'x-xsrf-token'           => xsrf,
        'referer'                => ref,
        'origin'                 => @origin,
        'cache-control'          => 'no-cache',
        'pragma'                 => 'no-cache'
      }
    end

    def form_headers(referer:, csrf_meta:, xsrf_cookie:, cookie:)
      json_headers(referer: referer).merge(
        'content-type' => 'application/x-www-form-urlencoded; charset=UTF-8',
        'cookie'       => cookie.to_s,
        'x-csrf-token' => csrf_meta.to_s,
        'x-xsrf-token' => xsrf_cookie.to_s
      )
    end

    # ===================== JSON =====================
    def get_json(path:, referer: nil, params: {}, **_)
      url = build_url(path, params)
      body, _ = do_request(:get, url, headers: json_headers(referer: referer)) { |_req| }
      body
    end

    # app/services/lme/http_client.rb
    def post_json(path:, json:, referer:, cookie: nil, xsrf_cookie: nil, extra_headers: {}, **_)
      cookie       ||= ctx&.cookie_header
      xsrf_cookie  ||= ctx&.xsrf_header
      extra_headers = extra_headers.to_h

      conn = Faraday.new(url: @origin) { |f| f.adapter Faraday.default_adapter }
      res = conn.post(path) do |req|
        req.headers['accept']              = 'application/json, text/plain, */*'
        req.headers['accept-language']     = @accept_lang
        req.headers['content-type']        = 'application/json;charset=UTF-8'
        req.headers['cookie']              = cookie.to_s if cookie
        req.headers['origin']              = @origin
        req.headers['referer']             = referer
        req.headers['sec-ch-ua']           = @ch_ua
        req.headers['sec-ch-ua-mobile']    = '?0'
        req.headers['sec-ch-ua-platform']  = %Q("macOS")
        req.headers['sec-fetch-dest']      = 'empty'
        req.headers['sec-fetch-mode']      = 'cors'
        req.headers['sec-fetch-site']      = 'same-origin'
        req.headers['user-agent']          = @ua
        req.headers['x-requested-with']    = 'XMLHttpRequest'
        req.headers['x-xsrf-token']        = xsrf_cookie.to_s if xsrf_cookie.present?

        extra_headers.each { |k,v| req.headers[k] = v }

        req.body = json.is_a?(String) ? json : JSON.dump(json)
      end
      raise Faraday::Error, "HTTP #{res.status}" if res.status >= 500
      res.body.to_s
    end


    def post_form(path:, form:, cookie: nil, csrf_meta: nil, xsrf_cookie: nil, referer:, **_)
      uri = URI.join(@origin, path)

      headers = {
        'accept'                => '*/*',
        'accept-language'       => @accept_lang,
        'content-type'          => 'application/x-www-form-urlencoded; charset=UTF-8',
        'user-agent'            => @ua,
        'sec-ch-ua'             => @ch_ua,
        'sec-ch-ua-mobile'      => '?0',
        'sec-ch-ua-platform'    => %Q("macOS"),
        'sec-fetch-dest'        => 'empty',
        'sec-fetch-mode'        => 'cors',
        'sec-fetch-site'        => 'same-origin',
        'x-requested-with'      => 'XMLHttpRequest',
        'origin'                => @origin,
        'referer'               => referer
      }

      headers['x-csrf-token']  = csrf_meta.to_s if csrf_meta.present?
      headers['x-xsrf-token']  = CGI.unescape(xsrf_cookie) if xsrf_cookie.present?
      headers['cookie']        = cookie.to_s if cookie.present?

      body = URI.encode_www_form(form)
      do_post(uri, headers, body)
    end


    # ================= Cookie付HTML（meta抽出用） =================
    def get_with_cookies(cookie_header, path)
      url = build_url(path)
      res = Faraday.new(url: @origin) { |f| f.adapter Faraday.default_adapter }.get(URI(url).request_uri) do |req|
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

    # ================= LOA/CSRF リトライ =================
    # 互換維持： with_loa_retry(@ctx.cookie_header, @ctx.xsrf_header) でも、
    #           with_loa_retry() でも使える
    def with_loa_retry(cookie_header_str = nil, xsrf_header_str = nil)
      tries = 0
      begin
        tries += 1
        body = yield
        # HTML が返る/短いエラー文字列は CSRF/未認可の疑い
        if looks_like_csrf_error?(body)
          raise Faraday::ClientError.new('HTTP 419 (csrf)')
        end
        body
      rescue Faraday::ClientError => e
        # 1回だけ復旧チャレンジ：/basic を Selenium で踏み直して CSRF/クッキー更新
        if tries == 1
          @logger.info("[LOA retry] #{e.message} → ensure_csrf_meta! → retry")
          ctx&.ensure_csrf_meta!
          # cookie/xsrf の参照文字列が渡されていれば置換で更新（互換目的）
          if cookie_header_str.is_a?(String) && ctx&.cookie_header.present?
            cookie_header_str.replace(ctx.cookie_header)
          end
          if xsrf_header_str.is_a?(String) && ctx&.xsrf_header.present?
            xsrf_header_str.replace(ctx.xsrf_header)
          end
          retry
        end
        raise
      end
    end

    private

    # app/services/lme/http_client.rb （クラス内・privateの所に追加）
    def do_post(uri, headers, body)
      conn = Faraday.new(url: @origin) { |f| f.adapter Faraday.default_adapter }
      res  = conn.post(uri.request_uri) do |req|
        headers.each { |k, v| req.headers[k] = v if v.present? }
        req.body = body.to_s
      end
      resp_body = res.body.to_s
      @logger.debug("[HTTP] POST #{uri} status=#{res.status} len=#{resp_body.bytesize}")
      if resp_body.lstrip.start_with?('<!DOCTYPE', '<html')
        @logger.warn("[HTTP] HTML returned (unexpected). status=#{res.status} head=#{resp_body[0,120].gsub(/\s+/, ' ')}")
      end
      resp_body
    end


    def build_url(path, params = {})
      return path.to_s if path.to_s.start_with?('http')
      uri = URI.join(@origin, path.to_s)
      unless params.empty?
        q = URI.decode_www_form(uri.query.to_s) + params.to_a
        uri.query = URI.encode_www_form(q)
      end
      uri.to_s
    end

    def do_request(verb, url, headers:)
      conn = Faraday.new(url: @origin) { |f| f.adapter Faraday.default_adapter }
      res = conn.public_send(verb, URI(url).request_uri) do |req|
        headers.each { |k, v| req.headers[k] = v if v.present? }
        yield(req) if block_given?
      end
      body = res.body.to_s
      @logger.debug("[HTTP] #{verb.to_s.upcase} #{url} status=#{res.status} len=#{body.bytesize}")
      if body.lstrip.start_with?('<!DOCTYPE', '<html')
        @logger.warn("[HTTP] HTML returned (unexpected). status=#{res.status} head=#{body[0,120].gsub(/\s+/, ' ')}")
      end
      [body, res.status]
    end

    def looks_like_csrf_error?(body)
      return false unless body.is_a?(String)
      return true  if body.size < 512 && body =~ /(csrf|419|expired|unauth|forbidden)/i
      false
    end

    def ctx_basic_url
      ctx&.instance_variable_get(:@basic_url)
    end
  end

  def with_loa_retry(cookie_header, xsrf_header)
    tries = 0
    begin
      tries += 1
      body = yield
      # Laravel系が 419/HTML でも JSON でも返すので、短文HTML内の csrf/419/expired 文字列も検出
      if body.is_a?(String) && body.size < 256 && body =~ /(csrf|419|expired)/i
        raise Faraday::ClientError.new('419 CSRF mismatch')
      end
      body
    rescue Faraday::ClientError => e
      code = e.message[/\b(\d{3})\b/, 1].to_i
      if tries == 1
        @logger.info("[LOA retry] #{code.zero? ? e.class : code} → ensure_basic_context!(Selenium) → retry")
        # ここが重要：Selenium で /admin/home → /basic/overview?botIdCurrent=... を踏んで
        # Cookie/XSRF を“実際のブラウザ状態”から取り直す
        new_cookie, new_xsrf = @cookie_service.ensure_basic_context!(nil, driver: @ctx.driver)
        cookie_header.replace(new_cookie) if new_cookie.present?
        xsrf_header.replace(new_xsrf)     if new_xsrf.present?
        retry
      end
      raise
    end
  end
end
