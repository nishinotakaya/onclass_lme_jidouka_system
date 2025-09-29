require 'cgi'
require 'uri'
require 'faraday'

module Lme
  class ApiContext
    attr_reader :http, :cookie_service, :origin, :bot_id
    attr_accessor :cookie_header, :xsrf_header, :csrf_meta, :login_cookies, :driver
    attr_reader :basic_url

    def initialize(origin:, ua:, accept_lang:, ch_ua:, logger:, bot_id:)
      @origin = origin
      @bot_id = bot_id.to_s
      @cookie_service = Lme::CookieContext.new(
        origin: origin, ua: ua, accept_lang: accept_lang, ch_ua: ch_ua, logger: logger
      )
      @http = Lme::HttpClient.new(
        origin: origin, ua: ua, accept_lang: accept_lang, ch_ua: ch_ua,
        cookie_service: @cookie_service, logger: logger
      )
      @http.instance_variable_set(:@ctx, self)
      @http.ctx = self
      @driver = nil
      @basic_url = nil
    end

    # --- ログイン & /basic の Cookie/XSRF 確立（LoginUserService = Selenium） ------
    # LoginUserService#fetch_friend_history が Selenium でログイン完了し、
    # cookies / cookie_header / basic_xsrf / driver / basic_url を返す前提。
    def login_with_google!(email:, password:, api_key:)
      login  = Lme::LoginUserService.new(email: email, password: password, api_key: api_key)
      result = login.fetch_friend_history

      @login_cookies = result[:cookies]
      @cookie_header = result[:basic_cookie_header].presence || result[:cookie_str].to_s
      @driver        = result[:driver] # ⬅ フォールバックで使う
      @basic_url     = result[:basic_url]

      xsrf_cookie = result[:basic_xsrf].presence ||
                    cookie_service.extract_cookie_from_pairs(@login_cookies, 'XSRF-TOKEN') ||
                    cookie_service.extract_cookie(@cookie_header, 'XSRF-TOKEN')

      unless @cookie_header.present? && xsrf_cookie.present?
        Rails.logger.info('[ApiContext] basic cookie/xsrf not found → ensure_basic_context!(Selenium)')
        @cookie_header, xsrf_cookie = cookie_service.ensure_basic_context!(@login_cookies, driver: @driver)
      end

      raise 'cookie_header missing' if @cookie_header.blank?
      raise 'xsrf_cookie missing'   if xsrf_cookie.blank?

      @xsrf_header = cookie_service.decode_xsrf(xsrf_cookie)
      self
    end

    # --- CSRFメタ確立：HTTP→失敗時 Selenium（Playwrightは使わない） -------------
    def ensure_csrf_meta!(paths: ['/admin/home', '/basic/overview', '/basic/friendlist', '/basic', '/'])
      # 1) HTTP でメタ読めるかを試す
      meta, meta_src = cookie_service.fetch_csrf_meta_with_cookies(cookie_header, paths)
      if meta.present?
        @csrf_meta = meta
        Rails.logger.debug("[ApiContext] csrf-meta ok via HTTP at #{meta_src}")
        close_driver_if_needed!
        return self
      end

      Rails.logger.warn('[ApiContext] csrf-meta HTTP失敗（Seleniumフォールバック）')

      # 2) Selenium フォールバック（driver 必須）
      if driver
        # fetch_friend_history 済みの “確実に basic 内” のURLを最優先で踏む
        sel_paths = []
        sel_paths << @basic_url if @basic_url.present?
        sel_paths.concat(paths)

        meta2, cook2, xsrf2, src2 = cookie_service.selenium_fetch_meta_csrf!(driver, login_cookies, sel_paths, origin: origin)
        if meta2.present?
          @csrf_meta     = meta2
          @cookie_header = cook2.presence || cookie_header
          @xsrf_header   = xsrf2.presence || xsrf_header
          Rails.logger.debug("[ApiContext] csrf-meta ok via Selenium at #{src2}")
        else
          Rails.logger.warn('[ApiContext] csrf-meta Selenium でも取得失敗')
        end
      else
        Rails.logger.warn('[ApiContext] driver が無いため Selenium フォールバック不可')
      end

      # 3) 最終フォールバック：csrf-meta が無くても xsrf があればそれを使う
      if @csrf_meta.to_s.strip.empty? && @xsrf_header.to_s.strip.present?
        @csrf_meta = @xsrf_header
        Rails.logger.info('[ApiContext] csrf-meta fallback: using XSRF header as CSRF token')
      end

      close_driver_if_needed!
      self
    end

    # --- chat-v3 経由の“焼き込み”：Selenium運用では通常スキップ ------------------
    def bake_chat_context_for!(_friend_id)
      Rails.logger.info('[ApiContext] bake_chat_context_for!: Selenium専用モードのためスキップ')
      self
    end

    def basic_referer_for(path = nil)
      return @basic_url if path.blank?
      # @basic_url のクエリ（botIdCurrent, isOtherBot）を残したまま、パスだけ差し替える
      begin
        bu = URI(@basic_url.presence || "#{origin}/basic/overview")
        u  = URI.join(origin, path.to_s)
        bu.path = u.path
        bu.to_s
      rescue
        "#{origin}#{path}"
      end
    end

    private

    def close_driver_if_needed!
      return unless ENV['LME_CLOSE_DRIVER_AFTER_META'].to_s == '1'
      begin
        @driver&.quit
      rescue => e
        Rails.logger.debug("[ApiContext] driver quit error: #{e.class}: #{e.message}")
      ensure
        @driver = nil
      end
    end
  end
end
