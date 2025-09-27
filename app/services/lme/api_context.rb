# frozen_string_literal: true

module Lme
  # Cookie / XSRF / CSRF メタの統合コンテキスト + Playwright フォールバック
  class ApiContext
    attr_reader :http, :cookie_service, :origin, :bot_id
    attr_accessor :cookie_header, :xsrf_header, :csrf_meta, :login_cookies

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
    end

    # --- ログイン & 基本クッキー/XSRF確立 --------------------------------------
    def login_with_google!(email:, password:, api_key:)
      login = Lme::LoginUserService.new(email: email, password: password, api_key: api_key)
      result = login.fetch_friend_history
      @login_cookies = result[:cookies]
      @cookie_header = result[:basic_cookie_header].presence || result[:cookie_str].to_s

      xsrf_cookie = result[:basic_xsrf].presence ||
                    cookie_service.extract_cookie_from_pairs(@login_cookies, 'XSRF-TOKEN') ||
                    cookie_service.extract_cookie(@cookie_header, 'XSRF-TOKEN')
      unless @cookie_header.present? && xsrf_cookie.present?
        Rails.logger.info('[ApiContext] basic cookie/xsrf not found → ensure_basic_context!')
        @cookie_header, xsrf_cookie = cookie_service.ensure_basic_context!(@login_cookies)
      end
      raise 'cookie_header missing' if @cookie_header.blank?
      raise 'xsrf_cookie missing'   if xsrf_cookie.blank?
      @xsrf_header = cookie_service.decode_xsrf(xsrf_cookie)
      self
    end

    # --- CSRFメタ確立（HTTP→失敗時 Playwright） --------------------------------
    def ensure_csrf_meta!(paths: ['/basic/friendlist', '/basic/overview', '/basic', '/admin/home', '/'])
      meta, meta_src = cookie_service.fetch_csrf_meta_with_cookies(cookie_header, paths)
      unless meta.present?
        Rails.logger.warn('[ApiContext] csrf-meta HTTP失敗 → Playwright fallback')
        meta, cook2, xsrf2, src2 = cookie_service.playwright_fetch_meta_csrf!(login_cookies, paths)
        @cookie_header = cook2.presence || cookie_header
        @xsrf_header   = xsrf2.presence || xsrf_header
        meta_src = src2 if src2
      end
      @csrf_meta = meta if meta.present?
      Rails.logger.debug("[ApiContext] csrf-meta #{csrf_meta.present? ? 'ok' : 'miss'} at #{meta_src}")
      self
    end

    # --- chat-v3 経由で安定化（任意） ------------------------------------------
    def bake_chat_context_for!(friend_id)
      chat_cookie_header, chat_xsrf = cookie_service.playwright_bake_chat_cookies!(login_cookies, friend_id, bot_id)
      @cookie_header = chat_cookie_header.presence || cookie_header
      @xsrf_header   = chat_xsrf.presence          || xsrf_header
      meta2, cook2, xsrf2, _ = cookie_service.playwright_fetch_meta_csrf!(login_cookies, "/basic/chat-v3?friend_id=#{friend_id}")
      @csrf_meta     = meta2.presence  || csrf_meta
      @cookie_header = cook2.presence  || cookie_header
      @xsrf_header   = xsrf2.presence  || xsrf_header
      self
    end
  end
end
