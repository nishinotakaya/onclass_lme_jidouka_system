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

    # 保存済みLMEセッションの有効期限。過ぎたらキャッシュ削除（使用前に必ずHTTPで有効性検証もする）。
    LME_SESSION_TTL = 12.hours

    # --- ログイン & /basic の Cookie/XSRF 確立（LoginUserService = Selenium） ------
    # ★ 毎回 Selenium+2Captcha でログインすると 2Captcha 残高を消費するため、
    #   有効なセッション(Cookie)がキャッシュにあれば再ログインせず再利用する。
    #   有効性チェックは HTTP のみ（2Captcha/Selenium を使わない）。
    def login_with_google!(email:, password:, api_key:)
      return self if restore_session_from_cache!

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
      save_session_to_cache!
      self
    end

    # 有効なセッションが失効/壊れた時に呼ぶ（キャッシュを消して次回フルログインさせる）
    def invalidate_session_cache!
      Rails.cache.delete(session_cache_key)
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

    # bot ごとにセッションを1本キャッシュ（同じbotを使う全ワーカーで共有）
    def session_cache_key
      "lme_session:#{@bot_id.presence || 'default'}"
    end

    # キャッシュのセッションを復元し、HTTPで有効性を確認できたら true（＝再ログイン不要）
    def restore_session_from_cache!
      data = Rails.cache.read(session_cache_key)
      return false if data.blank? || data[:cookie_header].blank?

      @login_cookies = data[:login_cookies]
      @cookie_header = data[:cookie_header]
      @xsrf_header   = data[:xsrf_header]
      @csrf_meta     = data[:csrf_meta]
      @basic_url     = data[:basic_url]
      @driver        = nil # 再利用時はSeleniumドライバを持たない

      # /basic 系ページから csrf-meta を引けるか＝セッション+LOAが生きているか（HTTPのみ）
      meta, src = cookie_service.fetch_csrf_meta_with_cookies(
        @cookie_header, ['/basic/overview', '/admin/home', '/basic/friendlist', '/basic']
      )
      if meta.present?
        @csrf_meta = meta
        Rails.logger.info("[ApiContext] 保存済みLMEセッションを再利用（再ログイン/2Captchaなし） src=#{src}")
        return true
      end

      Rails.logger.info('[ApiContext] 保存済みLMEセッションが無効 → 通常ログインへ')
      Rails.cache.delete(session_cache_key)
      false
    rescue => e
      Rails.logger.warn("[ApiContext] セッション復元に失敗（通常ログインへ）: #{e.class} #{e.message}")
      false
    end

    # ログイン成功時にセッションをキャッシュへ保存
    def save_session_to_cache!
      Rails.cache.write(
        session_cache_key,
        {
          login_cookies: @login_cookies,
          cookie_header: @cookie_header,
          xsrf_header:   @xsrf_header,
          csrf_meta:     @csrf_meta,
          basic_url:     @basic_url
        },
        expires_in: LME_SESSION_TTL
      )
      Rails.logger.info("[ApiContext] LMEセッションを保存（#{(LME_SESSION_TTL / 3600).to_i}h有効）")
    rescue => e
      Rails.logger.warn("[ApiContext] セッション保存に失敗: #{e.class} #{e.message}")
    end

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
