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

    # 保存済みLMEセッションの有効期限。過ぎたら再ログイン（使用前に必ずHTTPで有効性検証もする）。
    LME_SESSION_TTL = 12.hours

    # --- ログイン & /basic の Cookie/XSRF 確立（LoginUserService = Selenium） ------
    # ★ 毎回 Selenium+2Captcha でログインすると 2Captcha 残高を消費するため、
    #   有効なセッション(Cookie)を DB(lme_sessions) に保存し、次回は再利用する。
    #   有効性チェックは HTTP のみ（2Captcha/Selenium を使わない）。
    #   セッションが無効/期限切れの時だけ通常ログイン（＝再ログイン処理）を行う。
    def login_with_google!(email:, password:, api_key:)
      return self if restore_session_from_db!

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
      save_session_to_db!
      self
    end

    # 有効なセッションが失効/壊れた時に呼ぶ（DBを消して次回フルログインさせる）
    def invalidate_session!
      LmeSession.where(bot_id: @bot_id).delete_all
    rescue => e
      Rails.logger.warn("[ApiContext] セッション削除に失敗: #{e.class} #{e.message}")
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

    # DBの保存済みセッションを復元し、HTTPで有効性を確認できたら true（＝再ログイン不要）。
    # 期限切れ・無効なら false を返し、呼び出し側で通常ログイン（再ログイン処理）が走る。
    def restore_session_from_db!
      row = LmeSession.find_by(bot_id: @bot_id)
      return false if row.nil? || row.cookie_header.blank?

      if row.expired?
        Rails.logger.info('[ApiContext] DBのLMEセッションが期限切れ → 再ログイン')
        return false
      end

      @login_cookies = symbolize_login_cookies(row.login_cookies)
      @cookie_header = row.cookie_header
      @xsrf_header   = row.xsrf_header
      @csrf_meta     = row.csrf_meta
      @basic_url     = row.basic_url
      @driver        = nil # 再利用時はSeleniumドライバを持たない

      # 認証済みでしか成功しないAJAXでセッションの生死を判定（HTTPのみ・2Captcha/Selenium不要）
      if session_alive?
        Rails.logger.info('[ApiContext] DBのLMEセッションを再利用（再ログイン/2Captchaなし）')
        return true
      end

      Rails.logger.info('[ApiContext] DBのLMEセッションが無効 → 再ログイン')
      false
    rescue => e
      Rails.logger.warn("[ApiContext] セッション復元に失敗（再ログインへ）: #{e.class} #{e.message}")
      false
    end

    # 認証済みでしか {status:true, groups:[...]} を返さないAJAXで、セッションの生死を判定
    def session_alive?
      conn = Faraday.new(url: origin) { |f| f.adapter Faraday.default_adapter }
      res = conn.post('/ajax/get-list-group-landing') do |req|
        req.headers['accept']           = 'application/json, text/plain, */*'
        req.headers['content-type']     = 'application/x-www-form-urlencoded; charset=UTF-8'
        req.headers['x-requested-with'] = 'XMLHttpRequest'
        req.headers['x-csrf-token']     = @csrf_meta.to_s
        req.headers['cookie']           = effective_cookie_header
        req.headers['referer']          = "#{origin}/basic/landing"
        req.body = ''
      end
      body = res.body.to_s
      res.status == 200 && body.lstrip.start_with?('{') && body.include?('"groups"')
    rescue => e
      Rails.logger.debug("[ApiContext] session_alive? 判定失敗: #{e.class} #{e.message}")
      false
    end

    # AJAX認証用の完全なCookie（login_cookiesがあれば優先、無ければ sanitized cookie_header）
    def effective_cookie_header
      if @login_cookies.is_a?(Array) && @login_cookies.any?
        return @login_cookies.map { |c| "#{c[:name]}=#{c[:value]}" }.join('; ')
      end

      @cookie_header.to_s
    end

    # ログイン成功時にセッションを DB(lme_sessions) へ保存（bot単位でupsert）
    def save_session_to_db!
      row = LmeSession.find_or_initialize_by(bot_id: @bot_id)
      row.assign_attributes(
        cookie_header: @cookie_header,
        xsrf_header:   @xsrf_header,
        csrf_meta:     @csrf_meta,
        basic_url:     @basic_url,
        login_cookies: @login_cookies,
        last_login_at: Time.current,
        expires_at:    Time.current + LME_SESSION_TTL
      )
      row.save!
      Rails.logger.info("[ApiContext] LMEセッションをDB保存（#{(LME_SESSION_TTL / 3600).to_i}h有効）")
    rescue => e
      Rails.logger.warn("[ApiContext] セッションDB保存に失敗（本処理は継続）: #{e.class} #{e.message}")
    end

    # jsonb から読んだ login_cookies を [{name:, value:}] のシンボルキーに戻す
    def symbolize_login_cookies(cookies)
      return cookies unless cookies.is_a?(Array)

      cookies.map { |c| c.is_a?(Hash) ? c.transform_keys(&:to_sym) : c }
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
