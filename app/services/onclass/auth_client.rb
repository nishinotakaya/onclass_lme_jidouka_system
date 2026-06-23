# frozen_string_literal: true
require "faraday"
require "json"

module Onclass
  # OnClass 管理画面 API の認証クライアント。
  #
  # 2026/03 に OnClass 側の認証が devise_token_auth（access-token/client/uid ヘッダ）から
  # Cookie セッション + CSRF トークン方式へ変更された（旧 /v1/enterprise_manager/auth/sign_in は 410 Gone）。
  # 新フロー:
  #   1) GET  /v1/auth/enterprise_manager/csrf_token   → x-csrf-token ヘッダ取得
  #   2) POST /v1/auth/enterprise_manager/session       body={session:{email,password}} + X-CSRF-Token
  #      → 認証され Set-Cookie でセッション Cookie が返る（以降の API は Cookie で認証）
  # 以降の各 API 呼び出しは Cookie を送る。更新系(POST/PUT/PATCH/DELETE)は X-CSRF-Token も付与する。
  class AuthClient
    REDIS_KEY_PREFIX = "onclass:auth_session" # 旧 token キャッシュ(onclass:auth_headers)とは別キーにして混在を防ぐ
    SESSION_TTL_SEC  = 60 * 60 * 12

    CSRF_PATH    = "/v1/auth/enterprise_manager/csrf_token"
    SESSION_PATH = "/v1/auth/enterprise_manager/session"

    attr_reader :conn, :base_url, :email

    def initialize(base_url: ENV.fetch("ONLINE_CLASS_API_BASE", "https://api.the-online-class.com"),
                  email: ENV.fetch("ONLINE_CLASS_EMAIL", nil),
                  password: ENV.fetch("ONLINE_CLASS_PASSWORD", nil))
      @base_url = base_url
      @email    = email
      @password = password

      # 認証フローは自前でステータスを見て分岐するため raise_error は付けない
      @conn = Faraday.new(url: @base_url) do |f|
        f.request :json
        f.adapter Faraday.default_adapter
      end
    end

    # ========= 複数資格情報の取り出し =========
    def self.credentials_from_env
      creds = []

      if ENV["ONLINE_CLASS_CREDENTIALS"].present?
        begin
          arr = JSON.parse(ENV["ONLINE_CLASS_CREDENTIALS"])
          Array(arr).each do |h|
            e = h["email"] || h[:email]
            p = h["password"] || h[:password]
            creds << { email: e, password: p } if e.present? && p.present?
          end
        rescue JSON::ParserError
          # 無視
        end
      end

      if creds.empty?
        i = 1
        loop do
          e = ENV["ONLINE_CLASS_EMAIL_#{i}"]
          p = ENV["ONLINE_CLASS_PASSWORD_#{i}"]
          break if e.blank? && p.blank?
          creds << { email: e, password: p } if e.present? && p.present?
          i += 1
        end
      end

      if creds.empty? && ENV["ONLINE_CLASS_EMAIL"].to_s.include?(",")
        emails = ENV["ONLINE_CLASS_EMAIL"].to_s.split(/[,;\s]+/).reject(&:blank?)
        pwds   = ENV["ONLINE_CLASS_PASSWORD"].to_s.split(/[,;\s]+/).reject(&:blank?)
        emails.zip(pwds).each do |e, p|
          creds << { email: e, password: p } if e.present? && p.present?
        end
      end

      if creds.empty?
        e = ENV["ONLINE_CLASS_EMAIL"]
        p = ENV["ONLINE_CLASS_PASSWORD"]
        creds << ({ email: e, password: p }) if e.present? && p.present?
      end

      creds.uniq { |h| [h[:email], h[:password]] }
    end

    # ========= セッション取得 =========
    # Redis キャッシュ: { "cookie" => "k=v; k2=v2", "csrf" => "..." }
    def cached_session
      raw = Sidekiq.redis { |r| r.get(redis_key) }
      raw ? JSON.parse(raw) : nil
    end

    # csrf_token → session の 2 ステップでログインし、Cookie/CSRF を保存して返す
    def sign_in!
      raise "email/password required" if email.blank? || @password.blank?

      csrf_res  = @conn.get(CSRF_PATH) { |req| apply_common_headers(req) }
      init_csrf = response_csrf(csrf_res)
      init_jar  = cookie_jar(csrf_res.headers["set-cookie"])

      res = @conn.post(SESSION_PATH) do |req|
        apply_common_headers(req)
        req.headers["X-CSRF-Token"] = init_csrf if init_csrf.present?
        req.headers["Cookie"]       = init_jar  if init_jar.present?
        req.body = { session: { email: email, password: @password } }
      end

      body = (JSON.parse(res.body) rescue {})
      unless res.status == 200 && body["authenticated"]
        errors = Array(body["errors"]).join(", ")
        raise "Onclass sign-in failed (status=#{res.status}) #{errors}"
      end

      session = {
        "cookie" => cookie_jar(res.headers["set-cookie"]) || init_jar,
        "csrf"   => response_csrf(res) || init_csrf
      }
      raise "Onclass sign-in succeeded but no session cookie returned" if session["cookie"].blank?

      save_session!(session)
      session
    end

    # 有効なセッション（キャッシュがなければログイン）
    def session
      cached_session || sign_in!
    end

    # 認証付きヘッダ。GET は Cookie のみ、更新系は with_csrf: true で CSRF も付与する
    def auth_headers(with_csrf: false)
      s = session
      headers = { "Cookie" => s["cookie"] }
      headers["X-CSRF-Token"] = s["csrf"] if with_csrf && s["csrf"].present?
      headers.compact
    end

    # 旧 API 互換: かつて token ヘッダ(access-token/client/uid)を返していた #headers の置き換え。
    # 新方式では Cookie + CSRF を返す（リクエストヘッダにそのまま merge できる）。
    def headers
      auth_headers(with_csrf: true)
    end

    private

    def redis_key
      "#{REDIS_KEY_PREFIX}:#{email}"
    end

    def apply_common_headers(req)
      req.headers["accept"]     = "application/json, text/plain, */*"
      req.headers["origin"]     = "https://manager.the-online-class.com"
      req.headers["referer"]    = "https://manager.the-online-class.com/"
      req.headers["user-agent"] = "Mozilla/5.0"
    end

    def response_csrf(res)
      res.headers["x-csrf-token"] || res.headers["X-CSRF-Token"]
    end

    # Set-Cookie ヘッダ群を "k=v; k2=v2" 形式の Cookie リクエストヘッダ文字列へ変換
    def cookie_jar(set_cookie)
      return nil if set_cookie.blank?
      # 複数 Cookie がカンマ連結されるケースに対応（属性の Expires 等の "," とは区別する）
      set_cookie.split(/,(?=\s*[^;,\s]+=)/).map { |c| c.split(";").first.strip }.reject(&:empty?).join("; ")
    end

    def save_session!(session)
      Sidekiq.redis { |r| r.set(redis_key, session.to_json, ex: SESSION_TTL_SEC) }
    end
  end
end
