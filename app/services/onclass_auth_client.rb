# frozen_string_literal: true
require "faraday"
require "json"

class OnclassAuthClient
  REDIS_KEY_PREFIX = "onclass:auth_headers"
  TOKEN_KEYS = %w[access-token client uid token-type expiry].freeze

  attr_reader :conn, :base_url, :email

  def initialize(base_url: ENV.fetch("ONLINE_CLASS_API_BASE", "https://api.the-online-class.com"),
                 login_path: ENV.fetch("ONLINE_CLASS_LOGIN_PATH", "/v1/enterprise_manager/auth/sign_in"),
                 email: ENV.fetch("ONLINE_CLASS_EMAIL", nil),
                 password: ENV.fetch("ONLINE_CLASS_PASSWORD", nil))
    @base_url   = base_url
    @login_path = login_path
    @email      = email
    @password   = password

    @conn = Faraday.new(url: @base_url) do |f|
      f.request :json
      f.response :raise_error
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
      creds <<({ email: e, password: p }) if e.present? && p.present?
    end

    creds.uniq { |h| [h[:email], h[:password]] }
  end

  # ========= 既存動作（メール別キャッシュ） =========
  def cached_headers
    raw = Sidekiq.redis { |r| r.get(redis_key) }
    raw ? JSON.parse(raw) : nil
  end

  def sign_in!
    raise "email/password required" if email.blank? || @password.blank?

    res = @conn.post(@login_path) do |req|
      req.headers["Content-Type"] = "application/json"
      req.body = { email: email, password: @password }
    end
    headers = extract_token_headers(res)
    save_headers!(headers)
    headers
  end

  # 有効なヘッダ（キャッシュがなければログイン）
  def headers
    cached_headers || sign_in!
  end

  # レスポンスに新しいトークンが返ってきたら差し替え
  def refresh_from_response!(res)
    new_headers = extract_token_headers(res, allow_partial: true)
    save_headers!(new_headers) if new_headers
  end

  private

  def redis_key
    "#{REDIS_KEY_PREFIX}:#{email}"
  end

  def extract_token_headers(response, allow_partial: false)
    h = response.headers
    token_headers = TOKEN_KEYS.filter_map { |k|
      v = h[k] || h[k.downcase]
      v ? [k, v] : nil
    }.to_h
    return nil if token_headers.empty? && allow_partial
    raise "Token headers missing in response" if token_headers.values.any?(&:nil?)
    token_headers
  end

  def save_headers!(headers)
    Sidekiq.redis { |r| r.set(redis_key, headers.to_json) }
  end
end
