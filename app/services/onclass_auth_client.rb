# frozen_string_literal: true
require "faraday"
require "json"

class OnclassAuthClient
  REDIS_KEY = "onclass:auth_headers"

  # todoメールアドレス登録ができるようになり次第ONLINE_CLASS_EMAILとONLINE_CLASS_PASSWORDは変更する
  def initialize(base_url: ENV.fetch("ONLINE_CLASS_API_BASE", "https://api.the-online-class.com"),
                 login_path: ENV.fetch("ONLINE_CLASS_LOGIN_PATH", "/v1/enterprise_manager/auth/sign_in"),
                 email: ENV.fetch("ONLINE_CLASS_EMAIL"),
                 password: ENV.fetch("ONLINE_CLASS_PASSWORD"))
    @base_url = base_url
    @login_path = login_path
    @email = email
    @password = password

    @conn = Faraday.new(url: @base_url) do |f|
      f.request :json
      f.response :raise_error
      f.adapter Faraday.default_adapter
    end
  end

  def cached_headers
    raw = Sidekiq.redis { |r| r.get(REDIS_KEY) }
    raw ? JSON.parse(raw) : nil
  end

  def sign_in!
    res = @conn.post(@login_path) do |req|
      req.headers["Content-Type"] = "application/json"
      req.body = { email: @email, password: @password }
    end

    headers = extract_token_headers(res)
    save_headers!(headers)
    headers
  end

  # 有効なヘッダ（キャッシュがなければログイン）
  def headers
    cached_headers || sign_in!
  end

  # レスポンスに新しいトークンが返ってきたら差し替え（DeviseTokenAuthの更新）
  def refresh_from_response!(res)
    new_headers = extract_token_headers(res, allow_partial: true)
    save_headers!(new_headers) if new_headers
  end

  private

  TOKEN_KEYS = %w[access-token client uid token-type expiry].freeze

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
    Sidekiq.redis { |r| r.set(REDIS_KEY, headers.to_json) }
  end
end
