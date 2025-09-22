# app/services/lme_auth_client.rb
class LmeAuthClient
  require "http/cookie_jar"
  attr_accessor :current_cookies, :driver

  BASE_URL = "https://step.lme.jp"

  def initialize(driver = nil)
    @driver = driver
  end

  # Cookie 文字列と配列（任意）を受け取り、Faraday ヘッダに反映
  def manual_set!(cookie_str, cookies = [])
    @cookie = cookie_str.to_s
    @current_cookies = cookies if cookies.any?

    conn.headers["Cookie"] = @cookie

    xsrf = if cookies.any?
      cookies.find { |c| (c[:name] || c["name"]) == "XSRF-TOKEN" }&.dig(:value) ||
        cookies.find { |c| (c[:name] || c["name"]) == "XSRF-TOKEN" }&.dig("value")
    else
      extract_cookie(@cookie, "XSRF-TOKEN")
    end

    if xsrf
      token = CGI.unescape(xsrf.to_s)
      conn.headers["X-XSRF-TOKEN"] = token
      conn.headers["x-xsrf-token"] = token
    end
  end

  # Playwright などから取得した最新 Cookie/Token をまとめて適用
  # app/services/lme_auth_client.rb

  def apply_session!(cookie_header, xsrf, referer:)
    conn.headers["Cookie"]           = cookie_header
    conn.headers["X-XSRF-TOKEN"]     = xsrf if xsrf
    conn.headers["x-xsrf-token"]     = xsrf if xsrf
    conn.headers["x-requested-with"] = "XMLHttpRequest"
    conn.headers["Accept"]           = "application/json, text/plain, */*"
    conn.headers["Content-Type"]     = "application/json;charset=UTF-8"
    conn.headers["Referer"]          = referer
    conn.headers["Origin"]           = LmeAuthClient::BASE_URL
    @cookie = cookie_header
  end



  # Set-Cookie の取り込み（レスポンス -> 内部 Cookie へマージ）
  def refresh_from_response_cookies!(headers)
    raw = headers["set-cookie"] || headers["Set-Cookie"]
    return unless raw.present?

    merged = (Array(@current_cookies) + parse_cookie_header(raw)).uniq { |c| c[:name] }
    @current_cookies = merged
    @cookie = @current_cookies.map { |c| "#{c[:name]}=#{c[:value]}" }.join("; ")

    @conn.headers["Cookie"] = @cookie if defined?(@conn) && @conn
  end

  # ✅ 追加：現在の Cookie が有効か簡易チェック
  def valid_cookie?
    return false if cookie.blank?
    begin
      probe = Faraday.new(url: BASE_URL) { |f| f.response :raise_error; f.adapter Faraday.default_adapter }
      probe.headers["Cookie"] = cookie
      xsrf = extract_cookie(cookie, "XSRF-TOKEN")
      probe.headers["x-xsrf-token"] = CGI.unescape(xsrf.to_s) if xsrf
      res = probe.get("/basic/overview")
      res.status == 200
    rescue Faraday::Error
      false
    end
  end

  def cookie = @cookie

  def conn
    @conn ||= Faraday.new(url: BASE_URL) do |f|
      f.request :url_encoded
      f.response :raise_error
      f.adapter Faraday.default_adapter
    end
  end

  # 🚀 追加：CSRF トークン取得（Cookie から）
  def csrf_token_for(_path = nil)
    extract_cookie(@cookie, "XSRF-TOKEN")
  end

  # レスポンス Set-Cookie から {name,value} 配列へ
  private def parse_cookie_header(raw)
    raw.split(/,(?=[^ ;]+=)/).map do |chunk|
      pair = chunk.split(";", 2).first
      k, v = pair.strip.split("=", 2)
      { name: k, value: v }
    end
  end

  private def extract_cookie(cookie_str, key)
    return nil if cookie_str.blank?
    cookie_str.split(";").map(&:strip).each do |pair|
      k, v = pair.split("=", 2)
      return CGI.unescape(v) if k == key
    end
    nil
  end
end
