# frozen_string_literal: true
require "json"
require "cgi"
require "faraday"

class LmeAuthClient
  REDIS_KEY = ENV.fetch("LME_COOKIE_REDIS_KEY", "lme:cookie")
  BASE_URL  = ENV.fetch("LME_BASE_URL", "https://step.lme.jp")

  class RecaptchaRequired < StandardError; end

  def cookie
    ck = Sidekiq.redis { |r| r.get(REDIS_KEY) } ||
         Rails.application.credentials.dig(:lme, :cookie) ||
         ENV["LME_COOKIE"]
    raise "Bootstrap cookie missing. Please set via rake lme:cookie:set" if ck.blank?
    ck
  end

  def manual_set!(cookie_str)
    raise "cookie_str blank" if cookie_str.to_s.strip.empty?
    Sidekiq.redis { |r| r.set(REDIS_KEY, cookie_str) }
    cookie_str
  end

  def conn(cookie_str = cookie)
    xsrf = extract_cookie(cookie_str, "XSRF-TOKEN")
    xsrf = xsrf ? CGI.unescape(xsrf) : nil

    Faraday.new(url: BASE_URL) do |f|
      f.request  :json
      f.response :raise_error
      f.adapter  Faraday.default_adapter
      f.headers["Accept"]           = "application/json, text/plain, */*"
      f.headers["Content-Type"]     = "application/json;charset=UTF-8"
      f.headers["Origin"]           = BASE_URL
      f.headers["Referer"]          = "#{BASE_URL}/basic/overview"
      f.headers["User-Agent"]       = "Mozilla/5.0"
      f.headers["X-Requested-With"] = "XMLHttpRequest"
      f.headers["X-XSRF-TOKEN"]     = xsrf if xsrf.present?
      f.headers["Cookie"]           = cookie_str
    end
  end

  def valid_cookie?(cookie_str = cookie)
    c = conn(cookie_str)
    body = { data: { start: today, end: today }.to_json }
    res  = c.post("/ajax/init-data-history-add-friend", body.to_json)
    refresh_from_response_cookies!(res.headers, current: cookie_str)
    true
  rescue Faraday::ClientError => e
    Rails.logger.warn("[LmeAuthClient] valid_cookie? failed status=#{e.response&.dig(:status)}")
    false
  end

  def refresh_from_response_cookies!(headers, current: cookie)
    set_cookie = headers["set-cookie"] || headers["Set-Cookie"]
    return unless set_cookie

    updated = current.dup
    %w[XSRF-TOKEN laravel_session].each do |name|
      v = extract_set_cookie_value(set_cookie, name)
      next unless v
      updated = replace_cookie_pair(updated, name, v)
    end
    manual_set!(updated) if updated != current
  end

  # ---（オプション）パスワードログインを試みる（CAPTCHA があれば即中止）---
  # 使わなくてもOK。reCAPTCHA が出る場合は RecaptchaRequired を投げます。
  def attempt_password_login!(email:, password:)
    # 1) ログインフォームを取得して _token などを拾う
    form_conn = Faraday.new(url: BASE_URL) { |f| f.adapter Faraday.default_adapter }
    form = form_conn.get("/login")
    html = form.body.to_s
    raise RecaptchaRequired, "CAPTCHA present" if html.include?("私はロボットではありません") || html.downcase.include?("captcha")

    token = html.match(/name="_token"\s+value="([^"]+)"/)&.captures&.first
    raise "CSRF token not found" if token.blank?

    # Cookie 下地（XSRF/laravel_session）
    jar = build_cookie_jar_from_set_cookie(form.headers["set-cookie"])

    # 2) フォームポスト
    login_conn = Faraday.new(url: BASE_URL) do |f|
      f.request :url_encoded
      f.response :raise_error
      f.adapter Faraday.default_adapter
      f.headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
      f.headers["Cookie"]       = jar
    end

    res = login_conn.post("/login", { email: email, password: password, _token: token })
    sc  = res.status
    body = res.body.to_s

    # CAPTCHA 検出
    raise RecaptchaRequired, "CAPTCHA present" if body.include?("私はロボットではありません") || body.downcase.include?("captcha")

    # Set-Cookie 反映
    fresh = merge_set_cookie_into_cookie(jar, res.headers["set-cookie"])
    manual_set!(fresh)
    true
  end

  private

  def today
    Time.now.in_time_zone("Asia/Tokyo").to_date.to_s
  end

  def extract_cookie(cookie_str, key)
    return nil if cookie_str.blank?
    cookie_str.split(";").map(&:strip).each do |pair|
      k, v = pair.split("=", 2)
      return v if k == key
    end
    nil
  end

  def extract_set_cookie_value(set_cookie_header, name)
    m = set_cookie_header&.match(/#{Regexp.escape(name)}=([^;]+);/i)
    m && m[1]
  end

  def replace_cookie_pair(cookie_str, name, value)
    pairs = cookie_str.split(";").map(&:strip)
    found = false
    pairs.map! do |pair|
      k, _v = pair.split("=", 2)
      if k == name
        found = true
        "#{name}=#{value}"
      else
        pair
      end
    end
    pairs << "#{name}=#{value}" unless found
    pairs.join("; ")
  end

  def build_cookie_jar_from_set_cookie(set_cookie)
    return "" if set_cookie.blank?
    # XSRF-TOKEN / laravel_session だけ抜いてヘッダ用に整形
    names = %w[XSRF-TOKEN laravel_session]
    parts = names.filter_map { |n|
      v = extract_set_cookie_value(set_cookie, n)
      v ? "#{n}=#{v}" : nil
    }
    parts.join("; ")
  end

  def merge_set_cookie_into_cookie(base_cookie, set_cookie)
    return base_cookie if set_cookie.blank?
    updated = base_cookie.dup
    %w[XSRF-TOKEN laravel_session].each do |n|
      v = extract_set_cookie_value(set_cookie, n)
      next unless v
      updated = replace_cookie_pair(updated, n, v)
    end
    updated
  end
end
