# frozen_string_literal: true
require "json"
require "cgi"

module Lme
  class BaseService
    attr_reader :auth

    def initialize(auth: nil)
      @auth = auth || LmeAuthClient.new
    end

    protected

    def safe_json(str)
      JSON.parse(str.to_s)
    rescue JSON::ParserError
      {}
    end

    # ---- x-www-form-urlencoded を投げる系（/basic/chat/...）
    def apply_form_headers!(conn, referer:)
      apply_common_csrf!(conn, referer: referer)
      conn.headers["Accept"]           = "*/*"
      conn.headers["x-requested-with"] = "XMLHttpRequest"
      conn
    end

    # ---- application/json を投げる系（/ajax/...）
    def apply_json_headers!(conn, referer:)
      apply_common_csrf!(conn, referer: referer)
      conn.headers["Accept"]       = "application/json, text/plain, */*"
      conn.headers["Content-Type"] = "application/json;charset=UTF-8"
      conn
    end

    # 共通: Cookie / CSRF / Origin / Referer
    def apply_common_csrf!(conn, referer:)
      conn.headers["Cookie"]  = auth.cookie
      conn.headers["Origin"]  = LmeAuthClient::BASE_URL
      conn.headers["Referer"] = referer

      token = ENV["LME_X_CSRF_TOKEN"].presence || xsrf_from_cookie
      if token.present?
        %w[x-csrf-token X-CSRF-TOKEN X-XSRF-TOKEN x-xsrf-token].each { |k| conn.headers[k] = token }
      end
      conn
    end

    def xsrf_from_cookie
      if (raw = extract_cookie(auth.cookie, "XSRF-TOKEN"))
        CGI.unescape(raw)
      end
    end

    def extract_cookie(cookie_str, key)
      return nil if cookie_str.blank?
      cookie_str.split(";").map(&:strip).each do |pair|
        k, v = pair.split("=", 2)
        return v if k == key
      end
      nil
    end
  end
end
