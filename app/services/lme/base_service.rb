# app/services/lme/base_service.rb
# frozen_string_literal: true
module Lme
  class BaseService
    def initialize(auth)
      @auth = auth
    end

    private

    attr_reader :auth

    def apply_json_headers!(conn, referer:)
      conn.headers['Cookie']        = auth.cookie
      conn.headers['Referer']       = referer
      conn.headers['Accept']        = 'application/json, text/plain, */*'
      conn.headers['Content-Type']  = 'application/json;charset=UTF-8'
      attach_csrf_variants!(conn)
    end

    def apply_form_headers!(conn, referer:)
      conn.headers['Cookie']        = auth.cookie
      conn.headers['Referer']       = referer
      conn.headers['Accept']        = '*/*'
      conn.headers['x-requested-with'] = 'XMLHttpRequest'
      attach_csrf_variants!(conn, prefer_meta: true)
    end

    def attach_csrf_variants!(conn, prefer_meta: false)
      token = nil
      if prefer_meta && auth.respond_to?(:csrf_token_for)
        # chat-v3 など meta[name="csrf-token"] が必要な系に対応
        token = auth.csrf_token_for('/basic/chat-v3?lastTimeUpdateFriend=0') rescue nil
      end
      token ||= CGI.unescape(extract_cookie(auth.cookie, 'XSRF-TOKEN').to_s)

      return if token.blank?
      conn.headers['x-csrf-token'] = token
      conn.headers['X-CSRF-TOKEN'] = token
      conn.headers['X-XSRF-TOKEN'] = token
      conn.headers['x-xsrf-token'] = token
      conn.headers['X-Requested-With'] = 'XMLHttpRequest'
    end

    def extract_cookie(cookie_str, key)
      return nil if cookie_str.blank?
      cookie_str.split(';').map(&:strip).each do |pair|
        k, v = pair.split('=', 2)
        return v if k == key
      end
      nil
    end

    def safe_json(str)
      JSON.parse(str)
    rescue
      {}
    end
  end
end
