# frozen_string_literal: true
module Lme
  class ChatTagsService
    PATH = '/basic/chat/get-categories-tags'

    def initialize(ctx:) @ctx = ctx end

    # タグ一覧を取得（空/短文レスはワンリトライ）
    def fetch_for(line_user_id:)
      referer = "#{@ctx.origin}/basic/chat-v3?friend_id=#{line_user_id}"
      csrf    = csrf_from_meta(referer)
      form    = { line_user_id: line_user_id, is_all_tag: 0, botIdCurrent: @ctx.bot_id }

      body = post_once(form: form, referer: referer, csrf_meta: csrf).to_s.strip
      if body.bytesize < 10 || %w[[] {} null].include?(body)
        @ctx.ensure_csrf_meta! rescue nil
        csrf = csrf_from_meta(referer)
        body = post_once(form: form, referer: referer, csrf_meta: csrf).to_s.strip
      end
      body
    rescue => e
      Rails.logger.debug("[ChatTagsService] #{e.class}: #{e.message}")
      '[]'
    end

    private

    def csrf_from_meta(referer)
      html, _ = @ctx.http.get_with_cookies(full_cookie_header, referer)
      meta = (html[/<meta\s+name=["']csrf-token["']\s+content=["']([^"']+)["']/, 1] || '').strip rescue ''
      meta = @ctx.csrf_meta.to_s if meta.empty?
      meta
    end

    def post_once(form:, referer:, csrf_meta:)
      @ctx.http.with_loa_retry do
        @ctx.http.post_form(
          path: PATH, form: form,
          cookie:      full_cookie_header,
          csrf_meta:   csrf_meta,   # => x-csrf-token
          xsrf_cookie: nil,         # /basic 配下は不要
          referer:     referer
        )
      end
    end

    def full_cookie_header
      if @ctx.respond_to?(:login_cookies) && @ctx.login_cookies.is_a?(Array) && @ctx.login_cookies.any?
        return @ctx.login_cookies.map { |c| "#{c[:name]}=#{c[:value]}" }.join('; ')
      end
      @ctx.cookie_header.to_s
    end
  end
end
