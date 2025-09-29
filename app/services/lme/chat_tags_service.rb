# frozen_string_literal: true

module Lme
  class ChatTagsService
    PATH = '/basic/chat/get-categories-tags'

    def initialize(ctx:)
      @ctx = ctx
    end

    # タグ一覧を取得（空/短文レスはワンリトライ）
    def fetch_for(line_user_id:)
      referer = "#{@ctx.origin}/basic/chat-v3?friend_id=#{line_user_id}"
      csrf    = csrf_from_meta(referer) # meta優先 / フォールバックあり
      form    = { line_user_id: line_user_id, is_all_tag: 0, botIdCurrent: @ctx.bot_id }

      body = post_once(form: form, referer: referer, csrf_meta: csrf).to_s.strip

      # たまに返る短文（"[]", "{}", "null", ""など）→ 一度だけ CSRF を取り直して再試行
      if body.bytesize < 10 || %w[[] {} null].include?(body)
        @ctx.ensure_csrf_meta! rescue nil
        csrf = csrf_from_meta(referer)
        body = post_once(form: form, referer: referer, csrf_meta: csrf).to_s.strip
      end

      # パース安全化（壊れたJSON/空は空配列扱い）
      json = JSON.parse(body) rescue nil
      container = case json
                  when Hash then json['data'] || json['list'] || json['items'] || json
                  when Array then json
                  else []
                  end
      container
    rescue => e
      Rails.logger.debug("[ChatTagsService] #{e.class}: #{e.message}")
      []
    end

    private

    # meta の CSRF を /basic/chat-v3 から取る（失敗したら @ctx.csrf_meta）
    def csrf_from_meta(referer)
      html, _ = @ctx.http.get_with_cookies(full_cookie_header, referer)
      meta = (html[/<meta\s+name=["']csrf-token["']\s+content=["']([^"']+)["']/, 1] || '').strip rescue ''
      meta = @ctx.csrf_meta.to_s if meta.empty?
      meta
    end

    # 実送信（/basic は x-csrf-token のみでOK。cookie は “全部入り” を使う）
    def post_once(form:, referer:, csrf_meta:)
      @ctx.http.with_loa_retry do
        @ctx.http.post_form(
          path: PATH, form: form,
          cookie:      full_cookie_header,
          csrf_meta:   csrf_meta,
          xsrf_cookie: nil,
          referer:     referer
        )
      end
    end

    # Selenium の all_cookies から “全部入り” Cookie ヘッダを生成
    def full_cookie_header
      if @ctx.respond_to?(:login_cookies) && @ctx.login_cookies.is_a?(Array) && @ctx.login_cookies.any?
        return @ctx.login_cookies.map { |c| "#{c[:name]}=#{c[:value]}" }.join('; ')
      end
      @ctx.cookie_header.to_s
    end
  end
end
