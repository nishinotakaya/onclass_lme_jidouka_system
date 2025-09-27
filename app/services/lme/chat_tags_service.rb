# frozen_string_literal: true

module Lme
  class ChatTagsService
    PATH = '/basic/chat/get-categories-tags'

    def initialize(ctx:)
      @ctx = ctx
    end

    def fetch_for(line_user_id:)
      form = { line_user_id: line_user_id, is_all_tag: 0, botIdCurrent: @ctx.bot_id }
      @ctx.http.with_loa_retry(@ctx.cookie_header, @ctx.xsrf_header) do
        @ctx.http.post_form(
          path: PATH, form: form,
          cookie: @ctx.cookie_header, csrf_meta: @ctx.csrf_meta, xsrf_cookie: @ctx.xsrf_header,
          referer: "#{@ctx.origin}/basic/chat-v3?friend_id=#{line_user_id}"
        )
      end
    end
  end
end
