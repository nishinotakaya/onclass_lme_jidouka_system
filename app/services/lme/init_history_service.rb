# frozen_string_literal: true

module Lme
  class InitHistoryService
    PATH = '/ajax/init-data-history-add-friend'

    def initialize(ctx:)
      @ctx = ctx
    end

    def warmup!(start_on:, end_on:)
      form = { data: { start: start_on, end: end_on }.to_json }
      @ctx.http.with_loa_retry(@ctx.cookie_header, @ctx.xsrf_header) do
        @ctx.http.post_form(
          path: PATH, form: form,
          cookie: @ctx.cookie_header, csrf_meta: nil, xsrf_cookie: @ctx.xsrf_header,
          referer: "#{@ctx.origin}/basic/friendlist/friend-history"
        )
      end
      true
    end
  end
end
