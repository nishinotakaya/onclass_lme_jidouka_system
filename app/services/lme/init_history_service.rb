# app/services/lme/init_history_service.rb
# frozen_string_literal: true
module Lme
  class InitHistoryService
    PATH = '/ajax/init-data-history-add-friend'

    def initialize(ctx:) @ctx = ctx end

    def warmup!(start_on:, end_on:)
      payload = { data: { start: start_on, end: end_on }.to_json } # ← JSON 文字列で包むのは従来どおり
      ref = @ctx.basic_referer_for('/basic/friendlist/friend-history') # ← botIdCurrent 付きにする

      @ctx.http.with_loa_retry(@ctx.cookie_header, @ctx.xsrf_header) do
        @ctx.http.post_json(
          path: PATH,
          json: payload,
          cookie: @ctx.cookie_header,
          xsrf_cookie: @ctx.xsrf_header,
          referer: ref,
          # 参考 curl に合わせる（無くても動くケース多いが付けておく）
          extra_headers: { 'x-server' => 'ovh' }
        )
      end
      true
    end
  end
end
