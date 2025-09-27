# frozen_string_literal: true

module Lme
  class BlockListService
    PATH = '/basic/friendlist/get-friend-user-block'

    def initialize(ctx:)
      @ctx = ctx
    end

    def fetch(start_on:, end_on:)
      all = []
      page_no = 1
      start_jp = Date.parse(start_on).strftime('%Y/%-m/%-d') rescue start_on.to_s
      end_jp   = Date.parse(end_on).strftime('%Y/%-m/%-d')   rescue end_on.to_s

      loop do
        form = { page: page_no, start_date: start_jp, end_date: end_jp }
        res = @ctx.http.with_loa_retry(@ctx.cookie_header, @ctx.xsrf_header) do
          @ctx.http.post_form(
            path: PATH, form: form,
            cookie: @ctx.cookie_header, csrf_meta: @ctx.csrf_meta, xsrf_cookie: @ctx.xsrf_header,
            referer: "#{@ctx.origin}/basic/friendlist/user-block"
          )
        end

        payload   = JSON.parse(res) rescue {}
        container = payload['data'] || payload['result'] || payload
        list_src  = if container.is_a?(Hash)
                      container['data'] || container['list'] || container['items'] || container['rows'] || []
                    else
                      container
                    end
        data = Array(list_src)
        break if data.empty?
        all.concat(data)

        cur  = container.is_a?(Hash) ? container['current_page'].to_i : 0
        last = container.is_a?(Hash) ? container['last_page'].to_i    : 0
        break if last.nonzero? && cur >= last
        page_no += 1
        sleep 0.15
      end
      all
    rescue => e
      Rails.logger.debug("[BlockListService] #{e.class}: #{e.message}")
      []
    end
  end
end
