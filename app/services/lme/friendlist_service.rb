# frozen_string_literal: true

module Lme
  class FriendlistService
    PATH = '/basic/friendlist/post-advance-filter-v2'

    def initialize(ctx:)
      @ctx = ctx
    end

    # 期間でページング取得
    def fetch_between(start_on:, end_on:)
      page_no = 1
      rows = []
      loop do
        form = {
          item_search: '[]', item_search_or: '[]',
          scenario_stop_id: '', scenario_id_running: '', scenario_unfinish_id: '',
          orderBy: 0, sort_followed_at_increase: '', sort_last_time_increase: '',
          keyword: '', rich_menu_id: '', page: page_no,
          followed_to: end_on, followed_from: start_on,
          connect_db_replicate: 'false', line_user_id_deleted: '',
          is_cross: 'false'
        }
        res = @ctx.http.with_loa_retry(@ctx.cookie_header, @ctx.xsrf_header) do
          @ctx.http.post_form(
            path: PATH, form: form,
            cookie: @ctx.cookie_header, csrf_meta: @ctx.csrf_meta, xsrf_cookie: @ctx.xsrf_header,
            referer: "#{@ctx.origin}/basic/friendlist"
          )
        end
        body = JSON.parse(res) rescue {}
        data = Array(body.dig('data', 'data'))
        break if data.empty?
        rows.concat(data)

        cur  = body.dig('data', 'current_page').to_i
        last = body.dig('data', 'last_page').to_i
        break if last.zero? || cur >= last
        page_no += 1
        sleep 0.15
      end
      rows
    end
  end
end
