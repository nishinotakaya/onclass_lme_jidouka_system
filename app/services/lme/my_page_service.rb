# frozen_string_literal: true

module Lme
  class MyPageService
    PATH = '/ajax/get_data_my_page'

    def initialize(ctx:)
      @ctx = ctx
    end

    def fetch_common(line_user_id:)
      path = "#{PATH}?page=1&type=common&user_id=#{line_user_id}"
      @ctx.http.get_json(path: path, cookie: @ctx.cookie_header,
                         referer: "#{@ctx.origin}/basic/friendlist/my_page/#{line_user_id}")
    rescue => e
      Rails.logger.debug("[MyPageService] uid=#{line_user_id} #{e.class}: #{e.message}")
      {}
    end
  end
end
