# frozen_string_literal: true
module Lme
  class MyPageService
    PATH = '/ajax/get_data_my_page'
    def initialize(ctx:) @ctx = ctx end

    # 共通タブの JSON を取得（必ず Hash を返す）
    def fetch_common(line_user_id:)
      url     = URI.join(@ctx.origin, "#{PATH}?page=1&type=common&user_id=#{line_user_id}").to_s
      referer = "#{@ctx.origin}/basic/friendlist/my_page/#{line_user_id}"

      body, _headers = @ctx.http.get_with_cookies(full_cookie_header, url) # ← Cookie必須
      json = JSON.parse(body) rescue {}
      json.is_a?(Hash) ? json : {}
    rescue => e
      Rails.logger.debug("[MyPageService] uid=#{line_user_id} #{e.class}: #{e.message}")
      {}
    end

    # /ajax/get_data_my_page の JSON から流入元っぽい値を抜く
    def extract_inflow(common_json)
      h = common_json.is_a?(Hash) ? common_json : {}
      h['inflow'] || h['flow'] || h['route'] || h['entry_route'] ||
        h.dig('data','inflow') || h.dig('data','route') ||
        h.dig('infoBasic','inflow') || '' # 念のため
    end

    private

    # Selenium の all_cookies から “全部入り” Cookie ヘッダを生成
    def full_cookie_header
      if @ctx.respond_to?(:login_cookies) && @ctx.login_cookies.is_a?(Array) && @ctx.login_cookies.any?
        return @ctx.login_cookies.map { |c| "#{c[:name]}=#{c[:value]}" }.join('; ')
      end
      @ctx.cookie_header.to_s
    end
  end
end
