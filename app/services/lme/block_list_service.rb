# frozen_string_literal: true

module Lme
  class BlockListService
    PATH = '/basic/friendlist/get-friend-user-block'

    def initialize(ctx:) @ctx = ctx end

    def fetch(start_on:, end_on:)
      all     = []
      page_no = 1
      start_jp = Date.parse(start_on).strftime('%Y/%-m/%-d') rescue start_on.to_s
      end_jp   = Date.parse(end_on).strftime('%Y/%-m/%-d')   rescue end_on.to_s

      # ← curl と揃える：参照元は “素の” /basic/friendlist/user-block
      referer = URI.join(@ctx.origin, '/basic/friendlist/user-block').to_s

      # ← meta の CSRF を user-block 画面から確実に取得（取れなければ @ctx.csrf_meta フォールバック）
      csrf_meta = begin
        html, _ = @ctx.http.get_with_cookies(full_cookie_header, referer)
        (html[/<meta\s+name=["']csrf-token["']\s+content=["']([^"']+)["']/, 1] || '').strip
      rescue
        ''
      end
      csrf_meta = @ctx.csrf_meta.to_s if csrf_meta.empty?

      loop do
        form = { page: page_no, start_date: start_jp, end_date: end_jp }

        res = @ctx.http.with_loa_retry do
          @ctx.http.post_form(
            path: PATH, form: form,
            cookie:      full_cookie_header, # ← 重要：Seleniumの all_cookies を連結
            csrf_meta:   csrf_meta,          # ← x-csrf-token は meta の値
            xsrf_cookie: nil,                # ← /basic 配下は不要
            referer:     referer
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

    private

    # Selenium の all_cookies 由来の “全部入り” Cookie ヘッダを作る（Friendlist と同じ実装）
    def full_cookie_header
      if @ctx.respond_to?(:login_cookies) && @ctx.login_cookies.is_a?(Array) && @ctx.login_cookies.any?
        return @ctx.login_cookies.map { |c| "#{c[:name]}=#{c[:value]}" }.join('; ')
      end
      @ctx.cookie_header.to_s
    end
  end
end
