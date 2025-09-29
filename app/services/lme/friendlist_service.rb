# frozen_string_literal: true
module Lme
  class FriendlistService
    PATH = '/basic/friendlist/post-advance-filter-v2'

    def initialize(ctx:) @ctx = ctx end

    def fetch_between(start_on:, end_on:)
      page_no = 1
      rows    = []

      # ← curl と揃える：参照元は“素の” /basic/friendlist に固定
      referer = URI.join(@ctx.origin, '/basic/friendlist').to_s

      # ← meta の CSRF を /basic/friendlist から確実に取る
      csrf_meta = begin
        html, _ = @ctx.http.get_with_cookies(full_cookie_header, referer)
        (html[/<meta\s+name=["']csrf-token["']\s+content=["']([^"']+)["']/, 1] || '').strip
      rescue
        ''
      end
      csrf_meta = @ctx.csrf_meta.to_s if csrf_meta.empty?

      loop do
        form = {
          item_search: '[]', item_search_or: '[]',
          scenario_stop_id: '', scenario_id_running: '', scenario_unfinish_id: '',
          orderBy: 0, sort_followed_at_increase: '', sort_last_time_increase: '',
          keyword: '', rich_menu_id: '', page: page_no,
          # ← 成功している curl と同じく空で送る（期間はクライアント側で絞る）
          followed_from: '', followed_to: '',
          connect_db_replicate: 'false', line_user_id_deleted: '',
          is_cross: 'false'
        }

        res = @ctx.http.with_loa_retry do
          @ctx.http.post_form(
            path: PATH, form: form,
            cookie:      full_cookie_header, # ← ここが肝（JnkV0IWS… 等ぜんぶ）
            csrf_meta:   csrf_meta,          # ← x-csrf-token は meta の値
            xsrf_cookie: nil,                # ← /basic は不要
            referer:     referer
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

      # 期間フィルタはローカルで
      start_cut = Date.parse(start_on) rescue Date.new(1970,1,1)
      end_cut   = Date.parse(end_on)   rescue Date.new(2999,1,1)
      rows.select! do |row|
        d = (Date.parse(row['followed_at']) rescue nil)
        d && (start_cut <= d && d <= end_cut)
      end

      rows
    end

    private

    # Selenium の all_cookies 由来の “全部入り” Cookie ヘッダを作る
    def full_cookie_header
      # 1) login_cookies が配列で持ってるならそれを使う
      if @ctx.respond_to?(:login_cookies) && @ctx.login_cookies.is_a?(Array) && @ctx.login_cookies.any?
        # ドメイン/有効期限などは無視して name=value 連結
        pairs = @ctx.login_cookies.map { |c| "#{c[:name]}=#{c[:value]}" }
        return pairs.join('; ')
      end
      # 2) フォールバック：従来の cookie_header（足りないかも）
      @ctx.cookie_header.to_s
    end
  end
end
