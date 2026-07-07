# frozen_string_literal: true
require 'json'

module Lme
  # QRコードアクション（ランディング）の作成・設定・URL取得
  # https://step.lme.jp/basic/landing の「新規作成」相当を API で行う
  class LandingService
    PATH_CREATE_LANDING = '/basic/create-landing-v2'
    PATH_SETTING_DETAIL = '/ajax/v2/landing/%<landing_id>s/setting-detail'
    PATH_LANDING_EDIT   = '/basic/landing/v2/edit/%<landing_id>s'
    PATH_LANDING_LIST   = '/basic/landing'

    LANDING_QR_URL_PATTERN = %r{https://s\.lmes\.jp/landing-qr/[A-Za-z0-9\-]+\?uLand=[A-Za-z0-9]+}

    def initialize(ctx:) @ctx = ctx end

    # ランディングを作成して { landing_id:, landing_url:, raw_body: } を返す。
    # landing_url はレスポンスに含まれていれば同時に返す（無ければ nil）。
    def create_landing(name:, category_id:, action_with_friend: 1)
      form = {
        'newQrs[name]'               => name.to_s,
        'newQrs[category_id]'        => category_id.to_s,
        'newQrs[action_with_friend]' => action_with_friend.to_s
      }
      body = post_urlencoded(PATH_CREATE_LANDING, form, referer: landing_list_url)

      landing_id = extract_landing_id(body)
      if landing_id
        Rails.logger.info("[LmeLanding] create-landing-v2 ok name=#{name} landing_id=#{landing_id}")
      else
        Rails.logger.warn("[LmeLanding] create-landing-v2 id not found name=#{name} resp=#{body.to_s[0, 500]}")
      end
      {
        landing_id:  landing_id,
        landing_url: body.to_s[LANDING_QR_URL_PATTERN],
        raw_body:    body
      }
    end

    # 作成直後の「アクション設定」保存（UI の設定ダイアログの保存相当）
    def update_setting_detail(landing_id:)
      form = {
        'action_id'            => '',
        'general_message'      => '',
        'action_type'          => '2',
        'use_msg_new_friend'   => '1',
        'use_msg_old_friend'   => '0',
        'use_msg_unblock'      => '0',
        'interval_action'      => '0',
        'time_interval_action' => ''
      }
      path = format(PATH_SETTING_DETAIL, landing_id: landing_id)
      post_urlencoded(path, form, referer: landing_edit_url(landing_id))
    end

    # 編集画面 HTML から https://s.lmes.jp/landing-qr/...?uLand=... を抽出
    def fetch_landing_url(landing_id)
      html, _url = @ctx.http.get_with_cookies(cookie_header, landing_edit_url(landing_id))
      html.to_s[LANDING_QR_URL_PATTERN]
    end

    private

    def post_urlencoded(path, form, referer:)
      csrf = ensure_csrf!(referer)
      @ctx.http.with_loa_retry do
        @ctx.http.post_form(
          path:        path,
          form:        form,
          cookie:      cookie_header,
          csrf_meta:   csrf,
          xsrf_cookie: nil, # /basic 配下は XSRF cookie 不要（BroadcastService と同様）
          referer:     referer
        )
      end
    end

    def ensure_csrf!(referer)
      html, _ = @ctx.http.get_with_cookies(cookie_header, referer)
      meta = (html[/<meta\s+name=["']csrf-token["']\s+content=["']([^"']+)["']/, 1] || '').to_s.strip rescue ''
      meta.present? ? meta : @ctx.csrf_meta.to_s
    rescue
      @ctx.csrf_meta.to_s
    end

    def cookie_header
      if @ctx.respond_to?(:login_cookies) && @ctx.login_cookies.is_a?(Array) && @ctx.login_cookies.any?
        return @ctx.login_cookies.map { |c| "#{c[:name]}=#{c[:value]}" }.join('; ')
      end
      @ctx.cookie_header.to_s
    end

    # レスポンス JSON から作成されたランディングの id を掘り出す。
    # 返却形が {"id":..} / {"data":{"id":..}} / {"landing":{"id":..}} 等どれでも拾えるよう
    # 再帰的に "id" キーを探す。
    def extract_landing_id(body)
      json = JSON.parse(body.to_s) rescue nil
      return nil unless json

      find_first_id(json)
    end

    def find_first_id(node)
      case node
      when Hash
        id_value = node['id'] || node['landing_id']
        return id_value.to_i if id_value.to_s =~ /\A\d+\z/

        node.each_value do |child|
          found = find_first_id(child)
          return found if found
        end
        nil
      when Array
        node.each do |child|
          found = find_first_id(child)
          return found if found
        end
        nil
      end
    end

    def landing_list_url
      "#{@ctx.origin}#{PATH_LANDING_LIST}"
    end

    def landing_edit_url(landing_id)
      "#{@ctx.origin}#{format(PATH_LANDING_EDIT, landing_id: landing_id)}"
    end
  end
end
