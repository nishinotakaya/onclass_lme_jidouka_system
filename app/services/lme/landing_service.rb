# frozen_string_literal: true
require 'json'

module Lme
  # QRコードアクション（ランディング）の作成・設定・URL取得
  # https://step.lme.jp/basic/landing の「新規作成」相当を API で行う
  class LandingService
    PATH_CREATE_LANDING = '/basic/create-landing-v2'
    PATH_INIT_DETAIL    = '/ajax/v2/landing/get-init-detail-landing'
    PATH_CREATE_TAG     = '/ajax/save-add-tag-in-modal-action'
    PATH_LIST_GROUP_TAG = '/ajax/get-list-group-tag'
    PATH_ACTION_SAVE    = '/ajax/action/save'
    PATH_UPDATE_BASIC   = '/ajax/v2/landing/update-basic/%<landing_id>s' # PUT
    PATH_SETTING_DETAIL = '/ajax/v2/landing/%<landing_id>s/setting-detail'
    PATH_LANDING_EDIT   = '/basic/landing/v2/edit/%<landing_id>s'
    PATH_LANDING_LIST   = '/basic/landing'
    # ランディング一覧（QRコードアクション一覧）取得 ajax。UI の一覧描画と同じ GET。
    # レスポンス: { data: { data: [ {id, name, ...}, ... ], last_page, current_page } }
    PATH_LANDING_LIST_AJAX = '/ajax/v2/landing'

    # youtube タグをまとめるタグフォルダ（/ajax/get-list-group-tag の group）
    DEFAULT_TAG_FOLDER_ID = '57807'

    # 公開QR URL のドメイン・ベース（全ランディング共通。link_qr_code から導出も可）
    PUBLIC_QR_HOST  = 's.lmes.jp'
    LANDING_QR_BASE = '1627543300-4PoDJorL'

    LANDING_QR_URL_PATTERN = %r{https://s\.lmes\.jp/landing-qr/[A-Za-z0-9\-]+\?uLand=[A-Za-z0-9]+}

    def initialize(ctx:) @ctx = ctx end

    # ランディング作成〜タグ紐付け〜名前/フォルダ確定までの一連を実行し、
    # { landing_id:, landing_url:, action_id:, tag_id: } を返す。
    # name はランディング名 & タグ名の両方に使う（例「西野- <動画タイトル>」）。
    def create_landing_with_tag(name:, category_id:, tag_folder_id: DEFAULT_TAG_FOLDER_ID)
      created    = create_landing(name: name, category_id: category_id)
      landing_id = created[:landing_id]
      return created if landing_id.blank?

      action_id = created[:action_id] || fetch_action_id(landing_id)

      # タグ: 既存があれば再利用、無ければ作成
      tag_id = find_tag_id(tag_folder_id: tag_folder_id, tag_name: name) ||
               create_tag(tag_folder_id: tag_folder_id, tag_name: name)

      if tag_id.present?
        saved_action_id = attach_tag_action(action_id: action_id, tag_id: tag_id, tag_folder_id: tag_folder_id, tag_name: name)
        action_id = saved_action_id if saved_action_id.present?
      else
        Rails.logger.warn("[LmeLanding] tag_id を取得できずタグ紐付けをスキップ name=#{name}")
      end

      # 名前・フォルダを確定（UI の保存相当）
      update_basic(landing_id: landing_id, action_id: action_id, name: name, category_id: category_id)
      update_setting_detail(landing_id: landing_id, action_id: action_id)

      {
        landing_id:  landing_id,
        landing_url: fetch_landing_url(landing_id, category_id: category_id),
        action_id:   action_id,
        tag_id:      tag_id
      }
    end

    # ランディングを作成して { landing_id:, landing_url:, raw_body: } を返す。
    # landing_url はレスポンスに含まれていれば同時に返す（無ければ nil）。
    def create_landing(name:, category_id:, action_with_friend: 1)
      form = {
        'newQrs[name]'               => name.to_s,
        'newQrs[category_id]'        => category_id.to_s,
        'newQrs[action_with_friend]' => action_with_friend.to_s
      }
      # UI と同じく x-server: data を付与（無いと 500 になる）
      body = post_urlencoded(PATH_CREATE_LANDING, form, referer: landing_list_url, extra_headers: { 'x-server' => 'data' })

      json       = JSON.parse(body.to_s) rescue nil
      landing_id = find_first(json, 'id')
      action_id  = find_first(json, 'action_id')
      if landing_id
        Rails.logger.info("[LmeLanding] create-landing-v2 ok name=#{name} landing_id=#{landing_id} action_id=#{action_id}")
      else
        Rails.logger.warn("[LmeLanding] create-landing-v2 id not found name=#{name} resp=#{body.to_s[0, 500]}")
      end
      {
        landing_id:  landing_id,
        action_id:   action_id,
        landing_url: body.to_s[LANDING_QR_URL_PATTERN],
        raw_body:    body
      }
    end

    # 作成した landing の action_id を編集初期データから取得
    def fetch_action_id(landing_id)
      body = post_urlencoded(PATH_INIT_DETAIL, { 'id' => landing_id, 'landing_id' => landing_id }, referer: landing_edit_url(landing_id))
      json = JSON.parse(body.to_s) rescue nil
      find_first(json, 'action_id')
    rescue => e
      Rails.logger.warn("[LmeLanding] fetch_action_id 失敗 landing_id=#{landing_id}: #{e.message}")
      nil
    end

    # タグフォルダ内に同名タグがあれば id を返す（無ければ nil）
    def find_tag_id(tag_folder_id:, tag_name:)
      body = post_urlencoded(PATH_LIST_GROUP_TAG, { 'group_id' => tag_folder_id, 'action' => 'showGroup' }, referer: landing_list_url)
      json = JSON.parse(body.to_s) rescue nil
      return nil unless json

      items = json['tag_items'] || json['items'] || []
      wanted = normalize_name(tag_name)
      hit = Array(items).find { |item| item.is_a?(Hash) && normalize_name(item['name']) == wanted }
      hit && hit['id']
    rescue => e
      Rails.logger.warn("[LmeLanding] find_tag_id 失敗: #{e.message}")
      nil
    end

    # タグを新規作成し id を返す（既存名なら nil→呼び出し側は find_tag_id で拾う）
    def create_tag(tag_folder_id:, tag_name:)
      body = post_urlencoded(PATH_CREATE_TAG, { 'folder_id' => tag_folder_id, 'tag_name' => tag_name }, referer: landing_list_url)
      json = JSON.parse(body.to_s) rescue nil
      tag_id = find_first(json, 'id')
      if tag_id.blank?
        Rails.logger.info("[LmeLanding] create_tag 既存/失敗 name=#{tag_name} resp=#{body.to_s[0, 200]} → 既存検索")
        tag_id = find_tag_id(tag_folder_id: tag_folder_id, tag_name: tag_name)
      end
      tag_id
    end

    # action/save でランディングのアクションに「友だち追加時にタグを付与」を紐付ける。
    # 保存された action_id を返す。
    def attach_tag_action(action_id:, tag_id:, tag_folder_id:, tag_name:)
      tag_object = {
        'id' => tag_id, 'name' => tag_name, 'category_id' => tag_folder_id.to_i, 'setting_actions' => []
      }
      action_detail = [{
        'title'           => 'タグ',
        'type'            => 'tag',
        'active'          => true,
        'is_edit_content' => true,
        'change_filter'   => 1,
        'data'            => { 'ids' => [tag_id], 'action' => 1, 'is_select_all' => false, 'filters' => { 'and' => [], 'or' => [] } },
        'list_tags'       => [tag_object],
        'group_open_tag'  => tag_folder_id.to_s,
        'tag_items'       => [tag_object],
        'items_default_tag' => []
      }]
      body = post_urlencoded(
        PATH_ACTION_SAVE,
        { 'action_detail' => action_detail.to_json, 'type' => 'qrcode', 'id' => action_id.to_s },
        referer: landing_list_url
      )
      json = JSON.parse(body.to_s) rescue nil
      find_first(json, 'action_id')
    rescue => e
      Rails.logger.warn("[LmeLanding] attach_tag_action 失敗: #{e.message}")
      action_id
    end

    # ランディングの名前・フォルダを確定（PUT /ajax/v2/landing/update-basic/{id}）
    def update_basic(landing_id:, action_id:, name:, category_id:)
      form = {
        'action_id'                   => action_id.to_s,
        'user_introduction_action_id' => '',
        'action_limit_id'             => '',
        'name'                        => name.to_s,
        'category_id'                 => category_id.to_s
      }
      path = format(PATH_UPDATE_BASIC, landing_id: landing_id)
      post_urlencoded(path, form, referer: landing_edit_url(landing_id), http_method: :put)
    end

    # 作成直後の「アクション設定」保存（UI の設定ダイアログの保存相当）
    def update_setting_detail(landing_id:, action_id: '')
      form = {
        'action_id'            => action_id.to_s,
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

    # 一覧APIで landing_id 一致のレコードを引き、公開QR URL（s.lmes.jp）を返す。
    # ★ 編集画面HTMLにはプレースホルダしか無いため、一覧APIの link_qr_code / code を使う。
    def fetch_landing_url(landing_id, category_id: nil)
      record = fetch_landing_list(category_id: category_id).find { |row| row['id'].to_s == landing_id.to_s }
      return nil unless record

      link = record['link_qr_code'].to_s
      # 公開ドメイン s.lmes.jp に寄せる（link_qr_code は step.lme.jp で返る）
      url = link.sub(%r{\Ahttps?://[^/]+}, "https://#{PUBLIC_QR_HOST}")
      return url if url =~ %r{\Ahttps://#{Regexp.escape(PUBLIC_QR_HOST)}/landing-qr/}

      code = record['code'].to_s
      code.present? ? "https://#{PUBLIC_QR_HOST}/landing-qr/#{LANDING_QR_BASE}?uLand=#{code}" : nil
    end

    # 指定した名前候補のいずれかと完全一致するランディングが既に存在するか。
    # 取得失敗時は false（＝作成に進む。二重作成は管理シートで抑止済み）。
    def landing_exists?(name_candidates, category_id: nil)
      wanted = Array(name_candidates).map { |name| normalize_name(name) }.reject(&:blank?)
      return false if wanted.empty?

      existing_names(category_id: category_id).any? { |name| wanted.include?(normalize_name(name)) }
    end

    # 既存ランディングの name 一覧を取得（取得不能なら空配列）。
    def existing_names(category_id: nil)
      fetch_landing_list(category_id: category_id).map { |row| row['name'] }.compact
    end

    # ランディング一覧を取得（取得不能なら空配列）。UI の一覧描画と同じ GET /ajax/v2/landing。
    # ★ orders パラメータを付けると 500（Undefined index: dir）になるため付けない。
    # 返却レコード: { id, name, code(uLand), link_qr_code(フルURL), category_id, ... }
    def fetch_landing_list(category_id: nil)
      params = {
        'type'               => '',
        'limit'              => 500,
        'page'               => 1,
        'action_with_friend' => 0,
        'keyword'            => ''
      }
      params['category_id'] = category_id.to_s if category_id.present?

      body = @ctx.http.get_json(path: PATH_LANDING_LIST_AJAX, referer: landing_list_url, params: params)
      json = JSON.parse(to_utf8(body)) rescue nil
      return [] unless json

      rows = json.dig('data', 'data')
      rows = json['data'] if rows.nil? && json['data'].is_a?(Array)
      Array(rows).select { |row| row.is_a?(Hash) }
    rescue => e
      Rails.logger.warn("[LmeLanding] fetch_landing_list 取得失敗: #{e.class} #{e.message}")
      []
    end

    private

    def post_urlencoded(path, form, referer:, extra_headers: {}, http_method: :post)
      csrf = ensure_csrf!(referer)
      body = @ctx.http.with_loa_retry do
        @ctx.http.post_form(
          path:          path,
          form:          form,
          cookie:        cookie_header,
          csrf_meta:     csrf,
          xsrf_cookie:   nil, # /basic 配下は XSRF cookie 不要（BroadcastService と同様）
          referer:       referer,
          extra_headers: extra_headers,
          http_method:   http_method
        )
      end
      # LME レスポンスは ASCII-8BIT で返るため、UTF-8 の日本語(name等)と
      # 文字列連結するとエンコーディング例外になる。UTF-8 に正規化する。
      to_utf8(body)
    end

    # ASCII-8BIT のレスポンスを UTF-8 に正規化（不正バイトは除去）
    def to_utf8(str)
      str.to_s.dup.force_encoding('UTF-8').scrub('')
    end

    def ensure_csrf!(referer)
      html, _ = @ctx.http.get_with_cookies(cookie_header, referer)
      meta = (to_utf8(html)[/<meta\s+name=["']csrf-token["']\s+content=["']([^"']+)["']/, 1] || '').to_s.strip rescue ''
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

    # JSON の入れ子から最初に見つかった key の値を返す（数値/文字列どちらも）。
    # 返却形が {"id":..} / {"data":{"id":..}} / {"landing":{"id":..}} 等どれでも拾える。
    def find_first(node, key)
      case node
      when Hash
        node.each do |node_key, value|
          return value if node_key.to_s == key && !value.is_a?(Enumerable)

          found = find_first(value, key)
          return found unless found.nil?
        end
        nil
      when Array
        node.each do |child|
          found = find_first(child, key)
          return found unless found.nil?
        end
        nil
      end
    end

    # 全角/半角スペースを吸収して名前比較する
    def normalize_name(name)
      name.to_s.gsub(/[[:space:]　]+/, '').strip
    end

    def landing_list_url
      "#{@ctx.origin}#{PATH_LANDING_LIST}"
    end

    def landing_edit_url(landing_id)
      "#{@ctx.origin}#{format(PATH_LANDING_EDIT, landing_id: landing_id)}"
    end
  end
end
