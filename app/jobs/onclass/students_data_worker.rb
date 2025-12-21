# frozen_string_literal: true

require 'csv'
require 'date'
require 'json'
require 'fileutils'
require 'faraday'

module Onclass
  class StudentsDataWorker
    include Sidekiq::Worker

    sidekiq_options(
      queue: 'onclass_students_data',
      retry: 3,
      lock: :until_executed,
      on_conflict: :reject,
      lock_args_method: :lock_args,
      lock_ttl: 30.minutes
    )

    require 'google/apis/sheets_v4'
    require 'googleauth'

    DEFAULT_LEARNING_COURSE_ID = 'oYTO4UDI6MGb' # フロント

    # 表示順。最後に :unknown を補助的に扱う
    STATUS_ORDER = [
      ['very_good', '素晴らしい'],
      ['good',      '順調'],
      ['very_bad',  '離脱'],
      ['bad',       '停滞中'],
      ['normal',    '停滞気味']
    ].freeze

    API_TO_JP_MOTIVATION = {
      'very_good' => '素晴らしい',
      'good'      => '順調',
      'very_bad'  => '離脱',
      'bad'       => '停滞中',
      'normal'    => '停滞気味',
      'unknown'   => '不明'
    }.freeze

    TARGET_COLUMNS = %w[name email last_sign_in_at course_name course_start_at course_progress].freeze

    # 新PDCA（条件: BASE と weekly_goals?user_id= を含むURLが free_text に存在する時だけ採用）
    PDCA_APP_BASE_URL   = 'https://pdca-app-475677fd481e.herokuapp.com'.freeze
    PDCA_WEEKLY_PREFIX  = 'weekly_goals?user_id='.freeze

    # ================ Entry =================
    def perform(course_id = nil, sheet_name = nil)
      course_id  ||= ENV.fetch('ONCLASS_COURSE_ID', DEFAULT_LEARNING_COURSE_ID)
      sheet_name ||= ENV.fetch('ONCLASS_SHEET_NAME', 'フロントコース受講生')

      # 1) 既定アカウントでサインイン（トークン更新）
      Onclass::SignInWorker.new.perform
      base_client  = Onclass::AuthClient.new
      base_headers = base_http_headers.merge(
        'access-token' => base_client.headers['access-token'],
        'client'       => base_client.headers['client'],
        'uid'          => base_client.headers['uid']
      ).compact

      # 2) Faraday 接続
      conn = Faraday.new(url: base_client.base_url) do |f|
        f.request  :json
        f.response :raise_error
        f.adapter  Faraday.default_adapter
      end

      # 3) 一覧（※ motivation フィルタ無し。normal が壊れているため全件→後で分類）
      all_rows = fetch_users_all_pages(conn, base_headers, course_id)
      Rails.logger.info("[Onclass::StudentsDataWorker] list fetched total=#{all_rows.size} (unfiltered)")

      # 4) まず最低限の行データを作る（最新ログインも拾う）
      rows = all_rows.map { |u| build_user_row(conn, base_headers, u, jp_label: nil) }
      student_ids = rows.map { |r| r['id'] }.compact.uniq

      # 5) 詳細/基本/期限を取得しつつ motivation を補完
      details_by_id = {}
      extension_by_id = {}
      basic_by_id = {}

      student_ids.each do |sid|
        details_by_id[sid]   = fetch_user_learning_course(conn, base_headers, sid, course_id) rescue {}
        extension_by_id[sid] = fetch_extension_study_date(conn, base_headers, sid, course_id) rescue nil
        sleep 0.05
      end

      student_ids.each do |sid|
        begin
          basic_by_id[sid] = fetch_user_basic(conn, base_headers, sid)
          sleep 0.03
        rescue => e
          Rails.logger.warn("[Onclass::StudentsDataWorker] basic fetch failed for #{sid}: #{e.class} #{e.message}")
        end
      end

      # 6) 未読メンション（アカウント別ログイン）
      accounts      = JSON.parse(ENV['ONLINE_CLASS_CREDENTIALS'] || '[]') rescue []
      nishino_cred  = accounts[0] || {}
      kato_cred     = accounts[1] || {}

      nishino_headers =
        (nishino_cred['email'] && nishino_cred['password']) ?
          Onclass::SignInWorker.sign_in_headers_for(email: nishino_cred['email'], password: nishino_cred['password']) : nil
      kato_headers =
        (kato_cred['email'] && kato_cred['password']) ?
          Onclass::SignInWorker.sign_in_headers_for(email: kato_cred['email'], password: kato_cred['password']) : nil

      nishino_maps = nishino_headers ? fetch_unread_mentions_map(conn, nishino_headers) : { id: {}, name: {} }
      kato_maps    = kato_headers    ? fetch_unread_mentions_map(conn, kato_headers)    : { id: {}, name: {} }

      # 7) 付加情報 & motivation 補完
      rows.each do |r|
        d = details_by_id[r['id']] || {}
        r['current_category']               = current_category_name(d) || ''
        r['current_block']                  = current_block_name(d) || ''
        r['course_join_date']               = d['course_join_date']
        r['course_login_rate']              = d['course_login_rate']
        r['current_category_scheduled_at']  = scheduled_completed_at_for_current(d)
        r['current_category_started_at']    = (current_category_object(d) || {})['started_at']
        r['categories_schedule']            = categories_schedule_map(d)
        r['extension_study_date']           = extension_by_id[r['id']]

        # motivation が空なら詳細から補完（候補キーを広く探す）
        if r['motivation'].to_s.strip.empty?
          md = normalize_motivation(
            d['motivation'] ||
            d.dig('learning_course', 'motivation') ||
            d.dig('course', 'motivation')
          )
          if API_TO_JP_MOTIVATION.key?(md)
            r['motivation'] = md
            r['status']     = API_TO_JP_MOTIVATION[md]
          end
        end

        b = basic_by_id[r['id']] || {}
        free = b['free_text']

        # 旧PDCA（スプシURL）
        r['pdca_url'] = extract_gsheets_url(free)

        # ✅ 新PDCA（free_text に BASE + weekly_goals?user_id= を含むURLがある時だけ）
        r['new_pdca_url'] = extract_new_pdca_url(free)

        r['line_url'] = extract_line_url(free)

        key_id   = r['id']
        key_name = normalize_name(r['name'])
        r['nishino_mentions_count'] =
          (nishino_maps[:id][key_id]&.values&.sum || 0) +
          (nishino_maps[:name][key_name]&.values&.sum || 0)
        r['kato_mentions_count'] =
          (kato_maps[:id][key_id]&.values&.sum || 0) +
          (kato_maps[:name][key_name]&.values&.sum || 0)
      end

      # ソート（受講日降順）
      rows.sort_by! { |r| (Date.parse(r['course_join_date'].to_s) rescue Date.new(1900,1,1)) }
      rows.reverse!

      # 8) クライアント側でグルーピング（normal は API を叩かない）
      grouped_rows = group_by_motivation(rows)

      STATUS_ORDER.each do |motivation, jp|
        Rails.logger.info("[Onclass::StudentsDataWorker] grouped #{motivation}(#{jp}) => #{grouped_rows[motivation]&.size.to_i}")
      end
      Rails.logger.info("[Onclass::StudentsDataWorker] grouped unknown => #{grouped_rows['unknown']&.size.to_i}")

      timestamp = Time.zone.now.strftime('%Y%m%d_%H%M%S')
      dir       = Rails.root.join('tmp')
      FileUtils.mkdir_p(dir)
      course_tag = course_id

      status_files  = {}
      created_paths = []

      (STATUS_ORDER.map(&:first) + ['unknown']).each do |motivation|
        rows_for_m = grouped_rows[motivation] || []
        fname = dir.join("onclass_#{course_tag}_#{timestamp}_#{motivation}.csv")
        write_csv(fname, rows_for_m)
        status_files[motivation] = fname.to_s
        created_paths << fname.to_s
      end

      combined_csv_path  = dir.join("onclass_#{course_tag}_#{timestamp}_combined.csv")
      write_csv(combined_csv_path, rows)
      created_paths << combined_csv_path.to_s

      combined_xlsx_path = maybe_write_xlsx(dir, course_tag, timestamp, rows)
      created_paths << combined_xlsx_path if combined_xlsx_path.present?

      export_api_csv_path = export_official_csv(conn, base_headers, rows, dir, course_tag, timestamp)
      created_paths << export_api_csv_path if export_api_csv_path.present?

      result = {
        combined_csv: combined_csv_path.to_s,
        combined_xlsx: combined_xlsx_path,
        status_csvs: status_files,
        official_export_csv: export_api_csv_path
      }

      upload_to_gsheets!(
        rows: rows,
        spreadsheet_id: ENV.fetch('ONCLASS_SPREADSHEET_ID'),
        sheet_name:     sheet_name,
        nishino_maps:   nishino_maps,
        kato_maps:      kato_maps
      )

      # tmp削除（デフォルトON）
      if ENV.fetch('ONCLASS_CLEAN_TMP', '1') == '1'
        cleanup_tmp_files!(created_paths)
        Rails.logger.info("[Onclass::StudentsDataWorker] cleaned up #{created_paths.compact.size} tmp files.")
      end

      Rails.logger.info("[Onclass::StudentsDataWorker] done: #{result.inspect}")
      result

    rescue Faraday::Error => e
      Rails.logger.error("[Onclass::StudentsDataWorker] HTTP error: #{e.class} #{e.message}")
      raise
    rescue => e
      Rails.logger.error("[Onclass::StudentsDataWorker] unexpected error: #{e.class} #{e.message}")
      raise
    end

    # ================ Private =================
    private

    def normalize_motivation(v)
      (v || '').to_s.strip.downcase
    end

    def base_http_headers
      {
        'accept'       => 'application/json, text/plain, */*',
        'content-type' => 'application/json',
        'origin'       => 'https://manager.the-online-class.com',
        'referer'      => 'https://manager.the-online-class.com/',
        'user-agent'   => 'Mozilla/5.0'
      }
    end

    # Faraday GET（簡易バックオフ&ボディ付きログ）
    def safe_get(conn, path, params, headers, tries: 3)
      attempt = 0
      begin
        attempt += 1
        return conn.get(path, params, headers)
      rescue Faraday::Error => e
        body = e.respond_to?(:response) ? e.response&.dig(:body) : nil
        Rails.logger.warn("[OnclassStudentsDataWorker] GET #{path} #{params.inspect} failed (#{e.class} #{e.message}) body=#{body.inspect} attempt=#{attempt}/#{tries}")
        raise if attempt >= tries
        sleep(0.5 * attempt)
        retry
      end
    end

    # ---------- 一覧 ----------
    def build_user_row(conn, headers, u, jp_label: nil)
      # ここでは motivation を可能なら拾い、後段で詳細からも補完
      m = normalize_motivation(u['motivation'] || u.dig('learning_course', 'motivation') || u['status'])
      {
        'id'              => u['id'] || u['user_id'] || u['uid'],
        'name'            => u['name'],
        'email'           => u['email'],
        'last_sign_in_at' => u['last_sign_in_at'],
        'course_name'     => u['course_name'] || u['learning_course_name'] || u.dig('learning_course', 'name'),
        'course_start_at' => u['course_start_at'] || u.dig('learning_course', 'start_at'),
        'course_progress' => u['course_progress'],
        'latest_login_at' => fetch_latest_login_at(conn, headers, u['id']),
        'motivation'      => m,
        'status'          => jp_label || API_TO_JP_MOTIVATION[m] || ''
      }
    end

    # motivationフィルタなし全件ページング
    def fetch_users_all_pages(conn, headers, learning_course_id)
      page = 1
      rows = []
      loop do
        params = {
          page: page,
          learning_course_id: learning_course_id,
          name_or_email: '',
          admission_day_from: '',
          admission_day_to: '',
          free_text: '',
          category_id: '',
          category_status: 'all',
          account_group_id: ''
        }
        resp = safe_get(conn, '/v1/enterprise_manager/users', params, headers)
        json = JSON.parse(resp.body) rescue {}
        list = json.is_a?(Array) ? json : (json['users'] || json['data'] || json['records'] || [])

        rows.concat(list)

        total_pages  = (json['total_pages'] || json.dig('meta', 'total_pages')).to_i
        current_page = (json['current_page'] || json.dig('meta', 'current_page') || page).to_i
        next_link    = json['next_page'] || json.dig('links', 'next')
        break if list.empty? || (total_pages > 0 && current_page >= total_pages) || (!next_link.nil? && next_link == false)
        page += 1
      end
      rows
    end

    def write_csv(path, rows)
      headers = %w[id name email last_sign_in_at course_name course_start_at course_progress status]
      CSV.open(path, 'w', encoding: 'UTF-8') do |csv|
        csv << headers
        rows.each { |r| csv << headers.map { |h| r[h] } }
      end
    end

    def maybe_write_xlsx(dir, course_tag, timestamp, rows)
      begin
        require 'axlsx'
        path = dir.join("onclass_#{course_tag}_#{timestamp}_combined.xlsx")
        p = Axlsx::Package.new
        wb = p.workbook
        headers = %w[id name email last_sign_in_at course_name course_start_at course_progress status]
        wb.add_worksheet(name: 'Course') do |sheet|
          sheet.add_row headers
          rows.each { |r| sheet.add_row headers.map { |h| r[h] } }
        end
        p.serialize(path.to_s)
        path.to_s
      rescue LoadError
        Rails.logger.info('[OnclassStudentsDataWorker] axlsx 未導入のため xlsx 出力はスキップします')
        nil
      end
    end

    def export_official_csv(conn, headers, rows, dir, course_tag, timestamp)
      user_ids = rows.map { |r| r['id'] }.compact.uniq
      return nil if user_ids.empty?

      body = { target_columns: TARGET_COLUMNS, target_user_ids: user_ids }
      resp = conn.post('/v1/enterprise_manager/users/export_csv', body.to_json, headers)
      content_type = resp.headers['content-type'].to_s

      path = dir.join("onclass_#{course_tag}_#{timestamp}_official_export.csv")

      if content_type.include?('text/csv') || content_type.include?('application/octet-stream')
        File.binwrite(path, resp.body)
        return path.to_s
      end

      json = JSON.parse(resp.body) rescue {}
      if (csv_str = json['csv'])
        File.write(path, csv_str)
        return path.to_s
      elsif (file_url = json['file_url'])
        bin = Faraday.get(file_url).body rescue nil
        if bin
          File.binwrite(path, bin)
          return path.to_s
        end
      end

      Rails.logger.warn('[OnclassStudentsDataWorker] export_csv のレスポンス形式が想定外でした')
      nil
    end

    # ---------- Sheets ----------
    def build_sheets_service
      service = Google::Apis::SheetsV4::SheetsService.new
      service.client_options.application_name = 'Onclass Course Uploader'
      scope   = [Google::Apis::SheetsV4::AUTH_SPREADSHEETS]
      keyfile = ENV['GOOGLE_APPLICATION_CREDENTIALS']

      raise "ENV GOOGLE_APPLICATION_CREDENTIALS is not set." if keyfile.nil? || keyfile.strip.empty?
      raise "Service account key not found: #{keyfile}" unless File.exist?(keyfile)

      json = JSON.parse(File.read(keyfile)) rescue nil
      unless json && json['type'] == 'service_account' && json['private_key'] && json['client_email']
        raise "Invalid service account JSON: missing private_key/client_email/type=service_account"
      end

      authorizer = Google::Auth::ServiceAccountCredentials.make_creds(
        json_key_io: File.open(keyfile),
        scope: scope
      )
      authorizer.fetch_access_token!
      service.authorization = authorizer
      service
    end

    def ensure_sheet_exists!(service, spreadsheet_id, sheet_name)
      ss = service.get_spreadsheet(spreadsheet_id)
      exists = ss.sheets.any? { |s| s.properties&.title == sheet_name }
      return if exists

      add_req = Google::Apis::SheetsV4::AddSheetRequest.new(
        properties: Google::Apis::SheetsV4::SheetProperties.new(title: sheet_name)
      )
      batch = Google::Apis::SheetsV4::BatchUpdateSpreadsheetRequest.new(
        requests: [Google::Apis::SheetsV4::Request.new(add_sheet: add_req)]
      )
      service.batch_update_spreadsheet(spreadsheet_id, batch)
    end

    def upload_to_gsheets!(rows:, spreadsheet_id:, sheet_name:, nishino_maps:, kato_maps:)
      service = build_sheets_service
      ensure_sheet_exists!(service, spreadsheet_id, sheet_name)
      clear_req = Google::Apis::SheetsV4::ClearValuesRequest.new

      # 列構成
      # B:名前 C:Line D:メール E:ステータス F:ステータス_B G:カテゴリ/予定 H:ブロック I:受講日 J:期限 K:ログイン率 L:最新ログイン日
      # M:旧PDCA N:新PDCA O:PDCA更新日時(不触) P:西野メンション Q:加藤メンション
      # 右マトリクスは R 列以降

      # クリア（PDCA更新日時＝O列は触らない）
      service.clear_values(spreadsheet_id, "#{sheet_name}!B2:Q2", clear_req)
      service.clear_values(spreadsheet_id, "#{sheet_name}!B3:Q3", clear_req)
      service.clear_values(spreadsheet_id, "#{sheet_name}!B4:N",  clear_req) # 左ブロック（旧PDCA+新PDCAまで）
      service.clear_values(spreadsheet_id, "#{sheet_name}!P4:Q",  clear_req) # メンション（右ブロック）
      service.clear_values(spreadsheet_id, "#{sheet_name}!R2:ZZ", clear_req) # マトリクス

      # B2（メタ） ※B〜Qは16列なので、空埋めは 14
      meta_row   = ['バッチ実行タイミング', jp_timestamp] + Array.new(14, '')
      meta_range = "#{sheet_name}!B2:Q2"
      service.update_spreadsheet_value(
        spreadsheet_id,
        meta_range,
        Google::Apis::SheetsV4::ValueRange.new(range: meta_range, values: [meta_row]),
        value_input_option: 'USER_ENTERED'
      )

      # 見出し（B3:Q3）
      headers = %w[
        名前 Line メールアドレス ステータス ステータス_B
        現在進行カテゴリ/完了予定日 現在進行ブロック 受講日 受講期限日 ログイン率 最新ログイン日
        旧PDCA 新PDCA
        PDCA更新日時 西野メンション 加藤メンション
      ]
      header_range = "#{sheet_name}!B3:Q3"
      service.update_spreadsheet_value(
        spreadsheet_id,
        header_range,
        Google::Apis::SheetsV4::ValueRange.new(range: header_range, values: [headers]),
        value_input_option: 'USER_ENTERED'
      )

      # データ本体（B4〜）
      sanitized_rows = rows.reject do |r|
        id    = r['id'].to_s.strip
        name  = r['name'].to_s.strip
        email = r['email'].to_s.strip
        id.casecmp('id').zero? || name == '名前' || email == 'メールアドレス'
      end

      # 左ブロック（B〜N）：O（PDCA更新日時）は不触
      left_block_values = sanitized_rows.map do |r|
        g_val_name     = r['current_category'].to_s
        g_val_date     = to_jp_ymd(r['current_category_scheduled_at'])
        g_val_combined = g_val_name
        g_val_combined = "#{g_val_name} / #{g_val_date}" unless g_val_name.empty? || g_val_date.empty?

        old_pdca_url = r['pdca_url']      # 旧PDCA（スプシURL）
        new_pdca_url = r['new_pdca_url']  # ✅ 新PDCA（free_textから抽出できた時だけ）

        [
          hyperlink_name(r['id'], r['name']),
          hyperlink_line(r['line_url'], r['name']),
          r['email'],
          r['status'].to_s,
          status_b_for(r),
          g_val_combined,
          r['current_block']                   || '',
          to_jp_ymd(r['course_join_date'])     || '',
          to_jp_ymd(r['extension_study_date']) || '',
          (r['course_login_rate'].nil? ? '' : r['course_login_rate'].to_s),
          to_jp_ymdhm(r['latest_login_at'])    || '',
          hyperlink_pdca(old_pdca_url, r['name']), # 旧PDCA
          hyperlink_pdca(new_pdca_url, r['name'])  # 新PDCA
        ]
      end

      if left_block_values.any?
        service.update_spreadsheet_value(
          spreadsheet_id,
          "#{sheet_name}!B4",
          Google::Apis::SheetsV4::ValueRange.new(range: "#{sheet_name}!B4", values: left_block_values),
          value_input_option: 'USER_ENTERED'
        )
      end

      # メンション（P〜Q）※ OはPDCA更新日時（不触）
      right_block_values = sanitized_rows.map do |r|
        [
          mention_cell(channel_counts_for(nishino_maps, r)),
          mention_cell(channel_counts_for(kato_maps, r))
        ]
      end

      if right_block_values.any?
        service.update_spreadsheet_value(
          spreadsheet_id,
          "#{sheet_name}!P4",
          Google::Apis::SheetsV4::ValueRange.new(range: "#{sheet_name}!P4", values: right_block_values),
          value_input_option: 'USER_ENTERED'
        )
      end

      # 「カリキュラム完了予定日」マトリクス（R列〜に右シフト）
      service.update_spreadsheet_value(
        spreadsheet_id,
        "#{sheet_name}!R2",
        Google::Apis::SheetsV4::ValueRange.new(range: "#{sheet_name}!R2", values: [['カリキュラム完了予定日']]),
        value_input_option: 'USER_ENTERED'
      )

      category_order = []
      seen = {}
      rows.each do |r|
        (r['categories_schedule'] || {}).each_key do |name|
          next if name.to_s.empty? || seen[name]
          seen[name] = true
          category_order << name
        end
      end

      if category_order.any?
        service.update_spreadsheet_value(
          spreadsheet_id,
          "#{sheet_name}!R3",
          Google::Apis::SheetsV4::ValueRange.new(range: "#{sheet_name}!R3", values: [category_order]),
          value_input_option: 'USER_ENTERED'
        )

        schedule_matrix = sanitized_rows.map do |r|
          sched_map = r['categories_schedule'] || {}
          category_order.map { |name| to_jp_ymd(sched_map[name]) }
        end

        if schedule_matrix.any?
          service.update_spreadsheet_value(
            spreadsheet_id,
            "#{sheet_name}!R4",
            Google::Apis::SheetsV4::ValueRange.new(range: "#{sheet_name}!R4", values: schedule_matrix),
            value_input_option: 'USER_ENTERED'
          )
        end
      end

      Rails.logger.info("[Onclass::StudentsDataWorker] uploaded #{sanitized_rows.size} rows with old/new pdca, mentions and schedules (B列〜N列 + P/Q + R列〜).")
    end

    # ---------- 表示ヘルパ ----------
    def jp_timestamp
      Time.now.in_time_zone('Asia/Tokyo').strftime('%Y年%-m月%-d日 %H時 %M分')
    end

    def to_jp_ymd(str)
      return '' if str.blank?
      t = (Time.zone.parse(str.to_s) rescue Time.parse(str.to_s) rescue nil)
      return '' unless t
      t.in_time_zone('Asia/Tokyo').strftime('%Y年%-m月%-d日')
    end

    def to_jp_ymdhm(str)
      return '' if str.blank?
      t = (Time.zone.parse(str) rescue Time.parse(str) rescue nil)
      return '' unless t
      t.in_time_zone('Asia/Tokyo').strftime('%Y年%-m月%-d日 %H時%M分')
    end

    def hyperlink_name(id, name)
      return name.to_s if id.to_s.strip.empty?
      label = name.to_s.gsub('"', '""')
      url   = "https://manager.the-online-class.com/accounts/#{id}"
      %Q(=HYPERLINK("#{url}","#{label}"))
    end

    # 旧PDCA/新PDCA 両方とも表示文字は「pdca_名前」
    def hyperlink_pdca(url, name)
      return '' if url.to_s.strip.empty?
      label = "pdca_#{name}".to_s.gsub('"', '""')
      %Q(=HYPERLINK("#{url}","#{label}"))
    end

    def hyperlink_line(url, name)
      return '' if url.to_s.strip.empty?
      label = "#{name}_Line".gsub('"', '""')
      %Q(=HYPERLINK("#{url}","#{label}"))
    end

    # ---------- 詳細/基本/期限 ----------
    def fetch_user_learning_course(conn, headers, student_id, learning_course_id)
      params = { learning_course_id: learning_course_id }
      resp   = safe_get(conn, "/v1/enterprise_manager/users/#{student_id}/learning_course", params, headers)
      json   = JSON.parse(resp.body) rescue {}
      json['data'] || json
    end

    def fetch_latest_login_at(conn, headers, user_id)
      resp = safe_get(conn, "/v1/enterprise_manager/users/#{user_id}/logins", { page: 1 }, headers)
      json = JSON.parse(resp.body) rescue {}
      list = json['data'] || json['logins'] || json['records'] || []
      first = list.is_a?(Array) ? list.first : nil
      first && first['created_at']
    rescue Faraday::Error => e
      Rails.logger.warn("[Onclass::StudentsDataWorker] fetch_latest_login_at error for #{user_id}: #{e.class} #{e.message}")
      nil
    end

    def fetch_user_basic(conn, headers, student_id)
      resp = safe_get(conn, "/v1/enterprise_manager/users/#{student_id}", {}, headers)
      json = JSON.parse(resp.body) rescue {}
      json['data'] || json
    rescue Faraday::Error => e
      Rails.logger.warn("[Onclass::StudentsDataWorker] fetch_user_basic error for #{student_id}: #{e.class} #{e.message}")
      {}
    end

    def fetch_extension_study_date(conn, headers, user_id, learning_course_id)
      resp = safe_get(conn,
        "/v1/enterprise_manager/enterprise_managers/current/learning_courses_for_user",
        { user_id: user_id },
        headers
      )
      json  = JSON.parse(resp.body) rescue {}
      list  = json['data'] || json['learning_courses'] || []
      item  = Array(list).find { |c| (c['id'] || c[:id]).to_s == learning_course_id.to_s }
      item && (item['extension_study_date'] || item[:extension_study_date])
    rescue Faraday::Error => e
      Rails.logger.warn("[Onclass::StudentsDataWorker] fetch_extension_study_date error for #{user_id}: #{e.class} #{e.message}")
      nil
    end

    # ---------- メンション ----------
    def fetch_unread_mentions_map(conn, token_headers)
      headers = base_http_headers.merge(token_headers.slice('uid','access-token','client','token-type','expiry').compact)
      resp = safe_get(conn, '/v1/enterprise_manager/communities/activity/mentions', {}, headers)
      json = JSON.parse(resp.body) rescue {}
      list = Array(json['data'] || json['records'] || json)

      by_id   = Hash.new { |h,k| h[k] = Hash.new(0) }
      by_name = Hash.new { |h,k| h[k] = Hash.new(0) }

      list.each do |m|
        next unless m.is_a?(Hash) && m['is_read'] == false
        channel_name = m.dig('chat','channel','name') || m.dig('channel','name') || '不明チャンネル'
        sender_id    = m['sender_id'] || m.dig('chat','sender_id')
        by_id[sender_id][channel_name] += 1 if sender_id.present?
        sender_name = m['user_name'] || m.dig('chat','user_name')
        if sender_name.present?
          by_name[normalize_name(sender_name)][channel_name] += 1
        end
      end

      uid = token_headers && token_headers['uid']
      Rails.logger.info("[OnclassStudentsDataWorker] mentions uid=#{uid} unread_total=#{by_id.values.sum { |h| h.values.sum } + by_name.values.sum { |h| h.values.sum }}")
      { id: by_id, name: by_name }
    rescue Faraday::Error => e
      uid = token_headers && token_headers['uid']
      Rails.logger.warn("[OnclassStudentsDataWorker] fetch_unread_mentions_map(uid=#{uid}) error: #{e.class} #{e.message}")
      { id: {}, name: {} }
    end

    def mention_cell(channel_counts)
      counts = channel_counts || {}
      return '' if counts.empty?
      parts = counts.map { |ch, c| "#{ch}より#{c}件" }
      label = parts.join(' / ')
      %Q(=HYPERLINK("https://manager.the-online-class.com/community","#{label.gsub('"','""')}"))
    end

    # ---------- ステータスB ----------
    def status_b_for(row)
      return '声かけ必須' if stale_login?(row['latest_login_at']) &&
                              row['status'].to_s == '離脱' &&
                              slow_category_progress?(row)
      ''
    end

    def stale_login?(latest_login_at_str)
      t = parse_time_jst(latest_login_at_str)
      return true if t.nil?
      (now_jst - t) >= 4.days
    end

    def slow_category_progress?(row)
      cat = row['current_category'].to_s
      return false if cat.empty? || cat == '人工インターン' || cat == '全て完了'

      started_at = parse_time_jst(row['current_category_started_at'])
      return (now_jst - started_at) >= 12.days if started_at

      sched = parse_time_jst(row['current_category_scheduled_at'])
      return false unless sched
      (now_jst - sched) >= 12.days
    end

    def parse_time_jst(str)
      return nil if str.to_s.strip.empty?
      Time.zone ? (Time.zone.parse(str) rescue nil) : (Time.parse(str) rescue nil)
    end

    def now_jst
      Time.zone ? Time.zone.now : Time.now
    end

    # ---------- コース進行ヘルパ ----------
    def scheduled_completed_at_for_current(detail)
      cur = current_category_object(detail)
      cur && cur['scheduled_completed_at']
    end

    def categories_schedule_map(detail)
      map = {}
      Array(detail&.dig('course_categories')).each do |cat|
        name = cat['name'].to_s
        next if name.empty?
        map[name] = cat['scheduled_completed_at']
      end
      map
    end

    def current_category_object(detail)
      cats = Array(detail&.dig('course_categories'))
      return nil if cats.empty?

      bool = ->(v) { v == true }
      last_true_idx = cats.rindex { |c| bool.call(c['is_completed']) }

      if last_true_idx.nil?
        cats.find { |c| !bool.call(c['is_completed']) } || cats.first
      else
        cats[last_true_idx + 1]
      end
    end

    def current_category_name(detail)
      cat = current_category_object(detail)
      cat ? cat['name'] : '全て完了'
    end

    def current_block_name(detail)
      cat = current_category_object(detail)
      return '' unless cat

      blocks = Array(cat['category_blocks'])
      return '' if blocks.empty?

      bool = ->(v) { v == true }
      blk  = blocks.find { |b| !bool.call(b['is_completed']) } || blocks.first
      blk['name'].to_s
    end

    # ---------- テキスト抽出 ----------
    def extract_gsheets_url(free_text)
      return nil if free_text.to_s.strip.empty?
      m = free_text.match(%r{(https?://)?(docs\.google\.com/spreadsheets/d/[^\s"'>]+)}i)
      return nil unless m
      url = m.to_s
      url = "https://#{url}" unless url.start_with?('http')
      url
    end

    # 新PDCA：free_text 内に BASE + weekly_goals?user_id= を含むURLが「ある時だけ」拾う
    def extract_new_pdca_url(free_text)
      return nil if free_text.to_s.strip.empty?

      text = free_text.to_s
      base   = Regexp.escape(PDCA_APP_BASE_URL)
      prefix = Regexp.escape(PDCA_WEEKLY_PREFIX)

      # 例: https://pdca-app-.../weekly_goals?user_id=16
      re = %r{(#{base}/?#{prefix}\d+)}i
      m = text.match(re)
      return nil unless m

      url = m[1].to_s.strip
      url = url.sub(%r{\A#{base}/?}i, "#{PDCA_APP_BASE_URL}/")
      url
    end

    def extract_line_url(free_text)
      return nil if free_text.to_s.strip.empty?
      m = free_text.match(%r{https?://step\.lme\.jp/basic/friendlist/my_page/\S+}i)
      return nil unless m
      m.to_s.strip
    end

    def normalize_name(str)
      base = str.to_s
      base = base.gsub(/（.*?）|\(.*?\)/, '') # 括弧内除去
      base = base.tr('　', ' ')              # 全角空白→半角
      base = base.gsub(/\s+/, '')            # 空白全削除
      base
    end

    # ---------- メンション集計 ----------
    def merge_channel_counts(a, b)
      total = Hash.new(0)
      Array(a).each { |k,v| total[k.to_s] += v.to_i }
      Array(b).each { |k,v| total[k.to_s] += v.to_i }
      total
    end

    def channel_counts_for(maps, row)
      return {} if maps.blank?
      id_map   = maps[:id]   || {}
      name_map = maps[:name] || {}
      by_id    = id_map[row['id']] || {}
      by_name  = name_map[normalize_name(row['name'])] || {}
      merge_channel_counts(by_id, by_name)
    end

    # ---------- グルーピング ----------
    def group_by_motivation(rows)
      hash = Hash.new { |h,k| h[k] = [] }
      rows.each do |r|
        m = normalize_motivation(r['motivation'])
        m = 'unknown' unless API_TO_JP_MOTIVATION.key?(m)
        r['status'] ||= API_TO_JP_MOTIVATION[m] || ''
        hash[m] << r
      end
      hash
    end

    # tmpのファイル削除
    def safe_unlink(path)
      return unless path.present?
      File.delete(path) if File.file?(path)
    rescue => e
      Rails.logger.warn("[Onclass::StudentsDataWorker] tmp cleanup failed for #{path}: #{e.class} #{e.message}")
    end

    def cleanup_tmp_files!(paths)
      Array(paths).compact.each { |p| safe_unlink(p) }
    end

    def self.lock_args(args)
      course_id, sheet_name = args
      [course_id.to_s, sheet_name.to_s]
    end
  end
end
