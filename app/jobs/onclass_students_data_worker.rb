# frozen_string_literal: true

require 'csv'
require 'date'
require 'json'
require 'fileutils'
require 'faraday'

class OnclassStudentsDataWorker
  include Sidekiq::Worker
  sidekiq_options queue: 'onclass_students_data', retry: 3

  require 'google/apis/sheets_v4'
  require 'googleauth'

  # デフォルト（未指定時に使うコースID）
  DEFAULT_LEARNING_COURSE_ID = 'oYTO4UDI6MGb' # 旧: フロント

  # 表示順（上から）
  STATUS_ORDER = [
    ['very_good', '素晴らしい'],
    ['good',      '順調'],
    ['very_bad',  '離脱'],
    ['bad',       '停滞中'],
    ['normal',    '停滞気味']
  ].freeze

  TARGET_COLUMNS = %w[name email last_sign_in_at course_name course_start_at course_progress].freeze

  # course_id / sheet_name を引数で切り替え可能に
  # 引数未指定時は ENV（ONCLASS_COURSE_ID / ONCLASS_SHEET_NAME）→ デフォルト の順で使用
  def perform(course_id = nil, sheet_name = nil)
    course_id  ||= ENV.fetch('ONCLASS_COURSE_ID', DEFAULT_LEARNING_COURSE_ID)
    sheet_name ||= ENV.fetch('ONCLASS_SHEET_NAME', 'フロントコース受講生')

    # 1) サインイン（トークン更新）
    OnclassSignInWorker.new.perform
    client  = OnclassAuthClient.new
    headers = client.headers # => { "access-token", "client", "uid", ... }

    # 2) Faraday 接続
    conn = Faraday.new(url: client.base_url) do |f|
      f.request  :json
      f.response :raise_error
      f.adapter  Faraday.default_adapter
    end

    default_headers = {
      'accept'       => 'application/json, text/plain, */*',
      'content-type' => 'application/json',
      'origin'       => 'https://manager.the-online-class.com',
      'referer'      => 'https://manager.the-online-class.com/',
      'user-agent'   => 'Mozilla/5.0',
      'access-token' => headers['access-token'],
      'client'       => headers['client'],
      'uid'          => headers['uid']
    }.compact

    # 3) モチベーション別ページング取得
    grouped_rows = {} # motivation => rows
    STATUS_ORDER.each do |motivation, jp_label|
      rows = fetch_users_by_motivation(conn, default_headers, motivation, jp_label, course_id)
      grouped_rows[motivation] = rows
      Rails.logger.info("[OnclassStudentsDataWorker] fetched #{rows.size} users for #{motivation}(#{jp_label}) course=#{course_id}")
    end

    # 4) 結合 & IDユニーク化
    combined_rows = STATUS_ORDER.flat_map { |motivation, _| grouped_rows[motivation] }
    student_ids   = combined_rows.map { |r| r['id'] }.compact.uniq

    # 5) 受講状況詳細 / 基本情報
    details_by_id = {}
    student_ids.each do |sid|
      details_by_id[sid] = fetch_user_learning_course(conn, default_headers, sid, course_id)
      sleep 0.05
    end

    basic_by_id = {}
    student_ids.each do |sid|
      begin
        basic_by_id[sid] = fetch_user_basic(conn, default_headers, sid)
        sleep 0.03
      rescue => e
        Rails.logger.warn("[OnclassStudentsDataWorker] basic fetch failed for #{sid}: #{e.class} #{e.message}")
      end
    end

    # 6) 付加情報をマージ
    combined_rows.each do |r|
      d = details_by_id[r['id']] || {}
      r['current_category']  = current_category_name(d) || ''
      r['current_block']     = current_block_name(d) || ''
      r['course_join_date']  = d['course_join_date']
      r['course_login_rate'] = d['course_login_rate']

      b = basic_by_id[r['id']] || {}
      r['pdca_url'] = extract_gsheets_url(b['free_text'])
    end

    # 受講日で新しい順
    combined_rows.sort_by! { |r| (Date.parse(r['course_join_date'].to_s) rescue Date.new(1900,1,1)) }
    combined_rows.reverse!

    # 7) ローカル書き出し（任意）
    timestamp = Time.zone.now.strftime('%Y%m%d_%H%M%S')
    dir       = Rails.root.join('tmp')
    FileUtils.mkdir_p(dir)
    course_tag = course_id

    status_files = {}
    grouped_rows.each do |motivation, rows|
      fname = dir.join("onclass_#{course_tag}_#{timestamp}_#{motivation}.csv")
      write_csv(fname, rows)
      status_files[motivation] = fname.to_s
    end

    combined_csv_path  = dir.join("onclass_#{course_tag}_#{timestamp}_combined.csv")
    write_csv(combined_csv_path, combined_rows)

    combined_xlsx_path = maybe_write_xlsx(dir, course_tag, timestamp, combined_rows)

    export_api_csv_path = export_official_csv(conn, default_headers, combined_rows, dir, course_tag, timestamp)

    result = {
      combined_csv: combined_csv_path.to_s,
      combined_xlsx: combined_xlsx_path,
      status_csvs: status_files,
      official_export_csv: export_api_csv_path
    }

    # 8) スプレッドシートへアップロード
    upload_to_gsheets!(
      rows: combined_rows,
      spreadsheet_id: ENV.fetch('ONCLASS_SPREADSHEET_ID'),
      sheet_name:     sheet_name
    )

    Rails.logger.info("[OnclassStudentsDataWorker] done: #{result.inspect}")
    result
  rescue Faraday::Error => e
    Rails.logger.error("[OnclassStudentsDataWorker] HTTP error: #{e.class} #{e.message}")
    raise
  rescue => e
    Rails.logger.error("[OnclassStudentsDataWorker] unexpected error: #{e.class} #{e.message}")
    raise
  end

  private

  # --- 一覧取得（モチベーション別 + ページング） ---
  def fetch_users_by_motivation(conn, headers, motivation, jp_label, learning_course_id)
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
      params[:motivation] = motivation unless motivation.nil?

      resp = conn.get('/v1/enterprise_manager/users', params, headers)
      json = JSON.parse(resp.body) rescue {}

      list =
        if json.is_a?(Array)
          json
        else
          json['users'] || json['data'] || json['records'] || []
        end

      list.each do |u|
        rows << {
          'id'               => u['id'] || u['user_id'] || u['uid'],
          'name'             => u['name'],
          'email'            => u['email'],
          'last_sign_in_at'  => u['last_sign_in_at'],
          'course_name'      => u['course_name'] || u['learning_course_name'] || u.dig('learning_course', 'name'),
          'course_start_at'  => u['course_start_at'] || u.dig('learning_course', 'start_at'),
          'course_progress'  => u['course_progress'],
          'latest_login_at'  => fetch_latest_login_at(conn, headers, u['id']),
          'status'           => jp_label
        }
      end

      total_pages   = (json['total_pages'] || json.dig('meta', 'total_pages')).to_i
      current_page  = (json['current_page'] || json.dig('meta', 'current_page') || page).to_i
      next_page     = json['next_page'] || json.dig('links', 'next')

      break if list.empty? || (total_pages > 0 && current_page >= total_pages) || (!next_page.nil? && next_page == false)
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

  # xlsx 書き出し（axlsx が無ければスキップ）
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

  # 公式CSVエクスポートAPI
  def export_official_csv(conn, headers, rows, dir, course_tag, timestamp)
    user_ids = rows.map { |r| r['id'] }.compact.uniq
    return nil if user_ids.empty?

    body = {
      target_columns: TARGET_COLUMNS,
      target_user_ids: user_ids
    }

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

  # ---- Google Sheets ----
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

  # スプレッドシート書き込み
  def upload_to_gsheets!(rows:, spreadsheet_id:, sheet_name:)
    service = build_sheets_service
    ensure_sheet_exists!(service, spreadsheet_id, sheet_name)

    clear_req = Google::Apis::SheetsV4::ClearValuesRequest.new
    service.clear_values(spreadsheet_id, "#{sheet_name}!B2:L2", clear_req)
    service.clear_values(spreadsheet_id, "#{sheet_name}!B3:L",  clear_req)

    # B2（メタ）
    meta_row   = ['バッチ実行タイミング', jp_timestamp] + Array.new(9, '')
    meta_range = "#{sheet_name}!B2:L2"
    service.update_spreadsheet_value(
      spreadsheet_id,
      meta_range,
      Google::Apis::SheetsV4::ValueRange.new(range: meta_range, values: [meta_row]),
      value_input_option: 'USER_ENTERED'
    )

    # 見出し（B3:L3）※ L列に PDCA を追加
    headers = %w[
      id 名前 メールアドレス ステータス ステータス_B
      現在進行カテゴリ 現在進行ブロック 受講日 ログイン率 最新ログイン日 PDCA
    ]
    header_range = "#{sheet_name}!B3:L3"
    service.update_spreadsheet_value(
      spreadsheet_id,
      header_range,
      Google::Apis::SheetsV4::ValueRange.new(range: header_range, values: [headers]),
      value_input_option: 'USER_ENTERED'
    )

    sanitized_rows = rows.reject do |r|
      id    = r['id'].to_s.strip
      name  = r['name'].to_s.strip
      email = r['email'].to_s.strip
      id.casecmp('id').zero? || name == '名前' || email == 'メールアドレス'
    end

    data_values = sanitized_rows.map do |r|
      [
        r['id'],
        hyperlink_name(r['id'], r['name']),
        r['email'],
        r['status'].presence || '',
        '',  # ステータス_B（空）
        r['current_category']  || '',
        r['current_block']     || '',
        to_jp_ymd(r['course_join_date']) || '',
        (r['course_login_rate'].nil? ? '' : r['course_login_rate'].to_s),
        to_jp_ymdhm(r['latest_login_at']) || '',
        hyperlink_pdca(r['pdca_url'], r['name']) # L列: PDCA
      ]
    end

    if data_values.any?
      data_range = "#{sheet_name}!B4"
      service.update_spreadsheet_value(
        spreadsheet_id,
        data_range,
        Google::Apis::SheetsV4::ValueRange.new(range: data_range, values: data_values),
        value_input_option: 'USER_ENTERED'
      )
    end

    Rails.logger.info("[OnclassStudentsDataWorker] uploaded #{sanitized_rows.size} rows with PDCA column.")
  end

  # ---- 表示ヘルパ ----
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
    t = (Time.zone.parse(str.to_s) rescue Time.parse(str.to_s) rescue nil)
    return '' unless t
    t.in_time_zone('Asia/Tokyo').strftime('%Y年%-m月%-d日 %H時%M分')
  end

  def hyperlink_name(id, name)
    return name.to_s if id.to_s.strip.empty?
    label = name.to_s.gsub('"', '""')
    url   = "https://manager.the-online-class.com/accounts/#{id}"
    %Q(=HYPERLINK("#{url}","#{label}"))
  end

  # ---- API: 個別詳細 / ログイン履歴 / 基本情報 ----
  def fetch_user_learning_course(conn, headers, student_id, learning_course_id)
    params = { learning_course_id: learning_course_id }
    resp   = conn.get("/v1/enterprise_manager/users/#{student_id}/learning_course", params, headers)
    json   = JSON.parse(resp.body) rescue {}
    json['data'] || json
  end

  def fetch_latest_login_at(conn, headers, user_id)
    resp = conn.get("/v1/enterprise_manager/users/#{user_id}/logins", { page: 1 }, headers)
    json = JSON.parse(resp.body) rescue {}
    list = json['data'] || json['logins'] || json['records'] || []
    first = list.is_a?(Array) ? list.first : nil
    first && first['created_at']
  rescue Faraday::Error => e
    Rails.logger.warn("[OnclassStudentsDataWorker] fetch_latest_login_at error for #{user_id}: #{e.class} #{e.message}")
    nil
  end

  def fetch_user_basic(conn, headers, student_id)
    resp = conn.get("/v1/enterprise_manager/users/#{student_id}", {}, headers)
    json = JSON.parse(resp.body) rescue {}
    json['data'] || json
  rescue Faraday::Error => e
    Rails.logger.warn("[OnclassStudentsDataWorker] fetch_user_basic error for #{student_id}: #{e.class} #{e.message}")
    {}
  end

  # free_text からGoogleスプレッドシートURLを抽出
  def extract_gsheets_url(free_text)
    return nil if free_text.to_s.strip.empty?
    m = free_text.match(%r{(https?://)?(docs\.google\.com/spreadsheets/d/[^\s"'>]+)}i)
    return nil unless m
    url = m.to_s
    url = "https://#{url}" unless url.start_with?('http')
    url
  end

  # HYPERLINK（PDCA）
  def hyperlink_pdca(url, name)
    return '' if url.to_s.strip.empty?
    label = "#{name}_PDCA".gsub('"', '""')
    %Q(=HYPERLINK("#{url}","#{label}"))
  end

  # 進行中の親カテゴリ（オブジェクト）を返す
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
end
