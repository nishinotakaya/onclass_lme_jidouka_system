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

  DEFAULT_LEARNING_COURSE_ID = 'oYTO4UDI6MGb' # フロント

  STATUS_ORDER = [
    ['very_good', '素晴らしい'],
    ['good',      '順調'],
    ['very_bad',  '離脱'],
    ['bad',       '停滞中'],
    ['normal',    '停滞気味']
  ].freeze

  TARGET_COLUMNS = %w[name email last_sign_in_at course_name course_start_at course_progress].freeze

  def perform(course_id = nil, sheet_name = nil)
    course_id  ||= ENV.fetch('ONCLASS_COURSE_ID', DEFAULT_LEARNING_COURSE_ID)
    sheet_name ||= ENV.fetch('ONCLASS_SHEET_NAME', 'フロントコース受講生')

    # 1) 既定アカウントでサインイン（トークン更新）
    OnclassSignInWorker.new.perform
    base_client  = OnclassAuthClient.new
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

    # 3) 一覧取得
    grouped_rows = {}
    STATUS_ORDER.each do |motivation, jp_label|
      rows = fetch_users_by_motivation(conn, base_headers, motivation, jp_label, course_id)
      grouped_rows[motivation] = rows
      Rails.logger.info("[OnclassStudentsDataWorker] fetched #{rows.size} users for #{motivation}(#{jp_label}) course=#{course_id}")
    end

    combined_rows = STATUS_ORDER.flat_map { |motivation, _| grouped_rows[motivation] }
    student_ids   = combined_rows.map { |r| r['id'] }.compact.uniq

    # 4) 詳細・基本・受講期限日
    details_by_id = {}
    student_ids.each do |sid|
      details_by_id[sid] = fetch_user_learning_course(conn, base_headers, sid, course_id)
      sleep 0.05
    end

    basic_by_id = {}
    student_ids.each do |sid|
      begin
        basic_by_id[sid] = fetch_user_basic(conn, base_headers, sid)
        sleep 0.03
      rescue => e
        Rails.logger.warn("[OnclassStudentsDataWorker] basic fetch failed for #{sid}: #{e.class} #{e.message}")
      end
    end

    extension_by_id = {}
    student_ids.each do |sid|
      extension_by_id[sid] = fetch_extension_study_date(conn, base_headers, sid, course_id)
      sleep 0.03
    end

    # 5) 未読メンション（アカウント別ログイン）
    accounts      = JSON.parse(ENV['ONLINE_CLASS_CREDENTIALS'] || '[]') rescue []
    nishino_cred  = accounts[0] || {}
    kato_cred     = accounts[1] || {}

    nishino_headers =
      (nishino_cred['email'] && nishino_cred['password']) ?
        OnclassSignInWorker.sign_in_headers_for(email: nishino_cred['email'], password: nishino_cred['password']) : nil
    kato_headers =
      (kato_cred['email'] && kato_cred['password']) ?
        OnclassSignInWorker.sign_in_headers_for(email: kato_cred['email'], password: kato_cred['password']) : nil

    nishino_maps = nishino_headers ? fetch_unread_mentions_map(conn, nishino_headers) : { id: {}, name: {} }
    kato_maps    = kato_headers    ? fetch_unread_mentions_map(conn, kato_headers)    : { id: {}, name: {} }

    # 6) 付加情報
    combined_rows.each do |r|
      d = details_by_id[r['id']] || {}
      r['current_category']              = current_category_name(d) || ''
      r['current_block']                 = current_block_name(d) || ''
      r['course_join_date']              = d['course_join_date']
      r['course_login_rate']             = d['course_login_rate']
      r['current_category_scheduled_at'] = scheduled_completed_at_for_current(d)
      r['categories_schedule']           = categories_schedule_map(d)
      r['extension_study_date']          = extension_by_id[r['id']]

      b = basic_by_id[r['id']] || {}
      free = b['free_text']
      r['pdca_url'] = extract_gsheets_url(free)
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


    # 受講日降順
    combined_rows.sort_by! { |r| (Date.parse(r['course_join_date'].to_s) rescue Date.new(1900,1,1)) }
    combined_rows.reverse!

    # 7) ローカル書き出し
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
    export_api_csv_path = export_official_csv(conn, base_headers, combined_rows, dir, course_tag, timestamp)

    result = {
      combined_csv: combined_csv_path.to_s,
      combined_xlsx: combined_xlsx_path,
      status_csvs: status_files,
      official_export_csv: export_api_csv_path
    }

    # 8) スプレッドシート
    upload_to_gsheets!(
      rows: combined_rows,
      spreadsheet_id: ENV.fetch('ONCLASS_SPREADSHEET_ID'),
      sheet_name:     sheet_name,
      nishino_maps:   nishino_maps,
      kato_maps:      kato_maps
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

  # -------- 一覧 --------
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

  # -------- スプレットシートにデータ投入 --------
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
    # 本体は O 列まで（最後は「加藤メンション」）、右マトリクスは P 列以降
    service.clear_values(spreadsheet_id, "#{sheet_name}!B2:O2", clear_req)
    service.clear_values(spreadsheet_id, "#{sheet_name}!B3:O",  clear_req)
    service.clear_values(spreadsheet_id, "#{sheet_name}!P2:ZZ", clear_req)

    # B2（メタ）
    meta_row   = ['バッチ実行タイミング', jp_timestamp] + Array.new(12, '')
    meta_range = "#{sheet_name}!B2:O2"
    service.update_spreadsheet_value(
      spreadsheet_id,
      meta_range,
      Google::Apis::SheetsV4::ValueRange.new(range: meta_range, values: [meta_row]),
      value_input_option: 'USER_ENTERED'
    )

    # 見出し（B3:O3）
    headers = %w[
      名前 Line メールアドレス ステータス ステータス_B
      現在進行カテゴリ/完了予定日 現在進行ブロック 受講日 受講期限日 ログイン率 最新ログイン日 PDCA
      西野メンション 加藤メンション
    ]
    header_range = "#{sheet_name}!B3:O3"
    service.update_spreadsheet_value(
      spreadsheet_id,
      header_range,
      Google::Apis::SheetsV4::ValueRange.new(range: header_range, values: [headers]),
      value_input_option: 'USER_ENTERED'
    )

    # 見出し除外 + 受講期限日がある人は除外
    sanitized_rows = rows.reject do |r|
      id    = r['id'].to_s.strip
      name  = r['name'].to_s.strip
      email = r['email'].to_s.strip
      id.casecmp('id').zero? || name == '名前' || email == 'メールアドレス'
    end
    data_values = sanitized_rows.map do |r|
      g_val_name     = r['current_category'].to_s
      g_val_date     = to_jp_ymd(r['current_category_scheduled_at'])
      g_val_combined = g_val_name
      g_val_combined = "#{g_val_name} / #{g_val_date}" unless g_val_name.empty? || g_val_date.empty?

      [
        hyperlink_name(r['id'], r['name']),
        hyperlink_line(r['line_url'], r['name']),
        r['email'],
        r['status'].presence || '',
        status_b_for(r),
        g_val_combined,
        r['current_block']                   || '',
        to_jp_ymd(r['course_join_date'])     || '',
        to_jp_ymd(r['extension_study_date']) || '',
        (r['course_login_rate'].nil? ? '' : r['course_login_rate'].to_s),
        to_jp_ymdhm(r['latest_login_at'])    || '',
        hyperlink_pdca(r['pdca_url'], r['name']),
        mention_cell(channel_counts_for(nishino_maps, r)),
        mention_cell(channel_counts_for(kato_maps, r))
      ]
    end


    if data_values.any?
      service.update_spreadsheet_value(
        spreadsheet_id,
        "#{sheet_name}!B4",
        Google::Apis::SheetsV4::ValueRange.new(range: "#{sheet_name}!B4", values: data_values),
        value_input_option: 'USER_ENTERED'
      )
    end

    # 「カリキュラム完了予定日」マトリクス（P列〜）
    service.update_spreadsheet_value(
      spreadsheet_id,
      "#{sheet_name}!P2",
      Google::Apis::SheetsV4::ValueRange.new(range: "#{sheet_name}!P2", values: [['カリキュラム完了予定日']]),
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
        "#{sheet_name}!P3",
        Google::Apis::SheetsV4::ValueRange.new(range: "#{sheet_name}!P3", values: [category_order]),
        value_input_option: 'USER_ENTERED'
      )

      schedule_matrix = sanitized_rows.map do |r|
        sched_map = r['categories_schedule'] || {}
        category_order.map { |name| to_jp_ymd(sched_map[name]) }
      end

      if schedule_matrix.any?
        service.update_spreadsheet_value(
          spreadsheet_id,
          "#{sheet_name}!P4",
          Google::Apis::SheetsV4::ValueRange.new(range: "#{sheet_name}!P4", values: schedule_matrix),
          value_input_option: 'USER_ENTERED'
        )
      end
    end
    # Todo::条件式書式を使いたい時のみ使用(1回きりでOK)
    # apply_mention_highlight_rule!(service, spreadsheet_id, sheet_name)
    Rails.logger.info("[OnclassStudentsDataWorker] uploaded #{sanitized_rows.size} rows with extension date, mentions and schedules (O列まで + P列〜).")
  end

  # -------- 表示ヘルパ(Todo::helperに移す) --------
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

  # 受講期限日（extension_study_date）
  def fetch_extension_study_date(conn, headers, user_id, learning_course_id)
    resp = conn.get(
      "/v1/enterprise_manager/enterprise_managers/current/learning_courses_for_user",
      { user_id: user_id },
      headers
    )
    json  = JSON.parse(resp.body) rescue {}
    list  = json['data'] || json['learning_courses'] || []
    item  = Array(list).find { |c| (c['id'] || c[:id]).to_s == learning_course_id.to_s }
    item && (item['extension_study_date'] || item[:extension_study_date])
  rescue Faraday::Error => e
    Rails.logger.warn("[OnclassStudentsDataWorker] fetch_extension_study_date error for #{user_id}: #{e.class} #{e.message}")
    nil
  end

  # -------- メンション --------
  def fetch_unread_mentions_map(conn, token_headers)
    headers = base_http_headers.merge(token_headers.slice('uid','access-token','client','token-type','expiry').compact)
    resp = conn.get('/v1/enterprise_manager/communities/activity/mentions', {}, headers)

    json = JSON.parse(resp.body) rescue {}
    list = Array(json['data'] || json['records'] || json)

    by_id   = Hash.new { |h,k| h[k] = Hash.new(0) }   # { sender_id => { channel => count } }
    by_name = Hash.new { |h,k| h[k] = Hash.new(0) }   # { norm_name => { channel => count } }

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


  # 例: {"TechPutチーム"=>3} => "TechPutチームより3件"
  # 複数チャンネルは " / " で連結
  def mention_cell(channel_counts)
    counts = channel_counts || {}
    return '' if counts.empty?
    parts = counts.map { |ch, c| "#{ch}より#{c}件" }
    label = parts.join(' / ')
    %Q(=HYPERLINK("https://manager.the-online-class.com/community","#{label.gsub('"','""')}"))
  end

  # -------- ステータスB（声かけ必須） --------
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

  # 「現在進行カテゴリが終わるのに2週間以上」判定
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

  # -------- 共通ヘッダ --------
  def base_http_headers
    {
      'accept'       => 'application/json, text/plain, */*',
      'content-type' => 'application/json',
      'origin'       => 'https://manager.the-online-class.com',
      'referer'      => 'https://manager.the-online-class.com/',
      'user-agent'   => 'Mozilla/5.0'
    }
  end

  # ---- 現在進行カテゴリ helper ----
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

  # 追加: ユーザー基本情報（free_textを含む）取得
  def fetch_user_basic(conn, headers, student_id)
    resp = conn.get("/v1/enterprise_manager/users/#{student_id}", {}, headers)
    json = JSON.parse(resp.body) rescue {}
    json['data'] || json
  rescue Faraday::Error => e
    Rails.logger.warn("[OnclassStudentsDataWorker] fetch_user_basic error for #{student_id}: #{e.class} #{e.message}")
    {}
  end

  # 追加: free_text からGoogleスプレッドシートURLを抽出
  def extract_gsheets_url(free_text)
    return nil if free_text.to_s.strip.empty?
    # プロトコルなしの docs.google.com も拾う
    m = free_text.match(%r{(https?://)?(docs\.google\.com/spreadsheets/d/[^\s"'>]+)}i)
    return nil unless m
    url = m.to_s
    url = "https://#{url}" unless url.start_with?('http')
    url
  end

  # 追加: HYPERLINK作成（名前は "#{name}_PDCA"）
  def hyperlink_pdca(url, name)
    return '' if url.to_s.strip.empty?
    label = "#{name}_PDCA".gsub('"', '""')
    %Q(=HYPERLINK("#{url}","#{label}"))
  end

  # "（…）" や "(…)" の肩書きを削除、空白を除去、全角スペースも除去
  def normalize_name(str)
    base = str.to_s
    base = base.gsub(/（.*?）|\(.*?\)/, '') # 括弧内除去（全角/半角）
    base = base.tr('　', ' ')              # 全角空白→半角
    base = base.gsub(/\s+/, '')            # 空白全削除
    base
  end

  # シートIDを取得_条件式書式
  def sheet_id_for(service, spreadsheet_id, sheet_name)
    ss = service.get_spreadsheet(spreadsheet_id)
    sheet = ss.sheets.find { |s| s.properties&.title == sheet_name }
    sheet&.properties&.sheet_id
  end

  # N/O 列に「メンション」が含まれる行の背景をオレンジにする条件付き書式を追加
  def apply_mention_highlight_rule!(service, spreadsheet_id, sheet_name)
    sid = sheet_id_for(service, spreadsheet_id, sheet_name)
    return unless sid # 念のため

    # 適用範囲: B4:O（行は4行目以降、列はB〜O）
    grid_range = Google::Apis::SheetsV4::GridRange.new(
      sheet_id: sid,
      start_row_index: 3,   # 0-based -> 4行目
      start_column_index: 1,# B列
      end_column_index: 15  # O列(排他的) => 15でB..O
    )

    # 条件: =COUNTIF($N4:$O4,"*メンション*")>0
    # ※ 行番号「4」は相対参照（行方向は固定しない）なので、各行に適用時に自動で 5,6,... とずれます
    cond = Google::Apis::SheetsV4::BooleanCondition.new(
      type: 'CUSTOM_FORMULA',
      values: [Google::Apis::SheetsV4::ConditionValue.new(
        # N/O列のどちらかに値が入っていればハイライト
        user_entered_value: '=COUNTA($N4:$O4)>0'
      )]
    )

    format = Google::Apis::SheetsV4::CellFormat.new(
      background_color: Google::Apis::SheetsV4::Color.new(red: 1.0, green: 0.9, blue: 0.6) # やわらかいオレンジ
    )

    rule = Google::Apis::SheetsV4::ConditionalFormatRule.new(
      ranges: [grid_range],
      boolean_rule: Google::Apis::SheetsV4::BooleanRule.new(
        condition: cond,
        format: format
      )
    )

    add_req = Google::Apis::SheetsV4::AddConditionalFormatRuleRequest.new(
      index: 0, # 先頭に追加
      rule: rule
    )

    batch = Google::Apis::SheetsV4::BatchUpdateSpreadsheetRequest.new(
      requests: [Google::Apis::SheetsV4::Request.new(
        add_conditional_format_rule: add_req
      )]
    )
    service.batch_update_spreadsheet(spreadsheet_id, batch)
  end

  # 2つの {channel=>count} を合算
  def merge_channel_counts(a, b)
    total = Hash.new(0)
    Array(a).each { |k,v| total[k.to_s] += v.to_i }
    Array(b).each { |k,v| total[k.to_s] += v.to_i }
    total
  end

  # 受講生のメンションをチャンネル別に集計して返す
  # maps は fetch_unread_mentions_map の戻り値
  def channel_counts_for(maps, row)
    return {} if maps.blank?
    id_map   = maps[:id]   || {}
    name_map = maps[:name] || {}
    by_id    = id_map[row['id']] || {}
    by_name  = name_map[normalize_name(row['name'])] || {}
    merge_channel_counts(by_id, by_name)
  end


  # 受講生のメンションをチャンネル別に集計して返す
  # maps は fetch_unread_mentions_map の戻り値
  def channel_counts_for(maps, row)
    return {} if maps.blank?
    id_map   = maps[:id]   || {}
    name_map = maps[:name] || {}
    by_id    = id_map[row['id']] || {}
    by_name  = name_map[normalize_name(row['name'])] || {}
    merge_channel_counts(by_id, by_name)
  end


  # free_text(自由項目) から LINE の URL を抽出
  def extract_line_url(free_text)
    return nil if free_text.to_s.strip.empty?
    m = free_text.match(%r{https?://step\.lme\.jp/basic/friendlist/my_page/\S+}i)
    return nil unless m
    m.to_s.strip
  end

  # HYPERLINK作成（名前は "#{name}_Line"）
  def hyperlink_line(url, name)
    return '' if url.to_s.strip.empty?
    label = "#{name}_Line".gsub('"', '""')
    %Q(=HYPERLINK("#{url}","#{label}"))
  end


end
