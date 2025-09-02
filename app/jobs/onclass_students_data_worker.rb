# frozen_string_literal: true

require 'csv'
require 'date'

class OnclassStudentsDataWorker
  include Sidekiq::Worker
  sidekiq_options queue: 'onclass_students_data', retry: 3

  require 'google/apis/sheets_v4'
  require 'googleauth'


  LEARNING_COURSE_ID = 'oYTO4UDI6MGb' # フロントコース
  STATUS_ORDER = [
    ['very_good', '素晴らしい'],
    ['good',      '順調'],
    ['very_bad',  '離脱'],
    ['bad',       '停滞中'],
    ['normal',    '停滞気味']
  ].freeze

  TARGET_COLUMNS = %w[name email last_sign_in_at course_name course_start_at course_progress].freeze

  def perform
    # 1) サインイン → トークン取得
    OnclassSignInWorker.new.perform
    client  = OnclassAuthClient.new
    headers = client.headers # => { "access-token", "client", "uid", "token-type", "expiry" }
    # 2) Faraday 接続（cURL相当ヘッダも付与）
    conn = Faraday.new(url: client.base_url) do |f|
      f.request  :json
      f.response :raise_error
      f.adapter  Faraday.default_adapter
    end

    default_headers = {
      'accept'            => 'application/json, text/plain, */*',
      'content-type'      => 'application/json',
      'origin'            => 'https://manager.the-online-class.com',
      'referer'           => 'https://manager.the-online-class.com/',
      'user-agent'        => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
      'access-token'      => headers['access-token'],
      'client'            => headers['client'],
      'uid'               => headers['uid']
    }.compact

    # 3) 各モチベーション別にユーザー取得（ページング考慮）
    grouped_rows = {} # motivation => [{...row...}]
    STATUS_ORDER.each do |motivation, jp_label|
      rows = fetch_users_by_motivation(conn, default_headers, motivation, jp_label)
      grouped_rows[motivation] = rows
      Rails.logger.info("[OnclassStudentsDataWorker] fetched #{rows.size} users for #{motivation}(#{jp_label})")
    end

    # 4) 指定の順で結合 + 右端に日本語ステータス列
    combined_rows = STATUS_ORDER.flat_map { |motivation, _| grouped_rows[motivation] }
    # 重複防止のため uniq
    student_ids = combined_rows.map { |r| r['id'] }.compact.uniq

    # (A) 同期でこのWorkerの中で詳細も取って “手元のconn/headersを再利用” する場合
    details_by_id = {}
    student_ids.each do |sid|
      details_by_id[sid] = fetch_user_learning_course(conn, default_headers, sid, LEARNING_COURSE_ID)
      # 例：combined_rows の各行にマージしたければここで：
      # row = combined_rows.find { |r| r['id'] == sid }
      # row['course_progress_rate'] = details_by_id[sid]['course_progress_rate']
    end
    timestamp = Time.zone.now.strftime('%Y%m%d_%H%M%S')
    dir       = Rails.root.join('tmp')
    FileUtils.mkdir_p(dir)

    # 個別CSV（任意: ご要望に合わせて出力）
    status_files = {}
    grouped_rows.each do |motivation, rows|
      fname = dir.join("onclass_frontcourse_#{timestamp}_#{motivation}.csv")
      write_csv(fname, rows)
      status_files[motivation] = fname.to_s
    end

    # 結合CSV
    combined_csv_path = dir.join("onclass_frontcourse_#{timestamp}_combined.csv")
    write_csv(combined_csv_path, combined_rows)

    # 可能ならExcel(xlsx)も出力（axlsx が無ければスキップ）
    combined_xlsx_path = maybe_write_xlsx(dir, timestamp, combined_rows)

    # 5) 公式エクスポートAPI（/v1/enterprise_manager/users/export_csv）を叩いてCSVも保存
    #    対象は結合結果の user_id（= APIの id）全体
    export_api_csv_path = export_official_csv(conn, default_headers, combined_rows, dir, timestamp)

    result = {
      combined_csv: combined_csv_path.to_s,
      combined_xlsx: combined_xlsx_path,
      status_csvs: status_files,
      official_export_csv: export_api_csv_path
    }

    # 保存したCSV（ステータス別）を「素晴らしい→順調→離脱→停滞中→停滞気味」の順で読み込み・結合してからアップロード
    load_and_merge_csvs(ordered_status_csv_paths(status_files))

    student_ids   = combined_rows.map { |r| r['id'] }.compact.uniq
    details_by_id = {}
    student_ids.each do |sid|
      begin
        details_by_id[sid] = fetch_user_learning_course(conn, default_headers, sid, LEARNING_COURSE_ID)
        sleep 0.05 # 必要に応じて調整
      rescue => e
        Rails.logger.warn("[OnclassStudentsDataWorker] detail fetch failed for #{sid}: #{e.class} #{e.message}")
      end
    end

    # 'current_category' 列を追加
    combined_rows.each do |r|
      d = details_by_id[r['id']] || {}
      r['current_category']  = current_category_name(d) || ''
      r['current_block']     = current_block_name(d) || ''
      r['course_join_date']  = d['course_join_date']     # 例: "2025-07-31"
      r['course_login_rate'] = d['course_login_rate']    # 例: 73.7
    end

    combined_rows.sort_by! do |r|
      (Date.parse(r['course_join_date'].to_s) rescue Date.new(1900,1,1))
    end
    combined_rows.reverse!  # 新しい順


    upload_to_gsheets!(
      rows: combined_rows,
      spreadsheet_id: ENV.fetch('ONCLASS_SPREADSHEET_ID'),
      sheet_name:     ENV.fetch('ONCLASS_SHEET_NAME', '受講生自動化')
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

  # --- cURL(検索) の Ruby 実装 ---
  # GET /v1/enterprise_manager/users?learning_course_id=...&motivation=...
  def fetch_users_by_motivation(conn, headers, motivation, jp_label)
    page = 1
    rows = []

    loop do
      params = {
        page: page,
        learning_course_id: LEARNING_COURSE_ID,
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

      # APIの配列キーに備えてフォールバック（users / data / records など）
      list =
        if json.is_a?(Array)
          json
        else
          json['users'] || json['data'] || json['records'] || []
        end

      # レコード正規化（列名はご指定の target_columns を中心に、存在しなければnil）
      list.each do |u|
        # 受講コースで最終フィルタ（API側でlearning_course_id指定済みだが念のため）
        course_name = u['course_name'] || u['learning_course_name'] || u.dig('learning_course', 'name')
        next if course_name && !course_name.include?('フロント') # ゆるめフィルタ

        rows << {
          'id'               => u['id'] || u['user_id'] || u['uid'],
          'name'             => u['name'],
          'email'            => u['email'],
          'last_sign_in_at'  => u['last_sign_in_at'],
          'course_name'      => course_name,
          'course_start_at'  => u['course_start_at'] || u.dig('learning_course', 'start_at'),
          'course_progress'  => u['course_progress'],
          'latest_login_at'  => fetch_latest_login_at(conn, headers, u['id']),
          'status'           => jp_label # 右端に日本語ステータス
        }
      end

      # ページング判定：よくある total_pages/current_page/next_page → 無ければ「空になったら終了」
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
      rows.each do |r|
        csv << headers.map { |h| r[h] }
      end
    end
  end

  def maybe_write_xlsx(dir, timestamp, rows)
    begin
      require 'axlsx'
      path = dir.join("onclass_frontcourse_#{timestamp}_combined.xlsx")
      p = Axlsx::Package.new
      wb = p.workbook
      headers = %w[id name email last_sign_in_at course_name course_start_at course_progress status]
      wb.add_worksheet(name: 'FrontCourse') do |sheet|
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

  # --- cURL(公式CSVエクスポート) の Ruby 実装 ---
  # POST /v1/enterprise_manager/users/export_csv
  def export_official_csv(conn, headers, rows, dir, timestamp)
    user_ids = rows.map { |r| r['id'] }.compact.uniq
    return nil if user_ids.empty?

    body = {
      target_columns: TARGET_COLUMNS,
      target_user_ids: user_ids
    }

    resp = conn.post('/v1/enterprise_manager/users/export_csv', body.to_json, headers)
    content_type = resp.headers['content-type'].to_s

    path = dir.join("onclass_frontcourse_#{timestamp}_official_export.csv")

    if content_type.include?('text/csv') || content_type.include?('application/octet-stream')
      # そのままCSVとして保存
      File.binwrite(path, resp.body)
      return path.to_s
    end

    # APIがJSONでURLを返すタイプにも一応対応
    json = JSON.parse(resp.body) rescue {}
    if (csv_str = json['csv'])
      File.write(path, csv_str)
      return path.to_s
    elsif (file_url = json['file_url'])
      # 外部URLダウンロードが必要な場合（社内NWやS3署名URLなど）
      bin = Faraday.get(file_url).body rescue nil
      if bin
        File.binwrite(path, bin)
        return path.to_s
      end
    end

    Rails.logger.warn('[OnclassStudentsDataWorker] export_csv のレスポンス形式が想定外でした')
    nil
  end

  def build_sheets_service
    service = Google::Apis::SheetsV4::SheetsService.new
    service.client_options.application_name = 'Onclass FrontCourse Uploader'

    scope   = [Google::Apis::SheetsV4::AUTH_SPREADSHEETS]
    keyfile = ENV['GOOGLE_APPLICATION_CREDENTIALS']

    # 事前チェック（分かりやすい例外にする）
    if keyfile.nil? || keyfile.strip.empty?
      raise "ENV GOOGLE_APPLICATION_CREDENTIALS is not set."
    end
    unless File.exist?(keyfile)
      raise "Service account key not found: #{keyfile}"
    end

    json = JSON.parse(File.read(keyfile)) rescue nil
    unless json && json['type'] == 'service_account' && json['private_key'] && json['client_email']
      raise "Invalid service account JSON: missing private_key/client_email/type=service_account"
    end

    # ★ ここがポイント：鍵JSONを“明示的に”渡す（= nil.gsub 回避）
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

  def hyperlink_name(id, name)
    return name.to_s if id.to_s.strip.empty?
    label = name.to_s.gsub('"', '""') # ダブルクォートエスケープ
    url   = "https://manager.the-online-class.com/accounts/#{id}"
    %Q(=HYPERLINK("#{url}","#{label}"))
  end

  # 日本時間の更新日時文字列を返す
  def jp_timestamp
    Time.now.in_time_zone('Asia/Tokyo').strftime('%Y年%-m月%-d日 %H時 %M分')
  end

  # "2025-08-29T09:46:40.915+09:00" → "2025年8月29日"
  def to_jp_ymd(str)
    return '' if str.blank?
    t = (Time.zone.parse(str.to_s) rescue Time.parse(str.to_s) rescue nil)
    return '' unless t
    t.in_time_zone('Asia/Tokyo').strftime('%Y年%-m月%-d日')
  end

  # "2025-08-29T09:46:40.915+09:00" → "2025年8月29日 09時46分"
  def to_jp_ymdhm(str)
    return '' if str.blank?
    t = (Time.zone.parse(str.to_s) rescue Time.parse(str.to_s) rescue nil)
    return '' unless t
    t.in_time_zone('Asia/Tokyo').strftime('%Y年%-m月%-d日 %H時%M分')
  end

  # スプレッドシート書き込み
  def upload_to_gsheets!(rows:, spreadsheet_id:, sheet_name:)
    service = build_sheets_service
    ensure_sheet_exists!(service, spreadsheet_id, sheet_name)

    # --- 1) 2行目の古いヘッダー痕跡を完全クリア ---
    clear_req = Google::Apis::SheetsV4::ClearValuesRequest.new
    service.clear_values(spreadsheet_id, "#{sheet_name}!B2:K2", clear_req)
    service.clear_values(spreadsheet_id, "#{sheet_name}!B3:K",  clear_req)

    # --- 2) B2 に「更新日時」「値」だけ入れる（C2..J2は空で上書き）---
    meta_row = ['バッチ実行タイミング', jp_timestamp] + Array.new(7, '') # 合計9セル(B..J)
    meta_range = "#{sheet_name}!B2:J2"
    service.update_spreadsheet_value(
      spreadsheet_id,
      meta_range,
      Google::Apis::SheetsV4::ValueRange.new(range: meta_range, values: [meta_row]),
      value_input_option: 'USER_ENTERED'
    )

    # --- 3) 見出しは B3:J3 に1回だけ ---
    headers = %w[
                  id 名前 メールアドレス ステータス ステータス_B
                  現在進行カテゴリ 現在進行ブロック 受講日 ログイン率 最新ログイン日
                ]
    header_range = "#{sheet_name}!B3:K3"
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
                    r['status'].presence || '',        # ステータス
                    '',                                # ステータス_B（空欄固定）
                    r['current_category']  || '',
                    r['current_block']     || '',
                    to_jp_ymd(r['course_join_date']) || '',# 受講日
                    r['course_login_rate'].nil? ? '' : r['course_login_rate'].to_s, # ログイン率
                    to_jp_ymdhm(r['latest_login_at']) || ''                      # 最終ログイン日
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

    Rails.logger.info("[OnclassStudentsDataWorker] uploaded #{sanitized_rows.size} rows (header single row; B2:J2 cleaned).")
  end

  def load_and_merge_csvs(csv_paths)
    headers = %w[id name email last_sign_in_at course_name course_start_at course_progress status]
    rows = []
    csv_paths.each do |path|
      CSV.foreach(path, headers: true, encoding: 'UTF-8') do |row|
        rows << headers.index_with { |h| row[h] }
      end
    end
    # 重複除去（idとstatusでユニークにする）
    rows.uniq { |r| [r['id'], r['status']] }
  end

  def ordered_status_csv_paths(status_files)
    # STATUS_ORDER の順番に、生成済みCSVのパスを並べる
    STATUS_ORDER.map { |motivation, _| status_files[motivation] }.compact
  end

  def fetch_user_learning_course(conn, headers, student_id, learning_course_id)
    params = { learning_course_id: learning_course_id }
    resp = conn.get("/v1/enterprise_manager/users/#{student_id}/learning_course", params, headers)
    json = JSON.parse(resp.body) rescue {}
    json['data'] || json
  end

  # 進行中の親カテゴリ（オブジェクト）を返す
  # - 最後の true の“次”を採用
  # - 全て true → nil（=全完了）
  # - true が一つも無い → 先頭の false、なければ先頭
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

  # 既存の名前関数はオブジェクト版を使って実装
  def current_category_name(detail)
    cat = current_category_object(detail)
    cat ? cat['name'] : '全て完了'
  end

  # 親カテゴリの“子”ブロック（最初の未完了）の名前を返す
  def current_block_name(detail)
    cat = current_category_object(detail)
    return '' unless cat

    blocks = Array(cat['category_blocks'])
    return '' if blocks.empty?

    bool = ->(v) { v == true }
    blk  = blocks.find { |b| !bool.call(b['is_completed']) } || blocks.first
    blk['name'].to_s
  end

  # 最新ログイン日時を1件取得
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

end
