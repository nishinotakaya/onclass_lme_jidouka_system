# frozen_string_literal: true

require 'csv'

class OnclassStudentsDataWorker
  include Sidekiq::Worker
  sidekiq_options queue: 'onclass', retry: 3

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
end
