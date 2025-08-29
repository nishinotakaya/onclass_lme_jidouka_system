# app/workers/onclass_student_show_worker.rb
# frozen_string_literal: true

class OnclassStudentShowWorker
  include Sidekiq::Worker
  sidekiq_options queue: 'onclass_student_show_data', retry: 3, backtrace: true

  LEARNING_COURSE_ID = 'oYTO4UDI6MGb' # フロントコース

  def perform(student_id)
    raise ArgumentError, 'student_id is blank' if student_id.to_s.strip.empty?

    # 認証（トークン取得）
    OnclassSignInWorker.new.perform
    client  = OnclassAuthClient.new
    headers = client.headers

    conn = Faraday.new(url: client_base_url(client)) do |f|
      f.request  :json
      f.response :raise_error
      f.adapter  Faraday.default_adapter
    end

    default_headers = {
      'accept'       => 'application/json, text/plain, */*',
      'content-type' => 'application/json',
      'origin'       => 'https://manager.the-online-class.com',
      'referer'      => 'https://manager.the-online-class.com/',
      'user-agent'   => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
      'access-token' => headers['access-token'],
      'client'       => headers['client'],
      'uid'          => headers['uid']
    }.compact

    data = fetch_user_learning_course(conn, default_headers, student_id, LEARNING_COURSE_ID)

    # 保存（デバッグ/確認用）
    dir  = Rails.root.join('tmp', 'onclass_student')
    FileUtils.mkdir_p(dir)
    path = dir.join("student_#{student_id}_learning_course_#{Time.zone.now.strftime('%Y%m%d_%H%M%S')}.json")
    File.write(path, JSON.pretty_generate(data))
    Rails.logger.info("[OnclassStudentShowWorker] saved #{path}")

    data
  rescue Faraday::Error => e
    Rails.logger.error("[OnclassStudentShowWorker] HTTP error: #{e.class} #{e.message}")
    raise
  rescue => e
    Rails.logger.error("[OnclassStudentShowWorker] unexpected error: #{e.class} #{e.message}")
    raise
  end

  private

  # OnclassAuthClient に base_url アクセサが無くても動くようにフォールバック
  def client_base_url(client)
    return client.base_url if client.respond_to?(:base_url)
    client.instance_variable_get(:@base_url) ||
      client.instance_variable_get(:@conn)&.url_prefix&.to_s&.sub(%r{/\z}, '')
  end

  # GET /v1/enterprise_manager/users/:id/learning_course?learning_course_id=...
  def fetch_user_learning_course(conn, headers, student_id, learning_course_id)
    params = { learning_course_id: learning_course_id }
    resp = conn.get("/v1/enterprise_manager/users/#{student_id}/learning_course", params, headers)
    json = JSON.parse(resp.body) rescue {}
    # 返却は { "data": {...} } が想定
    json['data'] || json
  end
end
