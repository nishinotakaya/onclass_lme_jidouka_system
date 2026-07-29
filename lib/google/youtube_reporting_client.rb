# lib/google/youtube_reporting_client.rb
require "net/http"
require "csv"
require "json"

# YouTube Reporting API（バルクレポート）の薄いクライアント。
# サムネイルのインプレッション系メトリクスは Analytics API(targeted query) では提供されず、
# channel_reach_basic_a1 レポート（2026-01-15 追加）でのみ取得できるため REST を直接叩く。
# 認可スコープは yt-analytics.readonly（Analytics API と共通）。
class Google::YoutubeReportingClient
  API_BASE             = "https://youtubereporting.googleapis.com/v1".freeze
  REACH_REPORT_TYPE_ID = "channel_reach_basic_a1".freeze
  REACH_JOB_NAME       = "onclass_channel_reach".freeze

  class ApiError < StandardError; end

  # authorization: access_token を発行済みの Signet::OAuth2::Client
  def initialize(authorization)
    @authorization = authorization
  end

  # リーチレポートの生成ジョブを取得（無ければ作成）。
  # ジョブ作成の約2日後から日次レポートが生成され、過去約30日分もバックフィルされる。
  def ensure_reach_job!
    existing = list_jobs.find { |job| job["reportTypeId"] == REACH_REPORT_TYPE_ID }
    existing || create_job(REACH_REPORT_TYPE_ID, REACH_JOB_NAME)
  end

  # 直近 days 日分の日次リーチ行（CSVヘッダー名 => 値 の Hash）を全レポート分まとめて返す
  def recent_reach_rows(days:)
    job       = ensure_reach_job!
    threshold = (Time.now.utc - days * 86_400).iso8601

    list_reports(job["id"], start_time_at_or_after: threshold).flat_map do |report|
      csv_text = get_raw(report["downloadUrl"])
      CSV.parse(csv_text, headers: true).map(&:to_h)
    end
  end

  private

  def list_jobs
    get_json("#{API_BASE}/jobs")["jobs"] || []
  end

  def create_job(report_type_id, name)
    post_json("#{API_BASE}/jobs", { reportTypeId: report_type_id, name: name })
  end

  def list_reports(job_id, start_time_at_or_after:)
    reports    = []
    page_token = nil

    loop do
      params = { "startTimeAtOrAfter" => start_time_at_or_after }
      params["pageToken"] = page_token if page_token
      body = get_json("#{API_BASE}/jobs/#{job_id}/reports?#{URI.encode_www_form(params)}")

      reports.concat(body["reports"] || [])
      page_token = body["nextPageToken"]
      break if page_token.blank?
    end

    reports
  end

  def get_json(url)
    JSON.parse(request(Net::HTTP::Get, url).body)
  end

  def post_json(url, payload)
    response = request(Net::HTTP::Post, url) do |http_request|
      http_request["Content-Type"] = "application/json"
      http_request.body = payload.to_json
    end
    JSON.parse(response.body)
  end

  def get_raw(url)
    request(Net::HTTP::Get, url).body
  end

  def request(method_class, url)
    uri  = URI(url)
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true

    http_request = method_class.new(uri)
    http_request["Authorization"] = "Bearer #{@authorization.access_token}"
    yield http_request if block_given?

    response = http.request(http_request)
    unless response.is_a?(Net::HTTPSuccess)
      raise ApiError, "#{http_request.method} #{uri.path} failed: #{response.code} #{response.body.to_s[0, 300]}"
    end
    response
  end
end
