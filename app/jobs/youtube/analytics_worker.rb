# app/jobs/youtube/analytics_worker.rb
require "google/apis/youtube_v3"
require "google/apis/youtube_analytics_v2"
require "google/apis/sheets_v4"
require "googleauth"
require "json"

class Youtube::AnalyticsWorker
  include Sidekiq::Worker

  def perform
    Rails.logger.info("[YouTubeAnalytics] Start (videos -> sheets)")

    # 1) OAuth（ブラウザで認可済みの onclass_jidouka クライアント）
    client = Google::YoutubeClient.new
    auth   = client.authorize!

    # v3: 動画一覧・サムネ・タイトル・公開日・視聴回数・高評価数
    youtube = Google::Apis::YoutubeV3::YouTubeService.new
    youtube.authorization = auth

    # v2: 動画別アナリティクス（平均視聴時間）
    analytics = Google::Apis::YoutubeAnalyticsV2::YouTubeAnalyticsService.new
    analytics.authorization = auth

    # ----------------------------------------------------
    # 2) 公開動画一覧を取得（サムネ・タイトル・公開日・視聴回数・高評価数）
    # ----------------------------------------------------
    videos = fetch_all_public_videos(youtube)
    Rails.logger.info("[YouTubeAnalytics] public_videos_count=#{videos.size}")

    # ----------------------------------------------------
    # 3) Analytics で video 単位の平均視聴時間だけ取る
    #    impressions 系は 400 になるので今は諦める
    # ----------------------------------------------------
    metrics_by_video = fetch_video_metrics(analytics) # { "videoId" => { views:, likes:, average_view_duration: } }

    # ----------------------------------------------------
    # 4) スプレッドシートに書き込むための values を組み立て
    # ----------------------------------------------------
    header = [
      "サムネイル",
      "タイトル（リンク付き）",
      "公開日",
      "視聴回数",
      "高評価数",
      "インプレッション数",        # いまは空欄埋め
      "インプレッションCTR",      # いまは空欄埋め
      "平均視聴時間(秒)"
    ]

    values = [header]

    videos.each do |video|
      vid      = video.id
      snippet  = video.snippet
      stats    = video.statistics
      metrics  = metrics_by_video[vid] || {}

      thumbnail_url = safe_thumbnail_url(snippet)
      title         = snippet.title.to_s
      video_url     = "https://www.youtube.com/watch?v=#{vid}"
      published_at  = snippet.published_at
      publish_date  = published_at ? published_at.to_date.to_s : ""

      view_count    = (stats&.view_count || metrics[:views]).to_i
      like_count    = (stats&.like_count || metrics[:likes]).to_i
      avg_duration  = (metrics[:average_view_duration] || "").to_s

      # インプレッション系は、今のプロジェクトだと Unknown identifier で落ちるため空欄にしておく
      impressions        = ""
      impressions_ctr    = ""

      values << [
        thumbnail_url.present? ? %(=IMAGE("#{thumbnail_url}")) : "",
        %Q(=HYPERLINK("#{video_url}","#{escape_for_formula(title)}")),
        publish_date,
        view_count,
        like_count,
        impressions,
        impressions_ctr,
        avg_duration
      ]
    end

    # ----------------------------------------------------
    # 5) Sheets API で書き込み
    # ----------------------------------------------------
    spreadsheet_id = ENV.fetch("YOUTUBE_SPREADSHEET_ID", ENV.fetch("ONCLASS_SPREADSHEET_ID"))
    sheet_name     = ENV.fetch("YOUTUBE_SHEET_NAME", "YouTube動画一覧")

    sheets = build_sheets_service
    ensure_sheet_exists!(sheets, spreadsheet_id, sheet_name)

    clear_req   = Google::Apis::SheetsV4::ClearValuesRequest.new
    clear_range = "#{sheet_name}!A:Z"

    # タブ全体クリア
    sheets.clear_values(spreadsheet_id, clear_range, clear_req)

    # A1 から一括書き込み
    body = Google::Apis::SheetsV4::ValueRange.new(values: values)
    sheets.update_spreadsheet_value(
      spreadsheet_id,
      "#{sheet_name}!A1",
      body,
      value_input_option: "USER_ENTERED"
    )

    Rails.logger.info("[YouTubeAnalytics] wrote #{values.size - 1} rows to #{sheet_name}")
  rescue Google::Apis::ClientError => e
    Rails.logger.error("[YouTubeAnalytics] ClientError: #{e.message}")
    Rails.logger.error(e.response_body) if e.respond_to?(:response_body)
    raise
  rescue => e
    Rails.logger.error("[YouTubeAnalytics] Unexpected error: #{e.class} #{e.message}")
    Rails.logger.error(e.backtrace.join("\n"))
    raise
  end

  # ====================================================
  # private
  # ====================================================
  private

  # --------------------------------
  # v3: 公開動画を全部取得
  # --------------------------------
  # --------------------------------
  # v3: 公開動画を全部取得（YOUTUBE_CHANNEL_ID 優先）
  # --------------------------------
  def fetch_all_public_videos(youtube)
    target_channel_id = ENV["YOUTUBE_CHANNEL_ID"].to_s.strip

    if target_channel_id.present?
      Rails.logger.info("[YouTubeAnalytics] use channel_id=#{target_channel_id}")
      channels = youtube.list_channels("contentDetails", id: target_channel_id)
    else
      Rails.logger.warn("[YouTubeAnalytics] YOUTUBE_CHANNEL_ID not set. fallback to mine=true")
      channels = youtube.list_channels("contentDetails", mine: true)
    end

    ch = channels.items&.first
    unless ch
      Rails.logger.warn("[YouTubeAnalytics] no channel found")
      return []
    end

    uploads_playlist_id = ch.content_details&.related_playlists&.uploads
    unless uploads_playlist_id
      Rails.logger.warn("[YouTubeAnalytics] uploads playlist not found")
      return []
    end

    # uploads プレイリストから videoId を全部集める
    video_ids  = []
    page_token = nil

    loop do
      resp = youtube.list_playlist_items(
        "contentDetails",
        playlist_id: uploads_playlist_id,
        max_results: 50,
        page_token:  page_token
      )
      resp.items.each do |item|
        vid = item.content_details&.video_id
        video_ids << vid if vid.present?
      end
      page_token = resp.next_page_token
      break if page_token.blank?
    end

    videos = []
    video_ids.each_slice(50) do |ids|
      resp = youtube.list_videos(
        "snippet,statistics,status",
        id: ids.join(",")
      )
      resp.items.each do |v|
        # 公開動画だけに絞る
        next unless v.status&.privacy_status == "public"
        videos << v
      end
    end

    videos
  end

  # --------------------------------
  # v2: video 単位の analytics
  # impressions 系は無視
  # --------------------------------
  def fetch_video_metrics(analytics)
    resp = analytics.query_report(
      ids:         "channel==MINE",
      start_date:  "2006-01-01",
      end_date:    Date.today.to_s,
      metrics:     "views,likes,averageViewDuration",
      dimensions:  "video",
      max_results: 10000
    )

    rows = resp.rows || []
    rows.each_with_object({}) do |row, hash|
      video_id = row[0]
      hash[video_id] = {
        views:                 row[1].to_i,
        likes:                 row[2].to_i,
        average_view_duration: row[3].to_i
      }
    end
  rescue Google::Apis::ClientError => e
    # impressions 系と違って、これが落ちることはレアだと思うが一応握りつぶす
    Rails.logger.warn("[YouTubeAnalytics] fetch_video_metrics failed: #{e.message}")
    {}
  end

  # --------------------------------
  # Sheets
  # --------------------------------
  def build_sheets_service
    service = Google::Apis::SheetsV4::SheetsService.new
    service.client_options.application_name = "Onclass YouTube Analytics Uploader"
    scope   = [Google::Apis::SheetsV4::AUTH_SPREADSHEETS]
    keyfile = ENV["GOOGLE_APPLICATION_CREDENTIALS"]

    raise "ENV GOOGLE_APPLICATION_CREDENTIALS is not set." if keyfile.nil? || keyfile.strip.empty?
    raise "Service account key not found: #{keyfile}" unless File.exist?(keyfile)

    json = JSON.parse(File.read(keyfile)) rescue nil
    unless json && json["type"] == "service_account" && json["private_key"] && json["client_email"]
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

  # --------------------------------
  # 小物ヘルパー
  # --------------------------------
  def safe_thumbnail_url(snippet)
    thumbs = snippet&.thumbnails
    return thumbs.high.url   if thumbs&.high&.url
    return thumbs.medium.url if thumbs&.medium&.url
    return thumbs.default.url if thumbs&.default&.url
    nil
  end

  def escape_for_formula(str)
    str.to_s.gsub('"', '""')
  end
end
