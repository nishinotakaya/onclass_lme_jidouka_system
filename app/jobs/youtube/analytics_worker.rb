# app/jobs/youtube/analytics_worker.rb
require "google/apis/youtube_v3"
require "google/apis/sheets_v4"
require "googleauth"
require "json"

class Youtube::AnalyticsWorker
  include Sidekiq::Worker
  sidekiq_options queue: "youtube_analytics"

  def perform
    Rails.logger.info("[YouTubeAnalytics] Start (videos -> sheets)")

    # 1) OAuth（ブラウザで認可済みの onclass_jidouka クライアント）
    client = Google::YoutubeClient.new
    auth   = client.authorize!

    # v3: 動画一覧・サムネ・タイトル・公開日・視聴回数・高評価数
    youtube = Google::Apis::YoutubeV3::YouTubeService.new
    youtube.authorization = auth

    # ----------------------------------------------------
    # 2) 公開動画一覧を取得（サムネ・タイトル・公開日・視聴回数・高評価数）
    # ----------------------------------------------------
    videos = fetch_all_public_videos(youtube)
    Rails.logger.info("[YouTubeAnalytics] public_videos_count=#{videos.size}")

    # ----------------------------------------------------
    # 3) スプレッドシートに書き込むための values を組み立て
    # ----------------------------------------------------
    header = [
      "サムネイル",
      "タイトル（リンク付き）",
      "出演者",
      "公開日",
      "視聴回数",
      "高評価数",
      "アナリティクスURL"
    ]

    values = [header]

    videos.each do |video|
      vid      = video.id
      snippet  = video.snippet
      stats    = video.statistics

      thumbnail_url = safe_thumbnail_url(snippet)
      title         = snippet.title.to_s
      video_url     = "https://www.youtube.com/watch?v=#{vid}"
      published_at  = snippet.published_at
      publish_date  = published_at ? published_at.to_date.to_s : ""

      view_count    = (stats&.view_count || 0).to_i
      like_count    = (stats&.like_count || 0).to_i

      # ---------- 出演者判定（description 内の最初の URL） ----------
      desc      = snippet.description.to_s
      first_url = desc.scan(%r{https?://\S+}).first

      performer_name = "YouTube概要欄"

      if first_url
        # uLand パラメータを抽出（host が s.lmes.jp / form.lmes.jp どちらでもOK）
        uland = first_url[/uLand=([A-Za-z0-9]+)/, 1]

        performer_map = {
          "2jTFMb" => "小松",
          "6HfkXp" => "加藤",
          "acSx8R" => "西野",
          "r3UAhT" => "YouTubeLive",
          "48vXlm" => "西野日常",
          "Hsm2mV" => "西野 ショート",
          "4YnKLB" => "加藤ショート",
          "VmEg4f" => "YouTube TOP"
        }

        if uland && performer_map[uland]
          performer_name = performer_map[uland]
        end
      end

      performer_cell =
        if first_url
          %Q(=HYPERLINK("#{first_url}","#{escape_for_formula(performer_name)}"))
        else
          performer_name
        end
      # --------------------------------------------------

      # サムネ：セル内フィット（A列を 120x70px に後から揃える）
      thumbnail_cell =
        if thumbnail_url.present?
          %Q(=IMAGE("#{thumbnail_url}", 1))
        else
          ""
        end

      # アナリティクスURL（YouTube Studio）
      analytics_url = "https://studio.youtube.com/video/#{vid}/analytics/tab-reach_viewers/period-default"
      analytics_link_cell = %Q(=HYPERLINK("#{analytics_url}","アナリティクスURL"))

      values << [
        thumbnail_cell,
        %Q(=HYPERLINK("#{video_url}","#{escape_for_formula(title)}")),
        performer_cell,
        publish_date,
        view_count,
        like_count,
        analytics_link_cell
      ]
    end

    # ----------------------------------------------------
    # 4) Sheets API で書き込み & 列幅/行高 調整
    # ----------------------------------------------------
    spreadsheet_id = ENV.fetch("YOUTUBE_ANALYTICS_SPREADSHEET_ID", ENV.fetch("ONCLASS_SPREADSHEET_ID"))
    sheet_name     = ENV.fetch("YOUTUBE_ANALYTICS_SHEET_NAME", "YouTube動画一覧")

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

    # A列の幅 & サムネ行の高さを 120x70px に揃える
    sheet_id = sheet_id_for(sheets, spreadsheet_id, sheet_name)
    resize_thumbnail_column_and_rows!(
      sheets,
      spreadsheet_id,
      sheet_id,
      values.size,       # 行数（ヘッダ込み）
      width_px:  70,
      height_px: 40
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

  # 指定シート名の sheet_id を取得
  def sheet_id_for(service, spreadsheet_id, sheet_name)
    ss = service.get_spreadsheet(spreadsheet_id)
    sheet = ss.sheets&.find { |s| s.properties&.title == sheet_name }
    sheet&.properties&.sheet_id
  end

  # A列の幅と 2行目以降の行の高さを調整
  def resize_thumbnail_column_and_rows!(service, spreadsheet_id, sheet_id, row_count, width_px:, height_px:)
    return unless sheet_id

    requests = []

    # A列の幅
    requests << Google::Apis::SheetsV4::Request.new(
      update_dimension_properties: Google::Apis::SheetsV4::UpdateDimensionPropertiesRequest.new(
        range: Google::Apis::SheetsV4::DimensionRange.new(
          sheet_id:  sheet_id,
          dimension: "COLUMNS",
          start_index: 0,  # A列
          end_index:   1
        ),
        properties: Google::Apis::SheetsV4::DimensionProperties.new(
          pixel_size: width_px
        ),
        fields: "pixelSize"
      )
    )

    # 2行目〜最終行の高さ
    if row_count > 1
      requests << Google::Apis::SheetsV4::Request.new(
        update_dimension_properties: Google::Apis::SheetsV4::UpdateDimensionPropertiesRequest.new(
          range: Google::Apis::SheetsV4::DimensionRange.new(
            sheet_id:  sheet_id,
            dimension: "ROWS",
            start_index: 1,          # index 1 = 2行目
            end_index:   row_count   # ヘッダ含めた行数
          ),
          properties: Google::Apis::SheetsV4::DimensionProperties.new(
            pixel_size: height_px
          ),
          fields: "pixelSize"
        )
      )
    end

    batch = Google::Apis::SheetsV4::BatchUpdateSpreadsheetRequest.new(requests: requests)
    service.batch_update_spreadsheet(spreadsheet_id, batch)
  end

  # --------------------------------
  # 小物ヘルパー
  # --------------------------------
  def safe_thumbnail_url(snippet)
    thumbs = snippet&.thumbnails
    return thumbs.high.url    if thumbs&.high&.url
    return thumbs.medium.url  if thumbs&.medium&.url
    return thumbs.default.url if thumbs&.default&.url
    nil
  end

  def escape_for_formula(str)
    str.to_s.gsub('"', '""')
  end
end
