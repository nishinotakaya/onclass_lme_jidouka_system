# app/jobs/youtube/analytics_worker.rb
require "google/apis/youtube_v3"
require "google/apis/youtube_analytics_v2"
require "google/apis/sheets_v4"
require "googleauth"
require "json"

class Youtube::AnalyticsWorker
  include Sidekiq::Worker
  sidekiq_options queue: "youtube_analytics"

  # インプレッション集計の対象期間（Reporting API の日次リーチレポートを合算する日数）
  IMPRESSION_AGGREGATION_DAYS = 30

  # チャンネル登録増加数の「直近」集計期間（インプレッションと揃える）
  SUBSCRIBERS_RECENT_DAYS = IMPRESSION_AGGREGATION_DAYS

  # 「累計」クエリの開始日（YouTube 開設以前ならいつでもよい）
  ANALYTICS_LIFETIME_START_DATE = "2005-02-01".freeze

  # 引数:
  #   spreadsheet_url_arg : 出力先スプレッドシート URL / ID（nil なら ENV）
  #   sheet_name_arg      : シート名（nil なら ENV / デフォルト）
  # スケジューラからは引数なしで呼ばれ、画面フォームからは2引数で呼ばれる。
  def perform(spreadsheet_url_arg = nil, sheet_name_arg = nil)
    Rails.logger.info("[YouTubeAnalytics] Start (videos -> sheets)")

    # 1) OAuth（ブラウザで認可済みの onclass_jidouka クライアント）
    client = Google::YoutubeClient.new
    auth   = client.authorize!

    # v3: 動画一覧・サムネ・タイトル・公開日・視聴回数・高評価数・コメント
    youtube = Google::Apis::YoutubeV3::YouTubeService.new
    youtube.authorization = auth

    # ----------------------------------------------------
    # 2) 公開動画一覧を取得（サムネ・タイトル・公開日・視聴回数・高評価数）
    # ----------------------------------------------------
    videos = fetch_all_public_videos(youtube)
    Rails.logger.info("[YouTubeAnalytics] public_videos_count=#{videos.size}")

    # Reporting API: サムネイルのインプレッション数 / CTR（動画ID => [インプレッション数, CTR%]）
    impressions_by_video_id = fetch_thumbnail_impressions_by_video_id(auth)
    Rails.logger.info("[YouTubeAnalytics] impressions_rows=#{impressions_by_video_id.size}")

    # Analytics API: 動画をきっかけにしたチャンネル登録増加数（動画ID => [累計, 直近30日]）
    subscribers_gained_by_video_id =
      fetch_subscribers_gained_by_video_id(auth, videos.map(&:id))
    Rails.logger.info("[YouTubeAnalytics] subscribers_gained_rows=#{subscribers_gained_by_video_id.size}")

    # 1動画あたり取得するコメントの最大件数
    max_comments_per_video = 20

    # ----------------------------------------------------
    # 3) スプレッドシートに書き込むための values を組み立て
    # ----------------------------------------------------
    header = [
      "",
      "タイトル（リンク付き）",
      "出演者",
      "公開日",
      "視聴回数",
      "高評価数",
      "チャンネル登録増加数(累計)",
      "チャンネル登録増加数(直近30日)",
      "インプレッション数(直近30日)",
      "インプレッションCTR%(直近30日)",
      "アナリティクスURL"
    ] + (1..max_comments_per_video).map { |i| "コメント#{i}" }

    values = []

    # 1行目：空行
    values << []

    # 2行目：バッチ実行タイミング
    values << ["", "バッチ実行日時:  #{jp_timestamp}"]

    # 3行目：ヘッダー
    values << header

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

      impressions, impressions_click_rate_percent =
        impressions_by_video_id[vid] || [nil, nil]

      subscribers_gained_lifetime, subscribers_gained_recent =
        subscribers_gained_by_video_id[vid] || [nil, nil]

      # ---------- 出演者判定（description 内の最初の URL） ----------
      desc      = snippet.description.to_s
      first_url = desc.scan(%r{https?://\S+}).first

      # 出演者判定:
      #   1) uLand が既知の共通コードならその名前（日常/ショート/Live 等の区別を維持）
      #   2) タイトルに 加藤/小松/西野 が含まれていればその人
      #   3) それ以外は判定不能なので中立ラベル（無理に西野へ寄せない）
      #      ※ 多くの動画は uLand=mDTukc の汎用ランディングで出演者を特定できないため。
      performer_name = nil

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

      performer_name ||=
        if title.include?("加藤")
          "加藤"
        elsif title.include?("小松")
          "小松"
        elsif title.include?("西野")
          "西野"
        else
          "YouTube概要欄"
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
      analytics_url       = "https://studio.youtube.com/video/#{vid}/analytics/tab-reach_viewers/period-default"
      analytics_link_cell = %Q(=HYPERLINK("#{analytics_url}","アナリティクスURL"))

      # ---------- コメント取得（トップレベルコメントのみ） ----------
      comments = fetch_comments_for_video(youtube, vid, max_comments_per_video)

      # セル内で扱いやすいように整形
      # - 元の改行はスペースに
      # - 「。」「、」のあとでセル内改行
      comment_cells = comments.map { |text| format_comment_for_cell(text) }

      # 列数を揃えるため、足りない分は nil で埋める
      if comment_cells.size < max_comments_per_video
        comment_cells += Array.new(max_comments_per_video - comment_cells.size, nil)
      else
        comment_cells = comment_cells.first(max_comments_per_video)
      end

      values << [
        thumbnail_cell,
        %Q(=HYPERLINK("#{video_url}","#{escape_for_formula(title)}")),
        performer_cell,
        publish_date,
        view_count,
        like_count,
        subscribers_gained_lifetime,
        subscribers_gained_recent,
        impressions,
        impressions_click_rate_percent,
        analytics_link_cell,
        *comment_cells
      ]
    end

    # ----------------------------------------------------
    # 4) Sheets API で書き込み & 列幅/行高 調整
    # ----------------------------------------------------
    spreadsheet_id =
      if spreadsheet_url_arg.present?
        extract_spreadsheet_id_from_url(spreadsheet_url_arg)
      else
        ENV.fetch("YOUTUBE_ANALYTICS_SPREADSHEET_ID", ENV.fetch("ONCLASS_SPREADSHEET_ID"))
      end
    sheet_name = sheet_name_arg.presence || ENV.fetch("YOUTUBE_ANALYTICS_SHEET_NAME", "YouTube動画一覧")

    sheets = build_sheets_service
    ensure_sheet_exists!(sheets, spreadsheet_id, sheet_name)

    # 列はコメント含めると AE 列（固定11列＋コメント20列）まで使うので、少し余裕を見てクリア
    clear_req   = Google::Apis::SheetsV4::ClearValuesRequest.new
    clear_range = "#{sheet_name}!A:AZ"

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
      values.size,       # 行数（メタ＋ヘッダ込み）
      width_px:  120,
      height_px: 55
    )

    Rails.logger.info("[YouTubeAnalytics] wrote #{values.size - 3} rows to #{sheet_name}")
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

  # URL からスプレッドシート ID を抜き出す（ID だけ渡された場合はそのまま返す）
  def extract_spreadsheet_id_from_url(url)
    if url =~ %r{/spreadsheets/d/([^/]+)}
      Regexp.last_match(1)
    else
      url
    end
  end

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
  # Reporting API: サムネイルのインプレッション数 / CTR
  # 日次の channel_reach_basic_a1 レポートを直近30日分合算する。
  # レポートはジョブ作成の約2日後から生成されるため、未生成の間は空Hashを返し、
  # インプレッション2列は空のままシート書き込みを続行する。
  # --------------------------------
  def fetch_thumbnail_impressions_by_video_id(authorization)
    reporting  = Google::YoutubeReportingClient.new(authorization)
    daily_rows = reporting.recent_reach_rows(days: IMPRESSION_AGGREGATION_DAYS)

    if daily_rows.empty?
      Rails.logger.info("[YouTubeAnalytics] reachレポート未生成（ジョブ作成〜約2日はデータなし）。インプレッション2列は空で続行")
      return {}
    end

    impressions_and_clicks = Hash.new { |totals, video_id| totals[video_id] = { impressions: 0, clicks: 0.0 } }
    daily_rows.each do |daily_row|
      video_id = daily_row["video_id"].to_s
      next if video_id.empty?

      impressions = daily_row["video_thumbnail_impressions"].to_i
      click_rate  = daily_row["video_thumbnail_impressions_ctr"].to_f # 0〜1 の比率想定（初回データ到着時に要実測確認）
      impressions_and_clicks[video_id][:impressions] += impressions
      impressions_and_clicks[video_id][:clicks]      += impressions * click_rate
    end

    impressions_and_clicks.transform_values do |total|
      impressions        = total[:impressions]
      click_rate_percent = impressions.positive? ? (total[:clicks] / impressions * 100).round(2) : nil
      [impressions, click_rate_percent]
    end
  rescue Google::YoutubeReportingClient::ApiError => e
    Rails.logger.error("[YouTubeAnalytics] impressions(reachレポート)取得失敗: #{e.message}")
    {}
  end

  # --------------------------------
  # Analytics API: 動画をきっかけにしたチャンネル登録増加数（subscribersGained）
  # 動画ID => [累計, 直近30日] の Hash を返す。取得失敗時は空Hash
  # （＝該当2列は空のままシート書き込みを続行する）。
  # --------------------------------
  def fetch_subscribers_gained_by_video_id(authorization, video_ids)
    return {} if video_ids.empty?

    analytics = Google::Apis::YoutubeAnalyticsV2::YouTubeAnalyticsService.new
    analytics.authorization = authorization

    today_jp = Time.current.in_time_zone("Asia/Tokyo").to_date

    lifetime_by_video_id = query_subscribers_gained(
      analytics, video_ids,
      start_date: ANALYTICS_LIFETIME_START_DATE,
      end_date:   today_jp.to_s
    )
    recent_by_video_id = query_subscribers_gained(
      analytics, video_ids,
      start_date: (today_jp - SUBSCRIBERS_RECENT_DAYS).to_s,
      end_date:   today_jp.to_s
    )

    # クエリ成功時、レポートに行が無い動画は登録増 0 とみなす
    video_ids.index_with do |video_id|
      [lifetime_by_video_id.fetch(video_id, 0), recent_by_video_id.fetch(video_id, 0)]
    end
  rescue Google::Apis::ClientError, Google::Apis::AuthorizationError => e
    Rails.logger.error("[YouTubeAnalytics] subscribersGained取得失敗: #{e.message}")
    {}
  end

  # reports.query は video フィルタを1回あたり最大500件までしか受け付けないため分割して合算
  def query_subscribers_gained(analytics, video_ids, start_date:, end_date:)
    gained_by_video_id = {}

    video_ids.each_slice(500) do |sliced_video_ids|
      response = analytics.query_report(
        ids:         "channel==MINE",
        start_date:  start_date,
        end_date:    end_date,
        metrics:     "subscribersGained",
        dimensions:  "video",
        filters:     "video==#{sliced_video_ids.join(',')}",
        max_results: sliced_video_ids.size
      )

      (response.rows || []).each do |video_id, gained|
        gained_by_video_id[video_id.to_s] = gained.to_i
      end
    end

    gained_by_video_id
  end

  # --------------------------------
  # コメント取得（トップレベルのみ）
  # --------------------------------
  def fetch_comments_for_video(youtube, video_id, max_comments)
    comments   = []
    page_token = nil

    while comments.size < max_comments
      resp = youtube.list_comment_threads(
        "snippet",
        video_id:    video_id,
        max_results: [max_comments - comments.size, 100].min,
        page_token:  page_token,
        text_format: "plainText"
      )

      (resp.items || []).each do |thread|
        snippet = thread.snippet&.top_level_comment&.snippet
        text    = snippet&.text_display || snippet&.text_original
        next if text.to_s.strip.empty?

        comments << text
        break if comments.size >= max_comments
      end

      page_token = resp.next_page_token
      break if page_token.blank? || comments.size >= max_comments
    end

    comments
  rescue Google::Apis::ClientError => e
    Rails.logger.warn("[YouTubeAnalytics] fetch_comments_for_video(#{video_id}) failed: #{e.message}")
    []
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
          sheet_id:    sheet_id,
          dimension:   "COLUMNS",
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
            sheet_id:    sheet_id,
            dimension:   "ROWS",
            start_index: 1,          # index 1 = 2行目
            end_index:   row_count   # メタ＋ヘッダ含めた行数
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

  def jp_timestamp
    Time.current.in_time_zone("Asia/Tokyo").strftime("%Y年%-m月%-d日 %H時%M分")
  end

  def format_comment_for_cell(text)
    return "" if text.nil?

    s = text.to_s

    # 元の CR/LF はいったん潰す（YouTubeコメントの改行はとりあえずスペース扱い）
    s = s.gsub("\r", "").gsub("\n", " ")

    # 「。」「、」の直後でセル内改行
    # Sheets は "\n" をセル内改行として扱ってくれる
    s = s.gsub("。", "。\n").gsub("、", "、\n")

    # 末尾の余計な改行やスペースを削る
    s.rstrip
  end
end
