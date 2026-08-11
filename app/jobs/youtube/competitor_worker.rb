# app/jobs/youtube/competitor_worker.rb
require "google/apis/youtube_v3"
require "google/apis/sheets_v4"
require "googleauth"
require "json"

class Youtube::CompetitorWorker
  include Sidekiq::Worker
  sidekiq_options queue: "youtube_competitors"

  # 競合チャンネル一覧（表示名＋トップページURL）
  COMPETITORS = [
    { name: "ランテック",                    url: "https://www.youtube.com/@_runteq_" },
    { name: "エンジニアチャンネル",          url: "https://www.youtube.com/@engrch" },
    { name: "しまぶーのIT大学",             url: "https://www.youtube.com/@shimabu_it" },
    { name: "TECH WORLD",                  url: "https://www.youtube.com/@TECHWORLD111" },
    { name: "KENTA / 雑食系エンジニアTV",  url: "https://www.youtube.com/@zsk_engineer" },
    { name: "就活2chねる【ゆっくり2chスレ】", url: "https://www.youtube.com/@syuukatunoryuugi" },
    { name: "ITエンジニアのキャリア相談室",   url: "https://www.youtube.com/@engineer_yosho" },
    { name: "あつまれ ITの森【RaiseTech公式】", url: "https://www.youtube.com/@IT-crossing" },
    { name: "元運送業エンジニアHiroshi",      url: "https://www.youtube.com/@hiroshi7955" },
    { name: "ぬるぽよ@この世に不適合",        url: "https://www.youtube.com/@nullpoyo" },
    { name: "お仕事ずんだもん",              url: "https://www.youtube.com/@oshigotozundamon" },
    { name: "ずんだもんつんだもん",          url: "https://www.youtube.com/@zundamon_tundamon" },
    { name: "テックキャンプのプログラミング塾", url: "https://www.youtube.com/@TechCampChannel" },
    { name: "セイト先生のWeb・ITエンジニア転職ラボ", url: "https://www.youtube.com/@webit7652" },
    { name: "まゆり 未経験エンジニア転職",        url: "https://www.youtube.com/@mikeiken-engineer" },
    { name: "キノコード / プログラミング学習チャンネル", url: "https://www.youtube.com/@kinocode" },
    { name: "いまにゅのAIプログラミング塾",          url: "https://www.youtube.com/@imanyu_programming" },
    { name: "プログラミングチュートリアル",          url: "https://www.youtube.com/@programming_tutorial_youtube" },
    { name: "PythonプログラミングVTuber サプー",     url: "https://www.youtube.com/@pythonvtuber9917" },
    { name: "IT菩薩モロー",                          url: "https://www.youtube.com/@it_bosatsu_moro" },
    # ---- 2026-08-06 リサーチ追加分（スクール・サービス公式）----
    { name: "侍エンジニア",                          url: "https://www.youtube.com/@samuraiengineer" },
    { name: "デイトラ",                              url: "https://www.youtube.com/@daily-trial" },
    { name: "RaiseTech公式",                         url: "https://www.youtube.com/@raisetech-official" },
    { name: "Winスクール",                           url: "https://www.youtube.com/@-win1293" },
    { name: "シーライクスTV",                        url: "https://www.youtube.com/@sheofficial0411" },
    { name: "paiza公式",                             url: "https://www.youtube.com/@paiza_official" },
    { name: "レバテック",                            url: "https://www.youtube.com/@levtech_official" },
    { name: "ポテパン はじめてのプログラミング",     url: "https://www.youtube.com/@potepanda" },
    # ---- 2026-08-06 リサーチ追加分（エンジニア転職・キャリア）----
    { name: "だれでもエンジニア / 山浦清透",         url: "https://www.youtube.com/@KiyotoUniv" },
    { name: "エンジニアファースト",                  url: "https://www.youtube.com/@engineer_first" },
    { name: "SESチャンネル",                         url: "https://www.youtube.com/@SES_CH" },
    { name: "エンジニアの裏話",                      url: "https://www.youtube.com/@Engineer_InsideStory" },
    { name: "せお丸@AI駆動開発",                     url: "https://www.youtube.com/@seomaru" },
    # ---- 2026-08-06 リサーチ追加分（プログラミング学習・IT教育）----
    { name: "ITすきま教室",                          url: "https://www.youtube.com/@itsukima" },
    { name: "ともすた",                              url: "https://www.youtube.com/@tomosta" },
    { name: "ムーザルちゃんねる",                    url: "https://www.youtube.com/@moozaru" },
    # ---- 2026-08-06 リサーチ追加分（AI活用・生成AI教育）----
    { name: "KEITO【AI&WEB ch】",                    url: "https://www.youtube.com/@keitoaiweb" },
    { name: "いけともch",                            url: "https://www.youtube.com/@iketomo-ch" },
    { name: "mikimiki web スクール",                 url: "https://www.youtube.com/@mikimikiweb" },
    { name: "にゃんたのAIチャンネル",                url: "https://www.youtube.com/@aivtuber2866" },
    { name: "さきのAIでええやん",                    url: "https://www.youtube.com/@saki_AI_eyan" },
    { name: "ここなのAI大学",                        url: "https://www.youtube.com/@cocona_ai_school" },
    { name: "チャエン【AI研究所】",                  url: "https://www.youtube.com/@chaen-ai-lab" },
    { name: "AI大学【AI&ChatGPT最新情報】",          url: "https://www.youtube.com/@AIAIChatGPT-cj4sh" },
    { name: "ジェネトピ | AIラジオ",                 url: "https://www.youtube.com/@genAI-topic" },
    { name: "AI FREAK",                              url: "https://www.youtube.com/@aifreak_ch" }
  ].freeze

  MAX_COMMENTS_PER_VIDEO        = 20
  MAX_VIDEOS_PER_CHANNEL_DEFAULT = 50

  # 引数:
  #   competitors_text       : 画面から渡すチャンネル一覧テキスト（nil ならデフォルト COMPETITORS）
  #   max_videos_arg         : 1chあたりの動画数（nil なら MAX_VIDEOS_PER_CHANNEL_DEFAULT）
  #   spreadsheet_url_arg    : 出力先スプレッドシート URL（nil なら ENV）
  #   sheet_name_arg         : シート名（nil なら ENV / デフォルト）
  def perform(competitors_text = nil,
              max_videos_arg = nil,
              spreadsheet_url_arg = nil,
              sheet_name_arg = nil)

    Rails.logger.info("[YouTubeCompetitors] Start")

    # OAuth（ブラウザ認可済みトークンを使う）
    client = Google::YoutubeClient.new
    auth   = client.authorize!

    youtube = Google::Apis::YoutubeV3::YouTubeService.new
    youtube.authorization = auth

    # 競合一覧
    competitors =
      if competitors_text.present?
        parse_competitors_text(competitors_text)
      else
        default_competitors
      end

    # 1ch あたりの動画数
    max_videos = (max_videos_arg.presence || MAX_VIDEOS_PER_CHANNEL_DEFAULT).to_i
    max_videos = 1   if max_videos < 1
    max_videos = 100 if max_videos > 100

    # 出力先スプレッドシート
    spreadsheet_id =
      if spreadsheet_url_arg.present?
        extract_spreadsheet_id_from_url(spreadsheet_url_arg)
      else
        default_spreadsheet_id
      end

    sheet_name = sheet_name_arg.presence || default_sheet_name

    Rails.logger.info("[YouTubeCompetitors] channels=#{competitors.size}, max_videos=#{max_videos}, spreadsheet_id=#{spreadsheet_id}, sheet_name=#{sheet_name}")

    # Sheets サービス
    sheets = build_sheets_service
    ensure_sheet_exists!(sheets, spreadsheet_id, sheet_name)

    # ========== データ取得 ==========
    header = [
      "サムネ",
      "チャンネル名（トップへのリンク）",
      "タイトル（リンク付き）",
      "公開日",
      "視聴回数",
      "高評価数"
    ] + (1..MAX_COMMENTS_PER_VIDEO).map { |i| "コメント#{i}" }

    values = []
    values << []
    values << ["", "バッチ実行日時: #{jp_timestamp}"]
    values << header

    competitors.each do |comp|
      name = comp[:name]
      url  = comp[:url]

      channel_id = resolve_channel_id(youtube, url)
      unless channel_id
        Rails.logger.warn("[YouTubeCompetitors] channel_id not found for url=#{url}")
        next
      end

      Rails.logger.info("[YouTubeCompetitors] fetching videos for #{name} (#{channel_id})")

      videos = fetch_recent_videos_for_channel(youtube, channel_id, max_videos)

      videos.each do |video|
        vid      = video.id
        snippet  = video.snippet
        stats    = video.statistics

        thumbnail_url = safe_thumbnail_url(snippet)
        video_title   = snippet.title.to_s
        video_url     = "https://www.youtube.com/watch?v=#{vid}"
        published_at  = snippet.published_at
        publish_date  = published_at ? published_at.to_date.to_s : ""

        view_count = (stats&.view_count || 0).to_i
        like_count = (stats&.like_count || 0).to_i

        channel_link_cell = %Q(=HYPERLINK("#{url}","#{escape_for_formula(name)}"))

        thumbnail_cell =
          if thumbnail_url.present?
            %Q(=IMAGE("#{thumbnail_url}", 1))
          else
            ""
          end

        # コメント取得
        comments = fetch_comments_for_video(youtube, vid, MAX_COMMENTS_PER_VIDEO)
        comment_cells = comments.map { |text| format_comment_for_cell(text) }

        if comment_cells.size < MAX_COMMENTS_PER_VIDEO
          comment_cells += Array.new(MAX_COMMENTS_PER_VIDEO - comment_cells.size, nil)
        else
          comment_cells = comment_cells.first(MAX_COMMENTS_PER_VIDEO)
        end

        values << [
          thumbnail_cell,
          channel_link_cell,
          %Q(=HYPERLINK("#{video_url}","#{escape_for_formula(video_title)}")),
          publish_date,
          view_count,
          like_count,
          *comment_cells
        ]
      end
    end

    # ========== Sheets 書き込み ==========
    clear_req   = Google::Apis::SheetsV4::ClearValuesRequest.new
    clear_range = "#{sheet_name}!A:AZ"
    sheets.clear_values(spreadsheet_id, clear_range, clear_req)

    body = Google::Apis::SheetsV4::ValueRange.new(values: values)
    sheets.update_spreadsheet_value(
      spreadsheet_id,
      "#{sheet_name}!A1",
      body,
      value_input_option: "USER_ENTERED"
    )

    # サムネ列の見た目調整（A列の幅 ＋ 行の高さ）
    sheet_id = sheet_id_for(sheets, spreadsheet_id, sheet_name)
    resize_thumbnail_column_and_rows!(
      sheets,
      spreadsheet_id,
      sheet_id,
      values.size,
      width_px: 120,
      height_px: 55
    )

    Rails.logger.info("[YouTubeCompetitors] done: rows=#{values.size - 3}")
  rescue Google::Apis::ClientError => e
    Rails.logger.error("[YouTubeCompetitors] ClientError: #{e.message}")
    Rails.logger.error(e.response_body) if e.respond_to?(:response_body)
    raise
  rescue => e
    Rails.logger.error("[YouTubeCompetitors] Unexpected error: #{e.class} #{e.message}")
    Rails.logger.error(e.backtrace.join("\n"))
    raise
  end

  # ===================== private =====================
  private

  # ---- サブクラスで差し替える既定値（対象リストと出力先だけを変えて再利用する）----
  def default_competitors
    COMPETITORS
  end

  def default_spreadsheet_id
    ENV["YOUTUBE_COMPETITORS_SPREADSHEET_ID"] ||
      ENV["YOUTUBE_ANALYTICS_SPREADSHEET_ID"] ||
      raise("YOUTUBE_COMPETITORS_SPREADSHEET_ID も YOUTUBE_ANALYTICS_SPREADSHEET_ID も設定されていません")
  end

  def default_sheet_name
    ENV["YOUTUBE_COMPETITORS_SHEET_NAME"] || "YouTube競合"
  end

  # 画面から渡されたテキスト → [{name:, url:}, ...] に変換
  def parse_competitors_text(text)
    text.to_s.lines.map(&:strip).reject(&:blank?).map do |line|
      if line.include?(",")
        name, url = line.split(",", 2).map(&:strip)
      else
        name = nil
        url  = line
      end
      next if url.blank?

      { name: (name.presence || url), url: url }
    end.compact
  end

  # URL からスプレッドシート ID を抜き出す
  def extract_spreadsheet_id_from_url(url)
    # https://docs.google.com/spreadsheets/d/{ID}/edit...
    if url =~ %r{/spreadsheets/d/([^/]+)}
      Regexp.last_match(1)
    else
      url # すでに ID だけ渡されたケースも許容
    end
  end

  # 競合URL（@ハンドル）から channel_id を解決。
  # @handle は forHandle で「そのハンドルの正確なチャンネル」を引く（検索の先頭採用だと
  # 同名の別チャンネルを拾ってしまうため）。forHandle で取れない時だけ検索にフォールバック。
  def resolve_channel_id(youtube, url)
    handle = url[%r{/@([^/?]+)}, 1]

    if handle
      resp = youtube.list_channels("id", for_handle: "@#{handle}")
      return resp.items.first.id if resp.items&.any?

      # フォールバック: forHandle で解決できない旧 URL 等
      resp = youtube.list_searches("snippet", q: handle, type: "channel", max_results: 1)
      return resp.items.first.id.channel_id if resp.items&.any?
    end

    nil
  rescue Google::Apis::ClientError => e
    Rails.logger.warn("[YouTubeCompetitors] resolve_channel_id failed for url=#{url}: #{e.message}")
    nil
  end

  # 特定チャンネルの「最新」動画を取得する。
  # Search API(order:date) はインデックス遅延・件数制限で最新を取りこぼすため、
  # 自チャンネルと同じく uploads プレイリスト（新しい順）から確実に取得する。
  def fetch_recent_videos_for_channel(youtube, channel_id, max_videos)
    channel = youtube.list_channels("contentDetails", id: channel_id).items&.first
    uploads_playlist_id = channel&.content_details&.related_playlists&.uploads
    return [] unless uploads_playlist_id

    # uploads は新しい順。非公開を捨てる分を見込んで、少し多めに videoId を集める
    video_ids  = []
    page_token = nil
    target_ids = max_videos * 2

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
      break if page_token.blank? || video_ids.size >= target_ids
    end

    videos = []
    video_ids.each_slice(50) do |ids|
      detail = youtube.list_videos("snippet,statistics,status", id: ids.join(","))
      (detail.items || []).each do |v|
        next unless v.status&.privacy_status == "public"
        videos << v
        break if videos.size >= max_videos
      end
      break if videos.size >= max_videos
    end

    videos
  end

  # コメント取得
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
    Rails.logger.warn("[YouTubeCompetitors] fetch_comments_for_video(#{video_id}) failed: #{e.message}")
    []
  end

  # ===== Sheets 周り =====
  def build_sheets_service
    service = Google::Apis::SheetsV4::SheetsService.new
    service.client_options.application_name = "Onclass YouTube Competitors Uploader"
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

  def sheet_id_for(service, spreadsheet_id, sheet_name)
    ss = service.get_spreadsheet(spreadsheet_id)
    sheet = ss.sheets&.find { |s| s.properties&.title == sheet_name }
    sheet&.properties&.sheet_id
  end

  def resize_thumbnail_column_and_rows!(service, spreadsheet_id, sheet_id, row_count, width_px:, height_px:)
    return unless sheet_id

    requests = []

    # A列の幅
    requests << Google::Apis::SheetsV4::Request.new(
      update_dimension_properties: Google::Apis::SheetsV4::UpdateDimensionPropertiesRequest.new(
        range: Google::Apis::SheetsV4::DimensionRange.new(
          sheet_id:    sheet_id,
          dimension:   "COLUMNS",
          start_index: 0,
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
            start_index: 1,
            end_index:   row_count
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

  # ===== 小物ヘルパー =====
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
    s = s.gsub("\r", "").gsub("\n", " ")
    s = s.gsub("。", "。\n").gsub("、", "、\n")
    s.rstrip
  end
end
