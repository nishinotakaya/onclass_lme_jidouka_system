# frozen_string_literal: true
# app/jobs/youtube/lme_landing_worker.rb
#
# 新規に公開された YouTube 動画に対して、
#   1. LME の QRコードアクション（ランディング）を動画タイトル名で自動作成
#      （西野: 「youtube nishino」フォルダ = LME_YOUTUBE_LANDING_CATEGORY_ID）
#   2. 動画概要欄内の LME URL (https://s.lmes.jp/landing-qr/...?uLand=...) を
#      作成したランディングの URL に差し替え（YouTube Data API videos.update）
# を行うバッチ。
#
# 処理済み管理は管理シート（LMEランディング管理）で行う:
#   - シートが空の初回実行時は、既存の公開動画を「seeded」で登録するだけ（作成しない）
#   - 以降の実行で、シートに無い公開動画 = 新規動画 として処理する
require "google/apis/youtube_v3"
require "google/apis/sheets_v4"
require "googleauth"
require "json"

class Youtube::LmeLandingWorker
  include Sidekiq::Worker
  sidekiq_options queue: "youtube_lme_landing", retry: 1, lock: :until_executed

  ORIGIN      = 'https://step.lme.jp'
  UA          = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
  ACCEPT_LANG = 'ja,en-US;q=0.9,en;q=0.8'
  CH_UA       = %Q("Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140")

  # 差し替え対象は landing-qr URL に限定（form.lmes.jp 等の別種URLは触らない）
  LME_URL_PATTERN = %r{https://s\.lmes\.jp/landing-qr/[A-Za-z0-9\-]+(?:\?uLand=[A-Za-z0-9]+)?}

  # 出演者判定は「概要欄に貼られた LME URL の uLand コード」だけで行う。
  # 運用ルール: 動画公開時に出演者ごとの固定 uLand を概要欄に入れる。
  #   西野 → acSx8R（日常/ショート/Live/TOP 等の西野系コードも西野扱い）
  #   加藤 → 6HfkXp（ショート 4YnKLB も加藤扱い）
  #   小松 → 2jTFMb
  # ★ これらのコードに該当しない動画（専用uLandが既に入っている/LME URLが無い等）は
  #   対象外としてスルーする（フォールバックで西野にはしない）。
  # 管理名は「<出演者>　<動画タイトル>」、フォルダも出演者ごとに分ける。
  # フォルダ ID は LME 実在フォルダ（/ajax/get-list-group-landing で確認）:
  #   youtube nishino=5464631 / youtube kato=5463814 / youtube komatsu=5464667
  PERFORMERS = [
    {
      name:                "加藤",
      uland_codes:         %w[6HfkXp 4YnKLB],
      category_env:        "LME_YOUTUBE_KATO_CATEGORY_ID",
      default_category_id: "5463814" # youtube kato フォルダ
    },
    {
      name:                "小松",
      uland_codes:         %w[2jTFMb],
      category_env:        "LME_YOUTUBE_KOMATSU_CATEGORY_ID",
      default_category_id: "5464667" # youtube komatsu フォルダ
    },
    {
      name:                "西野",
      uland_codes:         %w[acSx8R 48vXlm Hsm2mV r3UAhT VmEg4f],
      category_env:        "LME_YOUTUBE_LANDING_CATEGORY_ID",
      default_category_id: "5464631" # youtube nishino フォルダ
    }
  ].freeze

  MAP_HEADER = %w[動画ID タイトル 公開日 landing_id landing_url ステータス 更新日時 出演者].freeze

  STATUS_SEEDED             = "seeded"             # 初回シード（処理対象外）
  STATUS_LANDING_CREATED    = "landing_created"    # ランディング作成済み・概要欄更新が未完（次回リトライ）
  STATUS_DONE               = "done"
  STATUS_NO_LME_URL         = "no_lme_url"         # 概要欄に LME URL が無く差し替え不能（要手動対応）
  STATUS_LANDING_ID_MISSING = "landing_id_missing" # 作成レスポンスから id を特定できず（重複作成防止のため自動リトライしない・要手動対応）
  STATUS_CATEGORY_MISSING   = "category_missing"   # 出演者は判明したがフォルダIDのENVが未設定（要設定→手動再実行）
  STATUS_ALREADY_IN_LME     = "already_in_lme"     # 同名ランディングが LME に既存（手動作成済みとみなしスルー）
  STATUS_NOT_TARGET         = "not_target"         # 概要欄に既知の出演者uLandが無く対象外（スルー）

  # video_id_arg を渡すと、その動画だけを強制的に処理する（done 以外なら再実行）
  def perform(video_id_arg = nil)
    Rails.logger.info("[YoutubeLmeLanding] Start (video_id_arg=#{video_id_arg.inspect})")

    youtube = build_youtube_service
    videos  = fetch_public_videos(youtube)
    Rails.logger.info("[YoutubeLmeLanding] public_videos_count=#{videos.size}")

    sheets = build_sheets_service
    ensure_sheet_exists!(sheets, spreadsheet_id, map_sheet_name)
    map_rows = load_map_rows(sheets)

    # ---- 初回はシードのみ（既存動画にはランディングを作らない）----
    if map_rows.empty? && video_id_arg.blank?
      seed_rows = videos.map { |video| build_row(video, status: STATUS_SEEDED) }
      write_map_rows(sheets, seed_rows)
      Rails.logger.info("[YoutubeLmeLanding] seeded #{seed_rows.size} existing videos. no landing created.")
      return
    end

    targets = select_targets(videos, map_rows, video_id_arg)
    if targets.empty?
      Rails.logger.info("[YoutubeLmeLanding] no new videos. skip LME login.")
      return
    end
    Rails.logger.info("[YoutubeLmeLanding] targets=#{targets.map(&:id).join(',')}")

    landing_service = build_landing_service

    targets.each do |video|
      process_video(video, map_rows, landing_service, youtube, sheets)
    rescue => e
      Rails.logger.error("[YoutubeLmeLanding] video=#{video.id} failed: #{e.class} #{e.message}")
    end

    Rails.logger.info("[YoutubeLmeLanding] Done")
  rescue => e
    Rails.logger.error("[YoutubeLmeLanding] Unexpected error: #{e.class} #{e.message}")
    Rails.logger.error(e.backtrace.join("\n"))
    raise
  end

  private

  # ====================================================
  # メイン処理（1動画）
  # ====================================================
  def process_video(video, map_rows, landing_service, youtube, sheets)
    video_id  = video.id
    title     = video.snippet.title.to_s
    row       = map_rows[video_id] ||= build_row(video, status: "")
    performer = detect_performer(video)

    # 既知の出演者uLandが無ければ対象外としてスルー（西野に寄せない）
    if performer.nil?
      row["ステータス"] = STATUS_NOT_TARGET
      touch_row(row)
      write_map_rows(sheets, map_rows.values)
      Rails.logger.info("[YoutubeLmeLanding] video=#{video_id} 既知の出演者uLand無し→対象外スルー")
      return
    end
    row["出演者"] = performer[:name]
    category_id = performer_category_id(performer)
    if category_id.blank?
      row["ステータス"] = STATUS_CATEGORY_MISSING
      touch_row(row)
      write_map_rows(sheets, map_rows.values)
      Rails.logger.warn("[YoutubeLmeLanding] video=#{video_id} performer=#{performer[:name]} のフォルダID(#{performer[:category_env]})が未設定。作成を保留。")
      return
    end

    # ---- 1) ランディング作成（未作成のときだけ）----
    if row["landing_id"].blank?
      landing_name = landing_name_for(performer, title)

      # すでに LME に同名ランディングがある（手動作成済み等）ならスルー
      if landing_service.landing_exists?([landing_name, title], category_id: category_id)
        row["ステータス"] = STATUS_ALREADY_IN_LME
        touch_row(row)
        write_map_rows(sheets, map_rows.values)
        Rails.logger.info("[YoutubeLmeLanding] video=#{video_id} 同名ランディングが既存のためスルー: #{landing_name}")
        return
      end

      # landing 作成 → タグ「<landing_name>」作成/紐付け → 名前・フォルダ確定まで一括
      created = landing_service.create_landing_with_tag(
        name:        landing_name,
        category_id: category_id
      )
      row["landing_id"]  = created[:landing_id].to_s
      row["landing_url"] = created[:landing_url].to_s
      # id を特定できなかった場合は自動リトライさせない（重複作成防止）。
      # ただし URL がレスポンスから取れていれば概要欄の差し替えまでは進める。
      row["ステータス"] = row["landing_id"].present? ? STATUS_LANDING_CREATED : STATUS_LANDING_ID_MISSING
      touch_row(row)
      # 作成直後に必ず保存（以降で失敗しても二重作成を防ぐ）
      write_map_rows(sheets, map_rows.values)

      if row["landing_id"].blank?
        Rails.logger.error("[YoutubeLmeLanding] video=#{video_id} landing_id not found in response. raw=#{created[:raw_body].to_s[0, 300]}")
        return if row["landing_url"].blank?
      end
    end

    # ---- 2) ランディング URL 取得 ----
    if row["landing_url"].blank? && row["landing_id"].blank?
      Rails.logger.error("[YoutubeLmeLanding] video=#{video_id} landing_id/url どちらも無し。手動対応が必要。")
      return
    end
    if row["landing_url"].blank?
      row["landing_url"] = landing_service.fetch_landing_url(row["landing_id"], category_id: category_id).to_s
      if row["landing_url"].blank?
        touch_row(row)
        write_map_rows(sheets, map_rows.values)
        Rails.logger.error("[YoutubeLmeLanding] video=#{video_id} landing_url not found (landing_id=#{row['landing_id']}). retry next run.")
        return
      end
    end

    # ---- 3) YouTube 概要欄の LME URL を差し替え ----
    replaced = replace_description_url(youtube, video_id, row["landing_url"])
    row["ステータス"] = replaced ? STATUS_DONE : STATUS_NO_LME_URL
    touch_row(row)
    write_map_rows(sheets, map_rows.values)

    Rails.logger.info("[YoutubeLmeLanding] video=#{video_id} status=#{row['ステータス']} landing_id=#{row['landing_id']} url=#{row['landing_url']}")
  end

  # 概要欄内の最初の LME URL を新 URL に置換（同一 URL は全箇所置換）
  # 置換できたら true / LME URL が無い・すでに新 URL なら false / true(実質no-op)
  def replace_description_url(youtube, video_id, new_landing_url)
    video = youtube.list_videos("snippet", id: video_id).items.first
    raise "video not found: #{video_id}" unless video

    description = video.snippet.description.to_s
    old_url = description[LME_URL_PATTERN]
    return false if old_url.blank?
    return true  if old_url == new_landing_url

    video.snippet.description = description.gsub(old_url, new_landing_url)
    youtube.update_video("snippet", video)
    true
  end

  # ====================================================
  # 対象選定
  # ====================================================
  def select_targets(videos, map_rows, video_id_arg)
    if video_id_arg.present?
      forced = videos.find { |v| v.id == video_id_arg }
      raise "public video not found: #{video_id_arg}" unless forced
      return [] if map_rows[video_id_arg]&.fetch("ステータス", nil) == STATUS_DONE
      return [forced]
    end

    # 未処理 or 作成途中、かつ「既知の出演者uLandを持つ」動画だけを対象にする。
    # → 対象が無ければ LME ログイン（2Captcha消費）自体を発生させない。
    videos.select do |video|
      row = map_rows[video.id]
      next false unless row.nil? || row["ステータス"] == STATUS_LANDING_CREATED

      detect_performer(video).present?
    end
  end

  # ====================================================
  # YouTube
  # ====================================================
  def build_youtube_service
    auth = Google::YoutubeClient.new.authorize!
    youtube = Google::Apis::YoutubeV3::YouTubeService.new
    youtube.authorization = auth
    youtube
  end

  def fetch_public_videos(youtube)
    target_channel_id = ENV["YOUTUBE_CHANNEL_ID"].to_s.strip
    channels =
      if target_channel_id.present?
        youtube.list_channels("contentDetails", id: target_channel_id)
      else
        youtube.list_channels("contentDetails", mine: true)
      end

    uploads_playlist_id = channels.items&.first&.content_details&.related_playlists&.uploads
    return [] unless uploads_playlist_id

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
        video_id = item.content_details&.video_id
        video_ids << video_id if video_id.present?
      end
      page_token = resp.next_page_token
      break if page_token.blank?
    end

    videos = []
    video_ids.each_slice(50) do |ids|
      resp = youtube.list_videos("snippet,status", id: ids.join(","))
      resp.items.each do |video|
        videos << video if video.status&.privacy_status == "public"
      end
    end
    videos
  end

  # ====================================================
  # LME
  # ====================================================
  def build_landing_service
    bot_id = (ENV['LME_BOT_ID'].presence || '17106').to_s
    ctx = Lme::ApiContext.new(
      origin: ORIGIN, ua: UA, accept_lang: ACCEPT_LANG, ch_ua: CH_UA,
      logger: Rails.logger, bot_id: bot_id
    )
    ctx.login_with_google!(
      email:    ENV['GOOGLE_EMAIL'],
      password: ENV['GOOGLE_PASSWORD'],
      api_key:  ENV['API2CAPTCHA_KEY']
    ).ensure_csrf_meta!

    Lme::LandingService.new(ctx: ctx)
  end

  # 概要欄の LME URL の uLand コードで出演者を判定。
  # 既知コードに該当しなければ nil（＝対象外）。
  def detect_performer(video)
    uland_code = video.snippet.description.to_s[LME_URL_PATTERN].to_s[/uLand=([A-Za-z0-9]+)/, 1]
    return nil if uland_code.blank?

    PERFORMERS.find { |performer| performer[:uland_codes].include?(uland_code) }
  end

  def performer_category_id(performer)
    ENV[performer[:category_env]].presence || performer[:default_category_id]
  end

  # 他の流入元の命名に合わせ、管理名は「<出演者>　<動画タイトル>」形式にする
  def landing_name_for(performer, title)
    "#{performer[:name]}　#{title}"
  end

  # ====================================================
  # 管理シート
  # ====================================================
  def spreadsheet_id
    ENV["YOUTUBE_ANALYTICS_SPREADSHEET_ID"].presence || ENV.fetch("ONCLASS_SPREADSHEET_ID")
  end

  def map_sheet_name
    ENV.fetch("YOUTUBE_LME_LANDING_SHEET_NAME", "LMEランディング管理")
  end

  def build_row(video, status:)
    {
      "動画ID"      => video.id,
      "タイトル"    => video.snippet.title.to_s,
      "公開日"      => video.snippet.published_at&.to_date.to_s,
      "landing_id"  => "",
      "landing_url" => "",
      "ステータス"  => status,
      "更新日時"    => jp_timestamp,
      "出演者"      => ""
    }
  end

  def touch_row(row)
    row["更新日時"] = jp_timestamp
  end

  # { video_id => row(Hash) } で返す
  def load_map_rows(sheets)
    range  = "#{map_sheet_name}!A2:G"
    values = sheets.get_spreadsheet_values(spreadsheet_id, range).values || []
    values.each_with_object({}) do |cells, rows|
      video_id = cells[0].to_s.strip
      next if video_id.blank?

      rows[video_id] = MAP_HEADER.each_with_index.to_h { |column, i| [column, cells[i].to_s] }
    end
  end

  # 行は増える/更新されるのみで減らないため、クリアせず A1 から上書きする
  # （clear→update の2段書きだと途中失敗でシート全損＝処理済み記録の喪失リスクがあるため）
  def write_map_rows(sheets, rows)
    values = [MAP_HEADER] + rows.map { |row| MAP_HEADER.map { |column| row[column] } }
    sheets.update_spreadsheet_value(
      spreadsheet_id,
      "#{map_sheet_name}!A1",
      Google::Apis::SheetsV4::ValueRange.new(values: values),
      value_input_option: "RAW"
    )
  end

  # ====================================================
  # Sheets（サービスアカウント認証・AnalyticsWorker と同方式）
  # ====================================================
  def build_sheets_service
    service = Google::Apis::SheetsV4::SheetsService.new
    service.client_options.application_name = "Onclass YouTube LME Landing"
    keyfile = ENV["GOOGLE_APPLICATION_CREDENTIALS"]

    raise "ENV GOOGLE_APPLICATION_CREDENTIALS is not set." if keyfile.nil? || keyfile.strip.empty?
    raise "Service account key not found: #{keyfile}" unless File.exist?(keyfile)

    authorizer = Google::Auth::ServiceAccountCredentials.make_creds(
      json_key_io: File.open(keyfile),
      scope: [Google::Apis::SheetsV4::AUTH_SPREADSHEETS]
    )
    authorizer.fetch_access_token!
    service.authorization = authorizer
    service
  end

  def ensure_sheet_exists!(service, target_spreadsheet_id, sheet_name)
    spreadsheet = service.get_spreadsheet(target_spreadsheet_id)
    return if spreadsheet.sheets.any? { |sheet| sheet.properties&.title == sheet_name }

    add_request = Google::Apis::SheetsV4::AddSheetRequest.new(
      properties: Google::Apis::SheetsV4::SheetProperties.new(title: sheet_name)
    )
    batch = Google::Apis::SheetsV4::BatchUpdateSpreadsheetRequest.new(
      requests: [Google::Apis::SheetsV4::Request.new(add_sheet: add_request)]
    )
    service.batch_update_spreadsheet(target_spreadsheet_id, batch)
  end

  def jp_timestamp
    Time.current.in_time_zone("Asia/Tokyo").strftime("%Y-%m-%d %H:%M:%S")
  end
end
