# lib/google/youtube_client.rb
require "google/apis/youtube_analytics_v2"
require "google/apis/youtube_v3"
require "googleauth"

class Google::YoutubeClient
  # ------------------------------------------------------
  # このクライアントで使うスコープ（★ここがポイント）
  #   - READONLY: 動画一覧など読み取り
  #   - FORCE_SSL: コメント取得などに必要
  # ------------------------------------------------------
  SCOPE = [
    Google::Apis::YoutubeV3::AUTH_YOUTUBE_READONLY,
    Google::Apis::YoutubeV3::AUTH_YOUTUBE_FORCE_SSL,
    # インプレッション数/CTR など reports.query の取得に必要
    Google::Apis::YoutubeAnalyticsV2::AUTH_YT_ANALYTICS_READONLY
  ].freeze

  # ------------------------------------------------------
  # OAuth 開始用クライアントを作るヘルパー
  #   /youtube/oauth/authorize で使う想定
  # ------------------------------------------------------
  def self.build_for_authorize(redirect_uri: ENV["YOUTUBE_REDIRECT_URI"])
    Signet::OAuth2::Client.new(
      client_id:            ENV["YOUTUBE_OAUTH_CLIENT_ID"],
      client_secret:        ENV["YOUTUBE_OAUTH_CLIENT_SECRET"],
      authorization_uri:    "https://accounts.google.com/o/oauth2/auth",
      token_credential_uri: "https://oauth2.googleapis.com/token",
      scope:                SCOPE,
      redirect_uri:         redirect_uri,
      access_type:          "offline",                # リフレッシュトークンをもらう
      include_granted_scopes: "true",                 # 既存スコープに追加
      prompt:               "consent"                 # 毎回同意画面を出したいとき
    )
  end

  # ------------------------------------------------------
  # Worker 等から呼ぶ用：キャッシュ済みトークンで認証するクライアント
  # ------------------------------------------------------
  def initialize
    access_token  = Rails.cache.read("youtube_access_token")
    # refresh_token は永続クレデンシャル。Redis キャッシュが飛んでいても
    # ENV["YOUTUBE_OAUTH_REFRESH_TOKEN"] があればそこから復帰できるようにする
    # （＝一度ブラウザ同意すれば、以降はバッチが自動でアクセストークンを再発行する）。
    refresh_token = Rails.cache.read("youtube_refresh_token").presence ||
                    ENV["YOUTUBE_OAUTH_REFRESH_TOKEN"].presence
    expires_at    = Rails.cache.read("youtube_expires_at")

    @client = Signet::OAuth2::Client.new(
      client_id:            ENV["YOUTUBE_OAUTH_CLIENT_ID"],
      client_secret:        ENV["YOUTUBE_OAUTH_CLIENT_SECRET"],
      token_credential_uri: "https://oauth2.googleapis.com/token",
      access_token:         access_token,
      refresh_token:        refresh_token
      # scope はリフレッシュトークン発行時のものが使われるのでここでは不要
    )

    # expires_at を知っていればセット（nil なら何もしない）
    @client.expires_at = expires_at if expires_at.present?
  end

  # Worker 側から使うメインメソッド
  def authorize!
    # access_token も refresh_token も無い → そもそも OAuth 未実施
    if @client.access_token.blank? && @client.refresh_token.blank?
      raise <<~MSG
        [YouTubeOAuth] access_token も refresh_token もありません。

        1. ブラウザで <APP_URL>/youtube/oauth/authorize にアクセス
        2. Google の同意画面で YouTube へのアクセスを許可
        3. 発行された refresh_token を Heroku config var
           YOUTUBE_OAUTH_REFRESH_TOKEN に設定

        を実行してから再度 Worker を動かしてください。
      MSG
    end

    # access_token が無い or 期限切れで refresh_token がある場合はリフレッシュして
    # アクセストークンを取り直す（refresh_token だけあれば自動復帰できる）。
    if (@client.access_token.blank? || @client.expired?) && @client.refresh_token.present?
      @client.refresh!

      Rails.cache.write("youtube_access_token",  @client.access_token)
      Rails.cache.write("youtube_refresh_token", @client.refresh_token)
      Rails.cache.write("youtube_expires_at",    @client.expires_at) if @client.expires_at
    end

    @client
  end
end
