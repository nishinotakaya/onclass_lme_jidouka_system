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
    Google::Apis::YoutubeV3::AUTH_YOUTUBE_FORCE_SSL
    # 必要なら Analytics 用もここに追加
    # Google::Apis::YoutubeAnalyticsV2::AUTH_YT_ANALYTICS_READONLY,
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
    refresh_token = Rails.cache.read("youtube_refresh_token")
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
    # access_token がない → そもそも OAuth 未実施
    if @client.access_token.blank?
      raise <<~MSG
        [YouTubeOAuth] access_token がありません。

        1. Rails を起動
        2. ブラウザで http://localhost:3008/youtube/oauth/authorize にアクセス
        3. Google の同意画面で YouTube へのアクセスを許可

        を実行してから再度 Worker を動かしてください。
      MSG
    end

    # 期限切れ & refresh_token があるときだけ更新する
    if @client.expired? && @client.refresh_token.present?
      new_token = @client.refresh!

      Rails.cache.write("youtube_access_token",  new_token["access_token"])
      Rails.cache.write("youtube_refresh_token", new_token["refresh_token"]) if new_token["refresh_token"]

      if new_token["expires_in"]
        Rails.cache.write("youtube_expires_at", Time.current + new_token["expires_in"].to_i.seconds)
      end
    end

    @client
  end
end
