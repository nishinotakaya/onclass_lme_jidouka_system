# lib/google/youtube_client.rb
require "google/apis/youtube_analytics_v2"
require "google/apis/youtube_v3"
require "googleauth"

class Google::YoutubeClient
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
    )

    # expires_at を知っていればセット（nil なら何もしない）
    @client.expires_at = expires_at if expires_at.present?
  end

  def authorize!
    # そもそも access_token が無いなら、まず認証してきてね、というエラー
    if @client.access_token.blank?
      raise "[YouTubeOAuth] access_token がありません。まず http://localhost:3008/youtube/oauth/authorize にブラウザでアクセスして認証してください。"
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
