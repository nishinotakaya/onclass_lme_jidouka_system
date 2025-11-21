# app/controllers/youtube/oauth_controller.rb
class Youtube::OauthController < ApplicationController
  protect_from_forgery except: :callback

  # GET /youtube/oauth/authorize
  def authorize
    client = Google::YoutubeClient.build_for_authorize(
      redirect_uri: ENV["YOUTUBE_REDIRECT_URI"]
    )

    redirect_to client.authorization_uri.to_s, allow_other_host: true
  end

  # GET /oauth2callback  （ENV["YOUTUBE_REDIRECT_URI"] に対応）
  def callback
    if params[:error].present?
      render plain: "OAuth エラー: #{params[:error]}", status: :unauthorized
      return
    end

    if params[:code].blank?
      render plain: "code パラメータがありません", status: :bad_request
      return
    end

    # authorize と同じスコープ / redirect_uri でクライアントを作る
    client = Google::YoutubeClient.build_for_authorize(
      redirect_uri: ENV["YOUTUBE_REDIRECT_URI"]
    )
    client.code = params[:code]

    token = client.fetch_access_token!

    Rails.cache.write("youtube_access_token",  token["access_token"])
    Rails.cache.write("youtube_refresh_token", token["refresh_token"]) if token["refresh_token"]
    Rails.cache.write(
      "youtube_expires_at",
      Time.current + token.fetch("expires_in", 3600).to_i.seconds
    )

    render plain: "認証成功！Worker が YouTube API（コメント含む）を叩けるようになりました。この画面は閉じてOKです。"
  rescue Signet::AuthorizationError => e
    Rails.logger.error("[YouTubeOAuth] AuthorizationError: #{e.message}")
    render plain: "トークン取得でエラーが発生しました: #{e.message}", status: :unauthorized
  end
end
