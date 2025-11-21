# app/controllers/youtube/oauth_controller.rb
class Youtube::OauthController < ApplicationController
  # CSRF 回りが邪魔なら dev では一旦無効化してもOK
  protect_from_forgery except: :callback

  def authorize
    client = Signet::OAuth2::Client.new(
      client_id: ENV["YOUTUBE_OAUTH_CLIENT_ID"],
      client_secret: ENV["YOUTUBE_OAUTH_CLIENT_SECRET"],
      authorization_uri: "https://accounts.google.com/o/oauth2/auth",
      token_credential_uri: "https://oauth2.googleapis.com/token",
      redirect_uri: ENV["YOUTUBE_REDIRECT_URI"],
      scope: [
        "https://www.googleapis.com/auth/youtube.readonly",
        "https://www.googleapis.com/auth/yt-analytics.readonly"
      ],
      access_type: "offline",   # refresh_token をもらうために必須
      prompt: "consent"         # 毎回 consent 画面を出して refresh_token を取り直せる
    )

    redirect_to client.authorization_uri.to_s, allow_other_host: true
  end

  def callback
    if params[:error].present?
      render plain: "OAuth エラー: #{params[:error]}", status: :unauthorized
      return
    end

    if params[:code].blank?
      render plain: "code パラメータがありません", status: :bad_request
      return
    end

    client = Signet::OAuth2::Client.new(
      client_id: ENV["YOUTUBE_OAUTH_CLIENT_ID"],
      client_secret: ENV["YOUTUBE_OAUTH_CLIENT_SECRET"],
      token_credential_uri: "https://oauth2.googleapis.com/token",
      redirect_uri: ENV["YOUTUBE_REDIRECT_URI"],
      code: params[:code]
    )

    # ここで Google に code を投げて access_token / refresh_token を取得
    token = client.fetch_access_token!

    # ---- ここ超重要：Redis に保存 ----
    Rails.cache.write("youtube_access_token",  token["access_token"])
    Rails.cache.write("youtube_refresh_token", token["refresh_token"]) if token["refresh_token"]
    Rails.cache.write(
      "youtube_expires_at",
      Time.current + token.fetch("expires_in", 3600).to_i.seconds
    )

    render plain: "認証成功！Worker がAPIを叩けるようになりました。"
  rescue Signet::AuthorizationError => e
    Rails.logger.error("[YouTubeOAuth] AuthorizationError: #{e.message}")
    render plain: "トークン取得でエラーが発生しました: #{e.message}", status: :unauthorized
  end
end
