# app/controllers/youtube/oauth_controller.rb
class Youtube::OauthController < ApplicationController
  protect_from_forgery except: :callback

  # GET /youtube/oauth
  def index
    @access_token = Rails.cache.read("youtube_access_token")
    @expires_at   = Rails.cache.read("youtube_expires_at")

    @authorized =
      @access_token.present? &&
      @expires_at.present? &&
      @expires_at > Time.current

    # 自チャンネル解析用：デフォルト値
    @youtube_sheet_id_default   = ENV["YOUTUBE_ANALYTICS_SPREADSHEET_ID"]
    @youtube_sheet_name_default = ENV["YOUTUBE_ANALYTICS_SHEET_NAME"] || "YouTube"

    # 競合用：デフォルト値
    @competitor_sheet_id_default   = ENV["YOUTUBE_COMPETITORS_SPREADSHEET_ID"] || ENV["YOUTUBE_ANALYTICS_SPREADSHEET_ID"]
    @competitor_sheet_name_default = ENV["YOUTUBE_COMPETITORS_SHEET_NAME"]     || "YouTube競合"

    # 競合フォームの初期値（Worker の定数から生成）
    if defined?(Youtube::CompetitorWorker::COMPETITORS)
      @competitors_text_default =
        Youtube::CompetitorWorker::COMPETITORS
          .map { |c| "#{c[:name]},#{c[:url]}" }
          .join("\n")
    else
      @competitors_text_default = ""
    end
  end

  # GET /youtube/oauth/authorize
  def authorize
    client = Google::YoutubeClient.build_for_authorize(
      redirect_uri: ENV["YOUTUBE_REDIRECT_URI"]
    )

    # redirect_to client.authorization_uri.to_s, allow_other_host: true
    redirect_to youtube_oauth_path
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

    render plain: "認証成功！YouTube バッチが実行できるようになりました。この画面は閉じてOKです。"
  rescue Signet::AuthorizationError => e
    Rails.logger.error("[YouTubeOAuth] AuthorizationError: #{e.message}")
    render plain: "トークン取得でエラーが発生しました: #{e.message}", status: :unauthorized
  end

  # POST /youtube/oauth/run_analytics
  # 自チャンネル（プロアカ）の一覧＆コメントなど
  def run_analytics
    spreadsheet_url  = params[:spreadsheet_url].to_s.strip
    sheet_name       = params[:sheet_name].to_s.strip

    Youtube::AnalyticsWorker.perform_async(
      spreadsheet_url.presence,
      sheet_name.presence
    )

    redirect_to youtube_oauth_path,
                notice: "YouTube 自チャンネル同期（AnalyticsWorker）をキューに積みました。"
  end

  # POST /youtube/oauth/run_competitors
  def run_competitors
    competitors_text       = params[:competitors_text].to_s
    max_videos_per_channel = params[:max_videos_per_channel].to_s
    spreadsheet_url        = params[:spreadsheet_url].to_s.strip
    sheet_name             = params[:sheet_name].to_s.strip

    Youtube::CompetitorWorker.perform_async(
      competitors_text.presence,
      max_videos_per_channel.presence,
      spreadsheet_url.presence,
      sheet_name.presence
    )

    redirect_to youtube_oauth_path,
                notice: "競合チャンネル同期（CompetitorWorker）をキューに積みました。"
  end
end
