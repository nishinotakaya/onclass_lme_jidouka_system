# app/jobs/youtube/analytics_worker.rb
class Youtube::AnalyticsWorker
  include Sidekiq::Worker

  def perform
    Rails.logger.info("[YouTubeAnalytics] Start")

    client = Google::YoutubeClient.new
    auth   = client.authorize!

    analytics_service = Google::Apis::YoutubeAnalyticsV2::YouTubeAnalyticsService.new
    analytics_service.authorization = auth

    channel_id = ENV["YOUTUBE_CHANNEL_ID"]

    result = analytics_service.query_report(
      ids:        "channel==MINE",          # ★ 固定で MINE を指定
      start_date: "2024-01-01",
      end_date:   Date.today.to_s,
      metrics:    "views,estimatedMinutesWatched,subscribersGained,subscribersLost",
      dimensions: "day",
      sort:       "day"
    )


    Rails.logger.info("[YouTubeAnalytics] result=#{result.to_h}")
  end
end
