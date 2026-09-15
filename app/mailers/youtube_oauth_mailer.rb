# frozen_string_literal: true

class YoutubeOauthMailer < ApplicationMailer
  # 通知先。ENV で上書きできるが、既定は運用担当のアドレス。
  DEFAULT_TO = "takaya314boxing@gmail.com"

  # YouTube の Google OAuth が失効し、バッチが止まったことを知らせる。
  def expired(authorize_url:, reason:)
    @authorize_url = authorize_url
    @reason        = reason
    @detected_at   = Time.current.in_time_zone("Asia/Tokyo")

    mail(
      to: ENV.fetch("YOUTUBE_OAUTH_ALERT_TO", DEFAULT_TO),
      subject: "【要対応】YouTubeバッチ停止 - Google再認証をお願いします"
    )
  end
end
