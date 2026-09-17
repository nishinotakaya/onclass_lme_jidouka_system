# frozen_string_literal: true

# 副業案件バッチの停止を知らせるメール。
# 宛先は運用担当（ENV で上書き可）。差出人は ApplicationMailer が SMTP 認証ユーザーに揃える。
class FreelanceJobsAlertMailer < ApplicationMailer
  DEFAULT_TO = "takaya314boxing@gmail.com"

  # 走ったが失敗・中断した。
  def failed(profile_label:, sheet_url:, reason:, google_auth_failure:)
    @profile_label       = profile_label
    @sheet_url           = sheet_url
    @reason              = reason
    @google_auth_failure = google_auth_failure
    @detected_at         = detected_at
    @sidekiq_url         = sidekiq_url
    @reauthorize_url     = reauthorize_url

    subject = if google_auth_failure
                "【要対応】Googleログインが切れて副業案件バッチが止まっています"
              else
                "【要対応】副業案件バッチが失敗しています（#{profile_label}）"
              end
    mail(to: alert_to, subject: subject)
  end

  # そもそも実行された形跡が無い。
  def stalled(profile_label:, sheet_url:, last_success_at:)
    @profile_label   = profile_label
    @sheet_url       = sheet_url
    @last_success_at = last_success_at&.in_time_zone("Asia/Tokyo")
    @detected_at     = detected_at
    @sidekiq_url     = sidekiq_url

    mail(to: alert_to, subject: "【要対応】副業案件バッチが実行されていません（#{profile_label}）")
  end

  private

  def alert_to
    ENV.fetch("FREELANCE_JOBS_ALERT_TO", DEFAULT_TO)
  end

  def detected_at
    Time.current.in_time_zone("Asia/Tokyo")
  end

  def sidekiq_url
    FreelanceJobs::SheetsClient.sidekiq_web_url
  end

  # Google認証が切れていた場合の入口。YouTube と同じ再認証画面を使う。
  def reauthorize_url
    "#{Youtube::OauthAlertNotifier::DEFAULT_APP_BASE_URL}/youtube/oauth/authorize"
  end
end
