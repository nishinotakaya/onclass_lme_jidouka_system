# frozen_string_literal: true

module FreelanceJobs
  # 副業案件バッチが止まったことをメールで知らせる。
  #
  # 止まり方は2通りあり、どちらも「気づけないまま毎朝空振りする」のが一番まずい。
  #   1. 走ったが失敗した／中断した（例外・aborted）        → notify_failure（ResearchWorker が呼ぶ）
  #   2. そもそも走っていない（スケジューラ・dyno停止など） → notify_stalled（WatchdogWorker が呼ぶ）
  #
  # 送信の重複抑止は BatchAlerts::OnceNotifier に任せる。成功したら送信枠を開放するので、
  # 「直った→また壊れた」は改めて1通届く。
  class AlertNotifier
    LOG_TAG = "[FreelanceJobsAlert]"

    # 同じ原因の通知を抑える期間。翌朝の実行でまだ直っていなければ、もう1通届く長さにする。
    ALERT_RETENTION = 20.hours

    # 最終成功時刻の保持期間。ウォッチドッグが「いつから止まっているか」を答えるために持つ。
    LAST_SUCCESS_RETENTION = 90.days

    # Google の認証そのものに失敗したときの例外。ネットワーク不調やサイト側の一時障害と区別し、
    # 「Googleログインし直してください」と言い切れる場合だけこの扱いにする。
    GOOGLE_AUTH_ERROR_CLASS_NAMES = %w[
      Signet::AuthorizationError
      Google::Auth::AuthorizationError
      Google::Apis::AuthorizationError
    ].freeze

    # 例外クラスが素の ClientError でも、中身が認証・権限エラーのことがある。
    GOOGLE_AUTH_ERROR_MESSAGE_PATTERN = /invalid_grant|invalid_client|unauthorized|Invalid Credentials|PERMISSION_DENIED/i

    class << self
      # 失敗・中断を検知したときに呼ぶ。reason（中断理由）か error（例外）のどちらかを渡す。
      def notify_failure(profile:, reason: nil, error: nil)
        BatchAlerts::OnceNotifier.deliver_once(
          cache_key: failure_cache_key(profile),
          retention: ALERT_RETENTION,
          log_tag: LOG_TAG,
          description: "#{profile.label} の失敗通知メール"
        ) do
          FreelanceJobsAlertMailer.failed(
            profile_label: profile.label,
            sheet_url: sheet_url(profile),
            reason: failure_reason_text(reason, error),
            google_auth_failure: google_auth_failure?(error)
          )
        end
      end

      # 実行された形跡が無いときに呼ぶ。last_success_at は nil（記録なし）もあり得る。
      def notify_stalled(profile:, last_success_at:)
        BatchAlerts::OnceNotifier.deliver_once(
          cache_key: stalled_cache_key(profile),
          retention: ALERT_RETENTION,
          log_tag: LOG_TAG,
          description: "#{profile.label} の未実行通知メール"
        ) do
          FreelanceJobsAlertMailer.stalled(
            profile_label: profile.label,
            sheet_url: sheet_url(profile),
            last_success_at: last_success_at
          )
        end
      end

      # 成功したときに呼ぶ。最終成功時刻を記録し、失敗・未実行の送信枠を開放する。
      def record_success(profile:)
        Rails.cache.write(last_success_cache_key(profile), Time.current.iso8601,
                          expires_in: LAST_SUCCESS_RETENTION)
        BatchAlerts::OnceNotifier.release_delivery_slot(failure_cache_key(profile))
        BatchAlerts::OnceNotifier.release_delivery_slot(stalled_cache_key(profile))
      end

      # 最終成功時刻（記録が無ければ nil）。
      def last_success_at(profile)
        recorded_at = Rails.cache.read(last_success_cache_key(profile))
        return nil if recorded_at.blank?

        Time.zone.parse(recorded_at.to_s)
      rescue ArgumentError
        nil
      end

      # 前回成功から stale_after を過ぎている（＝回っていない）か。記録が無い場合も「遅れている」扱い。
      def running_late?(profile, stale_after:)
        recorded_at = last_success_at(profile)
        recorded_at.nil? || recorded_at <= stale_after.ago
      end

      # 認証が原因かどうか。文面と件名を「Googleログインし直して」に切り替えるために使う。
      def google_auth_failure?(error)
        return false if error.nil?
        return true if GOOGLE_AUTH_ERROR_CLASS_NAMES.include?(error.class.name)

        error.message.to_s.match?(GOOGLE_AUTH_ERROR_MESSAGE_PATTERN)
      end

      private

      def failure_cache_key(profile)
        "freelance_jobs_alert_failure:#{profile.key}"
      end

      def stalled_cache_key(profile)
        "freelance_jobs_alert_stalled:#{profile.key}"
      end

      def last_success_cache_key(profile)
        "freelance_jobs_last_success:#{profile.key}"
      end

      def failure_reason_text(reason, error)
        return "#{error.class}: #{error.message}" if error

        reason.to_s
      end

      def sheet_url(profile)
        spreadsheet_id = ENV.fetch("FREELANCE_JOBS_SPREADSHEET_ID",
                                    FreelanceJobs::ResearchService::DEFAULT_SPREADSHEET_ID)
        "https://docs.google.com/spreadsheets/d/#{spreadsheet_id}/edit?gid=#{profile.sheet_gid}"
      end
    end
  end
end
