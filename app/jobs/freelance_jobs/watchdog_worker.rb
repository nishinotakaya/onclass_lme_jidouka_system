# frozen_string_literal: true
# app/jobs/freelance_jobs/watchdog_worker.rb
#
# 副業案件リサーチバッチが「そもそも走っていない」ことを検知して知らせる見張り番。
#
# ResearchWorker 側の通知は、ジョブが動いて失敗したときにしか出ない。スケジューラが登録
# されていない・worker dyno が寝ている・キューに積まれたまま消えた、といった場合は
# 何も起きないまま毎朝が過ぎる。そこで最終成功時刻を別ジョブから見張る。

class FreelanceJobs::WatchdogWorker
  include Sidekiq::Worker
  sidekiq_options queue: "freelance_jobs_research", retry: 0, lock: :until_executed

  # 前回成功からこれを超えていたら「回っていない」とみなす。
  # 本体は毎朝6時なので、1日分(24h)に実行時間と遅延のぶれ(2h)を足した26時間を閾値にする。
  STALE_AFTER = 26.hours

  def perform
    FreelanceJobs::Profile.all.each do |profile|
      next unless FreelanceJobs::AlertNotifier.running_late?(profile, stale_after: STALE_AFTER)

      last_success_at = FreelanceJobs::AlertNotifier.last_success_at(profile)
      logger.error("[FreelanceJobs::WatchdogWorker] profile=#{profile.key} " \
                   "last_success_at=#{last_success_at || 'none'} バッチが実行されていません")
      FreelanceJobs::AlertNotifier.notify_stalled(profile: profile, last_success_at: last_success_at)
    end
  end
end
