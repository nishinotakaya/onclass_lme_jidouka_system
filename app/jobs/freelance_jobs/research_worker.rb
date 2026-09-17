# frozen_string_literal: true
# app/jobs/freelance_jobs/research_worker.rb
#
# 副業案件リサーチバッチ（未経験向け HTML/CSS・Excel、エンジニア向け Ruby・TypeScript・React の
# 案件を各サイトから取得し、Googleスプレッドシートを更新する）。毎朝1回だけ実行する。
# 失敗・中断したときだけ運用担当へメールで知らせる（FreelanceJobs::AlertNotifier）。

require "json"

class FreelanceJobs::ResearchWorker
  include Sidekiq::Worker
  sidekiq_options queue: "freelance_jobs_research", retry: 1, backtrace: 5, lock: :until_executed

  # バナー冒頭に「⏰ 毎朝6時更新」として出る。scheduler の cron と一致させること。
  RUN_WINDOW_LABEL = "毎朝6時"

  # profile_keyがあればそのプロファイルだけ、無ければ全プロファイルを順に実行する。
  # 1プロファイルの失敗が他プロファイルの実行を止めないようそれぞれrescueしてログに記録し、
  # 全プロファイルを終えた後、失敗があれば最初の例外を再raiseしてSidekiqのretryに任せる
  # （成功済みプロファイルの再実行はマージが冪等なので無害）。
  #
  # 中断（aborted）は例外にならずsummaryで返る。ログに残すだけだと誰も気づけないので、
  # 例外と同じく通知する。逆に成功したら最終成功時刻を記録し、通知の抑止を解除する。
  def perform(profile_key = nil)
    profiles = profile_key ? [FreelanceJobs::Profile.find(profile_key)] : FreelanceJobs::Profile.all
    first_error = nil

    profiles.each do |profile|
      begin
        summary = FreelanceJobs::ResearchService.new(profile: profile, run_window_label: RUN_WINDOW_LABEL).call
        logger.info("[FreelanceJobs::ResearchWorker] profile=#{profile.key} #{summary.to_json}")

        if summary[:aborted]
          FreelanceJobs::AlertNotifier.notify_failure(profile: profile, reason: summary[:reason])
        else
          FreelanceJobs::AlertNotifier.record_success(profile: profile)
        end
      rescue StandardError => error
        first_error ||= error
        logger.error("[FreelanceJobs::ResearchWorker] profile=#{profile.key} #{error.class}: #{error.message}")
        FreelanceJobs::AlertNotifier.notify_failure(profile: profile, error: error)
      end
    end

    raise first_error if first_error
  end
end
