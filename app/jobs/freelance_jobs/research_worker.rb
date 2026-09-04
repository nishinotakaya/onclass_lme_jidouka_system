# frozen_string_literal: true
# app/jobs/freelance_jobs/research_worker.rb
#
# 副業案件リサーチバッチ（未経験向け HTML/CSS・Excel 案件を6サイトから取得し、
# Googleスプレッドシートを更新する）。毎朝1回だけ実行、メール通知は行わない。

require "json"

class FreelanceJobs::ResearchWorker
  include Sidekiq::Worker
  sidekiq_options queue: "freelance_jobs_research", retry: 1, backtrace: 5, lock: :until_executed

  RUN_WINDOW_LABEL = "毎朝 06:00〜06:30（日本時間）" # scheduler の cron と一致させる（バナー1行目に表示）

  def perform
    summary = FreelanceJobs::ResearchService.new(run_window_label: RUN_WINDOW_LABEL).call
    logger.info("[FreelanceJobs::ResearchWorker] #{summary.to_json}")
  end
end
