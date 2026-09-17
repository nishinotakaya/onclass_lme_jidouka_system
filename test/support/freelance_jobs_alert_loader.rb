# frozen_string_literal: true

# FreelanceJobs::AlertNotifier を Rails なしで読むためのローダー。
# 通知先URLの組み立てに Profile / ResearchService の定数を使うため、本体側のローダーも通す。
require "support/rails_stub_loader"
require "support/freelance_jobs_loader"

require File.expand_path("../../app/services/freelance_jobs/alert_notifier", __dir__)
