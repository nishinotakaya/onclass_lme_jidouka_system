# frozen_string_literal: true
# test/support/freelance_jobs_loader.rb
#
# FreelanceJobs 関連ファイルを、Railsが起動していない環境でも読み込めるようにするローダー。
# Rails起動済み（bin/rails test 等）ならZeitwerkに任せ、そうでなければ依存順にrequireする。
# app/services/freelance_jobs/** 側は兄弟ファイルをrequireしないため、読み込み順の責務はここに集約する。

if defined?(::Rails) && ::Rails.respond_to?(:application) && ::Rails.application
  # Zeitwerkのオートロードに任せる
else
  require "minitest/autorun"

  root = File.expand_path("../../app", __dir__)
  require "#{root}/services/freelance_jobs"
  %w[
    http_fetcher job_posting classifier row_builder sheet_merger sheets_client
    sources/crowdworks sources/lancers sources/coconala sources/shufti sources/mamaworks sources/craudia
    sources/levtech
    engineer_classifier profile
    research_service
  ].each { |name| require "#{root}/services/freelance_jobs/#{name}" }
end
