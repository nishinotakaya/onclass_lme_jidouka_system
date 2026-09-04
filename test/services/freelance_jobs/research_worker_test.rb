# frozen_string_literal: true
# test/services/freelance_jobs/research_worker_test.rb
#
# FreelanceJobs::ResearchWorker#perform を検証する。
# 実プロジェクトのbundleにはsidekiq gemが無いため、include Sidekiq::Worker /
# sidekiq_options が動く最小限のダミーSidekiq::Workerを先に定義してからワーカー本体をrequireする。
# FreelanceJobs::ResearchService.new は依存注入の口が無いため、Class#newをdefine_singleton_methodで
# 差し替えてスタブする（各テストのensureで必ず元に戻し、他テストへ影響が漏れないようにする）。

require_relative "../../support/freelance_jobs_loader"

unless defined?(Sidekiq::Worker)
  module Sidekiq
    module Worker
      def self.included(base)
        base.extend(ClassMethods)
      end

      module ClassMethods
        def sidekiq_options(*); end
      end

      def logger
        FreelanceJobs.logger
      end
    end
  end
end

require_relative "../../../app/jobs/freelance_jobs/research_worker"

class FreelanceJobsResearchWorkerTest < Minitest::Test
  # ResearchService#callの結果を差し替えるダブル。call_resultがExceptionならそれをraiseし、
  # そうでなければそのまま返す（成功時のsummary Hashを模す）。
  class FakeResearchServiceInstance
    def initialize(call_result:)
      @call_result = call_result
    end

    def call
      raise @call_result if @call_result.is_a?(Exception)

      @call_result
    end
  end

  # FreelanceJobs::ResearchService.new を一時的に差し替え、呼び出し時のkwargsを記録する。
  # results_by_profile_key: プロファイルキーごとの.call結果（Hashなら成功、Exceptionなら失敗）。
  # ブロックにreceived_kwargs（呼び出し順の配列）を渡す。ensureで必ず元のClass#newに戻す。
  def stub_research_service_new(results_by_profile_key:)
    received_kwargs = []
    FreelanceJobs::ResearchService.define_singleton_method(:new) do |**kwargs|
      received_kwargs << kwargs
      FakeResearchServiceInstance.new(call_result: results_by_profile_key.fetch(kwargs[:profile].key))
    end

    yield received_kwargs
  ensure
    begin
      FreelanceJobs::ResearchService.singleton_class.send(:remove_method, :new)
    rescue NameError
      nil
    end
  end

  def test_perform_without_profile_key_runs_all_profiles_in_order
    stub_research_service_new(results_by_profile_key: { "beginner" => {}, "engineer" => {} }) do |received_kwargs|
      FreelanceJobs::ResearchWorker.new.perform

      assert_equal ["beginner", "engineer"], received_kwargs.map { |kwargs| kwargs[:profile].key }
    end
  end

  def test_perform_passes_run_window_label_to_each_research_service
    stub_research_service_new(results_by_profile_key: { "beginner" => {}, "engineer" => {} }) do |received_kwargs|
      FreelanceJobs::ResearchWorker.new.perform

      assert(received_kwargs.all? { |kwargs| kwargs[:run_window_label] == FreelanceJobs::ResearchWorker::RUN_WINDOW_LABEL })
    end
  end

  def test_perform_with_profile_key_runs_only_that_profile
    stub_research_service_new(results_by_profile_key: { "engineer" => {} }) do |received_kwargs|
      FreelanceJobs::ResearchWorker.new.perform("engineer")

      assert_equal ["engineer"], received_kwargs.map { |kwargs| kwargs[:profile].key }
    end
  end

  def test_perform_raises_for_unknown_profile_key_before_calling_research_service
    stub_research_service_new(results_by_profile_key: {}) do |received_kwargs|
      assert_raises(ArgumentError) { FreelanceJobs::ResearchWorker.new.perform("no-such-profile") }

      assert_equal [], received_kwargs, "未知のprofile_keyはProfile.findの時点で例外になり、ResearchServiceは一度も呼ばれない"
    end
  end

  def test_perform_continues_to_second_profile_when_first_profile_raises
    beginner_error = StandardError.new("beginner boom")
    stub_research_service_new(results_by_profile_key: { "beginner" => beginner_error, "engineer" => {} }) do |received_kwargs|
      assert_raises(StandardError) { FreelanceJobs::ResearchWorker.new.perform }

      assert_equal ["beginner", "engineer"], received_kwargs.map { |kwargs| kwargs[:profile].key },
                   "1つ目のプロファイルが例外でも2つ目は実行される"
    end
  end

  def test_perform_reraises_the_error_from_the_failing_profile
    beginner_error = StandardError.new("beginner boom")
    stub_research_service_new(results_by_profile_key: { "beginner" => beginner_error, "engineer" => {} }) do |_received_kwargs|
      error = assert_raises(StandardError) { FreelanceJobs::ResearchWorker.new.perform }

      assert_equal "beginner boom", error.message
    end
  end

  def test_perform_reraises_only_the_first_error_when_both_profiles_fail
    beginner_error = StandardError.new("beginner boom")
    engineer_error = StandardError.new("engineer boom")
    stub_research_service_new(results_by_profile_key: { "beginner" => beginner_error, "engineer" => engineer_error }) do |received_kwargs|
      error = assert_raises(StandardError) { FreelanceJobs::ResearchWorker.new.perform }

      assert_equal "beginner boom", error.message, "最初(beginner)の例外が再raiseされる想定"
      assert_equal ["beginner", "engineer"], received_kwargs.map { |kwargs| kwargs[:profile].key }
    end
  end

  def test_perform_does_not_raise_when_all_profiles_succeed
    stub_research_service_new(results_by_profile_key: { "beginner" => {}, "engineer" => {} }) do |_received_kwargs|
      FreelanceJobs::ResearchWorker.new.perform
    end
  end

  def test_stub_is_fully_restored_after_each_test_so_class_new_is_the_original_class_new
    stub_research_service_new(results_by_profile_key: { "beginner" => {}, "engineer" => {} }) do |_received_kwargs|
      FreelanceJobs::ResearchWorker.new.perform
    end

    assert_equal Class, FreelanceJobs::ResearchService.singleton_class.instance_method(:new).owner,
                 "スタブ解除後はClass#newの委譲に戻っているはず"
  end
end
