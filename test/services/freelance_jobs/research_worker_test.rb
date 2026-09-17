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

# 通知そのものは alert_notifier_test で検証するので、ここでは「どのプロファイルで何を呼んだか」
# だけを記録する代役を置く（本物はRails.cache・ActionMailerに依存するため）。
module FreelanceJobs
  class AlertNotifier
    class << self
      attr_accessor :calls

      def notify_failure(profile:, reason: nil, error: nil)
        calls << { kind: :failure, profile_key: profile.key, reason: reason, error: error }
      end

      def record_success(profile:)
        calls << { kind: :success, profile_key: profile.key }
      end
    end
  end
end

class FreelanceJobsResearchWorkerTest < Minitest::Test
  def setup
    FreelanceJobs::AlertNotifier.calls = []
  end

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

  # 中断(aborted)は例外にならないため、ログに出るだけで誰も気づけない。必ず通知する。
  def test_perform_notifies_when_a_profile_is_aborted
    aborted_summary = { aborted: true, reason: "既存シートにヘッダー行(🌟おすすめ)が見つかりません" }
    stub_research_service_new(results_by_profile_key: { "beginner" => {}, "engineer" => aborted_summary }) do |_kwargs|
      FreelanceJobs::ResearchWorker.new.perform

      failures = FreelanceJobs::AlertNotifier.calls.select { |call| call[:kind] == :failure }
      assert_equal ["engineer"], failures.map { |call| call[:profile_key] }
      assert_includes failures.first[:reason], "ヘッダー行"
    end
  end

  # 例外で落ちた場合も同じく通知する（例外の中身をそのまま渡す）。
  def test_perform_notifies_when_a_profile_raises
    beginner_error = StandardError.new("beginner boom")
    stub_research_service_new(results_by_profile_key: { "beginner" => beginner_error, "engineer" => {} }) do |_kwargs|
      assert_raises(StandardError) { FreelanceJobs::ResearchWorker.new.perform }

      failure = FreelanceJobs::AlertNotifier.calls.find { |call| call[:kind] == :failure }
      assert_equal "beginner", failure[:profile_key]
      assert_equal beginner_error, failure[:error]
    end
  end

  # 成功したら最終成功時刻を記録する（ウォッチドッグの入力になる）。
  def test_perform_records_success_for_each_succeeded_profile
    stub_research_service_new(results_by_profile_key: { "beginner" => {}, "engineer" => {} }) do |_kwargs|
      FreelanceJobs::ResearchWorker.new.perform

      assert_equal [["beginner", :success], ["engineer", :success]],
                    FreelanceJobs::AlertNotifier.calls.map { |call| [call[:profile_key], call[:kind]] }
    end
  end
end
