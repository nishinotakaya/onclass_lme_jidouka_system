# frozen_string_literal: true

require "minitest/autorun"
require "net/smtp"
require "support/freelance_jobs_alert_loader"

class FreelanceJobsAlertNotifierTest < Minitest::Test
  # 送ったメールを記録するだけのメーラー代役。deliver_now まで含めて差し替える。
  class MailerSpy
    Message = Struct.new(:kind, :attributes, :spy) do
      def deliver_now
        raise spy.error_to_raise if spy.error_to_raise

        spy.delivered << attributes.merge(kind: kind)
      end
    end

    attr_reader :delivered
    attr_accessor :error_to_raise

    def initialize
      @delivered = []
    end

    def failed(**attributes)
      Message.new(:failed, attributes, self)
    end

    def stalled(**attributes)
      Message.new(:stalled, attributes, self)
    end
  end

  def setup
    Rails.cache = MemoryCacheStub.new
    @mailer = MailerSpy.new
    # 定数の再代入になるため、警告を出さずに差し替える。
    Object.send(:remove_const, :FreelanceJobsAlertMailer) if Object.const_defined?(:FreelanceJobsAlertMailer)
    Object.const_set(:FreelanceJobsAlertMailer, @mailer)
    @original_smtp_username = ENV["SMTP_USERNAME"]
    ENV["SMTP_USERNAME"] = "batch@example.com"
  end

  def teardown
    ENV.delete("SMTP_USERNAME")
    ENV["SMTP_USERNAME"] = @original_smtp_username if @original_smtp_username
  end

  # 本題1: 中断（aborted）はログにしか残らないので、理由とシートURL入りのメールで知らせる。
  def test_notify_failure_sends_a_mail_with_the_reason_and_the_sheet_url
    result = FreelanceJobs::AlertNotifier.notify_failure(
      profile: engineer_profile,
      reason: "既存シートにヘッダー行(🌟おすすめ)が見つかりません"
    )

    assert_equal :sent, result
    mail = @mailer.delivered.first
    assert_equal :failed, mail[:kind]
    assert_equal engineer_profile.label, mail[:profile_label]
    assert_includes mail[:reason], "ヘッダー行"
    assert_includes mail[:sheet_url], engineer_profile.sheet_gid.to_s
    refute mail[:google_auth_failure], "中断理由だけのときはGoogle認証エラー扱いにしない"
  end

  # 本題2: Googleログインが切れている場合は、文面を切り替えられるよう印を立てる。
  def test_notify_failure_marks_google_authentication_errors
    FreelanceJobs::AlertNotifier.notify_failure(profile: engineer_profile, error: google_auth_error)

    mail = @mailer.delivered.first
    assert mail[:google_auth_failure], "invalid_grant はGoogle認証エラーとして通知する"
    assert_includes mail[:reason], "invalid_grant"
  end

  # サイト側のHTTPエラーまでGoogle認証扱いにすると、文面が嘘になる。
  def test_notify_failure_does_not_mark_unrelated_errors_as_authentication_failures
    FreelanceJobs::AlertNotifier.notify_failure(
      profile: engineer_profile,
      error: FreelanceJobs::FetchError.new("HTTP 503 Service Unavailable")
    )

    refute @mailer.delivered.first[:google_auth_failure]
  end

  # Sidekiqのリトライで何度も失敗するため、同じ原因では1通しか送らない。
  def test_notify_failure_sends_only_one_mail_per_profile
    3.times { FreelanceJobs::AlertNotifier.notify_failure(profile: engineer_profile, reason: "失敗") }

    assert_equal 1, @mailer.delivered.size
    assert_equal :already_sent,
                  FreelanceJobs::AlertNotifier.notify_failure(profile: engineer_profile, reason: "失敗")
  end

  # プロファイルごとに独立して通知する（片方が壊れてももう片方の通知を塞がない）。
  def test_notify_failure_is_tracked_per_profile
    FreelanceJobs::AlertNotifier.notify_failure(profile: engineer_profile, reason: "失敗")

    assert_equal :sent, FreelanceJobs::AlertNotifier.notify_failure(profile: beginner_profile, reason: "失敗")
    assert_equal 2, @mailer.delivered.size
  end

  # 直ったら通知枠を開放する。次に壊れたときはまた1通届く。
  def test_record_success_releases_the_alert_slot
    FreelanceJobs::AlertNotifier.notify_failure(profile: engineer_profile, reason: "失敗")
    FreelanceJobs::AlertNotifier.record_success(profile: engineer_profile)

    assert_equal :sent, FreelanceJobs::AlertNotifier.notify_failure(profile: engineer_profile, reason: "失敗")
    assert_equal 2, @mailer.delivered.size
  end

  # 成功したら最終成功時刻を残す。ウォッチドッグはこれを見る。
  def test_record_success_stores_the_last_success_time
    FreelanceJobs::AlertNotifier.record_success(profile: engineer_profile)

    assert_in_delta Time.current.to_i,
                    FreelanceJobs::AlertNotifier.last_success_at(engineer_profile).to_i, 5
  end

  # 本題3: そもそも走っていない（記録が無い／古い）ことを検知できる。
  def test_running_late_detects_a_batch_that_has_not_run
    assert FreelanceJobs::AlertNotifier.running_late?(engineer_profile, stale_after: 26.hours),
            "成功の記録が無ければ「回っていない」とみなす"

    FreelanceJobs::AlertNotifier.record_success(profile: engineer_profile)
    refute FreelanceJobs::AlertNotifier.running_late?(engineer_profile, stale_after: 26.hours)

    Rails.cache.write("freelance_jobs_last_success:#{engineer_profile.key}", 3.days.ago.iso8601)
    assert FreelanceJobs::AlertNotifier.running_late?(engineer_profile, stale_after: 26.hours)
  end

  # 未実行の通知には「最後に成功した日時」を載せる（いつから止まっているかを知りたいため）。
  def test_notify_stalled_carries_the_last_success_time
    last_success_at = 3.days.ago
    result = FreelanceJobs::AlertNotifier.notify_stalled(profile: engineer_profile,
                                                          last_success_at: last_success_at)

    assert_equal :sent, result
    mail = @mailer.delivered.first
    assert_equal :stalled, mail[:kind]
    assert_equal last_success_at, mail[:last_success_at]
  end

  # SMTPが未設定の環境では送れない。例外にするとバッチ本来のエラーを覆い隠すので戻り値で知らせる。
  def test_notify_failure_does_nothing_without_smtp_settings
    ENV.delete("SMTP_USERNAME")

    assert_equal :not_configured,
                  FreelanceJobs::AlertNotifier.notify_failure(profile: engineer_profile, reason: "失敗")
    assert_empty @mailer.delivered
  end

  # 送信に失敗したら送信済みフラグを戻し、次の検知でもう一度送れるようにする。
  def test_notify_failure_rolls_back_the_sent_flag_when_delivery_fails
    @mailer.error_to_raise = Net::SMTPAuthenticationError.new("535 Authentication failed")

    assert_equal :failed,
                  FreelanceJobs::AlertNotifier.notify_failure(profile: engineer_profile, reason: "失敗")

    @mailer.error_to_raise = nil
    assert_equal :sent,
                  FreelanceJobs::AlertNotifier.notify_failure(profile: engineer_profile, reason: "失敗")
  end

  private

  def engineer_profile
    FreelanceJobs::Profile::ENGINEER
  end

  def beginner_profile
    FreelanceJobs::Profile::BEGINNER
  end

  def google_auth_error
    error_class = Class.new(StandardError)
    Object.const_set(:SignetAuthorizationErrorStub, error_class) unless Object.const_defined?(:SignetAuthorizationErrorStub)
    error_class.new('Authorization failed. Server message: { "error": "invalid_grant" }')
  end
end
