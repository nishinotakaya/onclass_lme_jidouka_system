# frozen_string_literal: true

require "minitest/autorun"
require "net/smtp"
require "support/youtube_oauth_loader"

class YoutubeOauthAlertNotifierTest < Minitest::Test
  # 送ったメールを記録するだけのメーラー代役。deliver_now まで含めて差し替える。
  class MailerSpy
    Message = Struct.new(:authorize_url, :reason, :spy) do
      def deliver_now
        raise spy.error_to_raise if spy.error_to_raise

        spy.delivered << { authorize_url: authorize_url, reason: reason }
      end
    end

    attr_reader :delivered
    attr_accessor :error_to_raise

    def initialize
      @delivered = []
    end

    def expired(authorize_url:, reason:)
      Message.new(authorize_url, reason, self)
    end
  end

  def setup
    Rails.cache = MemoryCacheStub.new
    @mailer = MailerSpy.new
    # 定数の再代入になるため、警告を出さずに差し替える。
    Object.send(:remove_const, :YoutubeOauthMailer) if Object.const_defined?(:YoutubeOauthMailer)
    Object.const_set(:YoutubeOauthMailer, @mailer)
    @original_env = ENV.to_h.slice("SMTP_USERNAME", "APP_BASE_URL", "YOUTUBE_REDIRECT_URI")
    ENV["SMTP_USERNAME"] = "batch@example.com"
    ENV.delete("APP_BASE_URL")
    ENV.delete("YOUTUBE_REDIRECT_URI")
  end

  def teardown
    %w[SMTP_USERNAME APP_BASE_URL YOUTUBE_REDIRECT_URI].each { |key| ENV.delete(key) }
    @original_env.each { |key, value| ENV[key] = value }
  end

  # 本題: invalid_grant を検知したら、再認証URL入りのメールを送る。
  def test_notify_once_sends_a_mail_containing_the_authorize_url
    ENV["YOUTUBE_REDIRECT_URI"] = "https://example.herokuapp.com/oauth2callback"

    result = Youtube::OauthAlertNotifier.notify_once(reason: invalid_grant_reason)

    assert_equal :sent, result
    assert_equal 1, @mailer.delivered.size
    assert_equal "https://example.herokuapp.com/youtube/oauth/authorize", @mailer.delivered.first[:authorize_url]
    assert_includes @mailer.delivered.first[:reason], "invalid_grant"
  end

  # Sidekiqのリトライで何十回も失敗するため、2回目以降は送らない（同じメールが大量に届かないように）。
  def test_notify_once_sends_only_one_mail_until_it_is_reset
    5.times { Youtube::OauthAlertNotifier.notify_once(reason: invalid_grant_reason) }

    assert_equal 1, @mailer.delivered.size
    assert_equal :already_sent, Youtube::OauthAlertNotifier.notify_once(reason: invalid_grant_reason)
  end

  # 再認証が通ったら、次に失効したときにまた1通送れるようにする。
  def test_reset_allows_the_next_alert
    Youtube::OauthAlertNotifier.notify_once(reason: invalid_grant_reason)
    Youtube::OauthAlertNotifier.reset!

    assert_equal :sent, Youtube::OauthAlertNotifier.notify_once(reason: invalid_grant_reason)
    assert_equal 2, @mailer.delivered.size
  end

  # SMTPが未設定の環境では送れない。ここで例外を投げるとバッチ本来のエラーを覆い隠すので、
  # 戻り値で知らせるだけにする。
  def test_notify_once_does_nothing_without_smtp_settings
    ENV.delete("SMTP_USERNAME")

    assert_equal :not_configured, Youtube::OauthAlertNotifier.notify_once(reason: invalid_grant_reason)
    assert_empty @mailer.delivered
  end

  # 送信に失敗したら送信済みフラグを戻し、次の失敗でもう一度送れるようにする。
  def test_notify_once_rolls_back_the_sent_flag_when_delivery_fails
    @mailer.error_to_raise = Net::SMTPAuthenticationError.new("535 Authentication failed")

    assert_equal :failed, Youtube::OauthAlertNotifier.notify_once(reason: invalid_grant_reason)
    refute Rails.cache.key?(Youtube::OauthAlertNotifier::ALERT_SENT_CACHE_KEY),
            "送信できなかった回は送信済みフラグを残さない"

    @mailer.error_to_raise = nil
    assert_equal :sent, Youtube::OauthAlertNotifier.notify_once(reason: invalid_grant_reason)
  end

  # メールの用は再認証URLを届けることなので、ENVが無くてもパスだけのURLは送らない。
  def test_authorize_url_falls_back_to_the_production_app_url
    Youtube::OauthAlertNotifier.notify_once(reason: invalid_grant_reason)

    assert_equal "#{Youtube::OauthAlertNotifier::DEFAULT_APP_BASE_URL}/youtube/oauth/authorize",
                  @mailer.delivered.first[:authorize_url]
  end

  # APP_BASE_URL が優先され、末尾スラッシュがあっても URL が壊れない。
  def test_app_base_url_takes_precedence_and_trailing_slash_is_removed
    ENV["APP_BASE_URL"] = "https://onclass.example.com/"
    ENV["YOUTUBE_REDIRECT_URI"] = "https://ignored.example.com/oauth2callback"

    Youtube::OauthAlertNotifier.notify_once(reason: invalid_grant_reason)

    assert_equal "https://onclass.example.com/youtube/oauth/authorize", @mailer.delivered.first[:authorize_url]
  end

  private

  def invalid_grant_reason
    'Google::Auth::AuthorizationError: Authorization failed. Server message: ' \
    '{ "error": "invalid_grant", "error_description": "Token has been expired or revoked." }'
  end
end
