# frozen_string_literal: true
# test/services/freelance_jobs/http_fetcher_test.rb
#
# Faradayのテストアダプタで200/404を検証する。sleepは個体にスタブして実際には待たない。

require_relative "../../support/freelance_jobs_loader"
require "faraday"
require "logger"

# HttpFetcherの`connection`は private かつ Faraday.default_adapter に依存しているため、
# テストではその1メソッドだけを差し替えて Faraday::Adapter::Test に接続する。
class TestableFreelanceJobsHttpFetcher < FreelanceJobs::HttpFetcher
  attr_writer :stub_connection

  private

  def connection
    @stub_connection
  end
end

class FreelanceJobsHttpFetcherTest < Minitest::Test
  def setup
    @silent_logger = Logger.new(File::NULL)
  end

  def build_fetcher(stubs, interval: 0)
    fetcher = TestableFreelanceJobsHttpFetcher.new(interval: interval, logger: @silent_logger)
    fetcher.stub_connection = Faraday.new { |builder| builder.adapter :test, stubs }
    fetcher
  end

  # sleepの呼び出しを記録しつつ、実際には待たないようにインスタンス単位でスタブする。
  def stub_sleep_on(fetcher)
    recorded = []
    fetcher.define_singleton_method(:sleep) { |seconds| recorded << seconds }
    recorded
  end

  def test_get_returns_body_on_http_200
    stubs = Faraday::Adapter::Test::Stubs.new
    stubs.get("https://example.com/ok") { [200, { "Content-Type" => "text/html" }, "<html>hello</html>"] }

    fetcher = build_fetcher(stubs)
    stub_sleep_on(fetcher)

    assert_equal "<html>hello</html>", fetcher.get("https://example.com/ok")
    stubs.verify_stubbed_calls
  end

  def test_get_raises_fetch_error_on_http_404
    stubs = Faraday::Adapter::Test::Stubs.new
    stubs.get("https://example.com/missing") { [404, {}, "not found"] }

    fetcher = build_fetcher(stubs)
    stub_sleep_on(fetcher)

    error = assert_raises(FreelanceJobs::FetchError) { fetcher.get("https://example.com/missing") }
    assert_includes error.message, "HTTP 404"
    assert_includes error.message, "https://example.com/missing"
    stubs.verify_stubbed_calls
  end

  def test_get_sends_default_headers_and_merges_custom_headers
    stubs = Faraday::Adapter::Test::Stubs.new
    seen_headers = nil
    stubs.get("https://example.com/headers") do |env|
      seen_headers = env.request_headers
      [200, {}, "ok"]
    end

    fetcher = build_fetcher(stubs)
    stub_sleep_on(fetcher)
    fetcher.get("https://example.com/headers", headers: { "Accept" => "application/json", "Origin" => "https://app.example" })

    assert_equal FreelanceJobs::HttpFetcher::USER_AGENT, seen_headers["User-Agent"]
    assert_equal "ja,en;q=0.8", seen_headers["Accept-Language"]
    assert_equal "application/json", seen_headers["Accept"]
    assert_equal "https://app.example", seen_headers["Origin"]
  end

  def test_get_sleeps_interval_before_second_request_but_not_before_first
    stubs = Faraday::Adapter::Test::Stubs.new
    stubs.get("https://example.com/a") { [200, {}, "a"] }
    stubs.get("https://example.com/b") { [200, {}, "b"] }

    fetcher = build_fetcher(stubs, interval: 1.5)
    recorded_sleeps = stub_sleep_on(fetcher)

    fetcher.get("https://example.com/a")
    assert_equal [], recorded_sleeps, "first request should not sleep"

    fetcher.get("https://example.com/b")
    assert_equal [1.5], recorded_sleeps, "second request should sleep for the configured interval"
  end

  def test_get_retries_once_on_connection_failed_then_succeeds
    stubs = Faraday::Adapter::Test::Stubs.new
    attempt = 0
    stubs.get("https://example.com/flaky") do
      attempt += 1
      raise Faraday::ConnectionFailed, "boom" if attempt == 1

      [200, {}, "recovered"]
    end

    fetcher = build_fetcher(stubs)
    recorded_sleeps = stub_sleep_on(fetcher)

    assert_equal "recovered", fetcher.get("https://example.com/flaky")
    assert_equal 2, attempt
    assert_includes recorded_sleeps, FreelanceJobs::HttpFetcher::RETRY_WAIT_SECONDS
  end

  def test_get_raises_fetch_error_after_retry_still_fails
    stubs = Faraday::Adapter::Test::Stubs.new
    stubs.get("https://example.com/down") { raise Faraday::ConnectionFailed, "boom" }

    fetcher = build_fetcher(stubs)
    stub_sleep_on(fetcher)

    assert_raises(Faraday::ConnectionFailed) { fetcher.get("https://example.com/down") }
  end
end
