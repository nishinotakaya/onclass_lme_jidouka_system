# frozen_string_literal: true
# test/services/freelance_jobs/http_fetcher_test.rb
#
# Faradayのテストアダプタで200/404を検証する。sleepは個体にスタブして実際には待たない。

require_relative "../../support/freelance_jobs_loader"
require "faraday"
require "logger"
require "json"

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

  def build_fetcher(stubs, interval: 0, logger: @silent_logger)
    fetcher = TestableFreelanceJobsHttpFetcher.new(interval: interval, logger: logger)
    fetcher.stub_connection = Faraday.new { |builder| builder.adapter :test, stubs }
    fetcher
  end

  # logger.warnに渡された文字列だけを記録するダブル。
  # request_with_retryのログにAPIキー入りのクエリが混入していないかを検証するために使う。
  class RecordingWarnLogger
    attr_reader :warn_messages

    def initialize
      @warn_messages = []
    end

    def warn(message)
      @warn_messages << message
    end
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

  # === D1: ランサーズ本番対象外（WAF CAPTCHA）対応 ===

  def test_get_raises_access_blocked_error_on_405_with_waf_action_header
    stubs = Faraday::Adapter::Test::Stubs.new
    stubs.get("https://www.lancers.jp/work/search/system?vid=0&open=1") do
      [405, { "x-amzn-waf-action" => "captcha" }, "Human Verification"]
    end

    fetcher = build_fetcher(stubs)
    stub_sleep_on(fetcher)

    error = assert_raises(FreelanceJobs::AccessBlockedError) do
      fetcher.get("https://www.lancers.jp/work/search/system?vid=0&open=1")
    end
    assert_includes error.message, "アクセス制限"
    assert_includes error.message, "HTTP 405"
    stubs.verify_stubbed_calls
  end

  def test_access_blocked_error_can_be_rescued_as_fetch_error
    stubs = Faraday::Adapter::Test::Stubs.new
    stubs.get("https://www.lancers.jp/work/search/system?vid=0&open=1") do
      [405, { "x-amzn-waf-action" => "captcha" }, "Human Verification"]
    end

    fetcher = build_fetcher(stubs)
    stub_sleep_on(fetcher)

    rescued_error = nil
    begin
      fetcher.get("https://www.lancers.jp/work/search/system?vid=0&open=1")
    rescue FreelanceJobs::FetchError => error
      rescued_error = error
    end

    assert_kind_of FreelanceJobs::AccessBlockedError, rescued_error,
                   "AccessBlockedErrorはFetchErrorとしてもrescueできる想定"
  end

  def test_get_raises_plain_fetch_error_on_405_without_waf_action_header
    stubs = Faraday::Adapter::Test::Stubs.new
    stubs.get("https://example.com/method-not-allowed") { [405, {}, "method not allowed"] }

    fetcher = build_fetcher(stubs)
    stub_sleep_on(fetcher)

    error = assert_raises(FreelanceJobs::FetchError) { fetcher.get("https://example.com/method-not-allowed") }
    refute_kind_of FreelanceJobs::AccessBlockedError, error,
                   "WAFのチャレンジ応答ヘッダが無ければ405でも通常のFetchErrorになる想定"
    assert_includes error.message, "HTTP 405"
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

  # === post_json: JSONをPOSTする（re:shineのサインインAPI等、GETでは足りない取得元向け） ===
  # getと同じUser-Agent固定・リクエスト間隔sleep・接続エラー時の1回リトライ・非200時のエラー分岐
  # （WAFヘッダの有無でAccessBlockedError/FetchErrorを切り分ける）を共有する想定。

  def test_post_json_sends_json_content_type_header_and_serialized_body
    stubs = Faraday::Adapter::Test::Stubs.new
    seen_body = nil
    seen_headers = nil
    stubs.post("https://example.com/signin") do |env|
      seen_body = env.body
      seen_headers = env.request_headers
      [200, {}, '{"idToken":"stub-id-token"}']
    end

    fetcher = build_fetcher(stubs)
    stub_sleep_on(fetcher)

    body = fetcher.post_json(
      "https://example.com/signin",
      { "email" => "user@example.com", "password" => "dummy-password-for-test" }
    )

    assert_equal '{"idToken":"stub-id-token"}', body
    assert_equal(
      { "email" => "user@example.com", "password" => "dummy-password-for-test" },
      JSON.parse(seen_body)
    )
    assert_equal "application/json", seen_headers["Content-Type"]
    assert_equal FreelanceJobs::HttpFetcher::USER_AGENT, seen_headers["User-Agent"]
    stubs.verify_stubbed_calls
  end

  def test_post_json_merges_custom_headers_on_top_of_defaults
    stubs = Faraday::Adapter::Test::Stubs.new
    seen_headers = nil
    stubs.post("https://example.com/signin") do |env|
      seen_headers = env.request_headers
      [200, {}, "ok"]
    end

    fetcher = build_fetcher(stubs)
    stub_sleep_on(fetcher)
    fetcher.post_json("https://example.com/signin", {}, headers: { "Accept" => "application/json" })

    assert_equal "application/json", seen_headers["Accept"]
    assert_equal "ja,en;q=0.8", seen_headers["Accept-Language"]
  end

  def test_post_json_raises_fetch_error_on_non_200
    stubs = Faraday::Adapter::Test::Stubs.new
    stubs.post("https://example.com/signin") { [400, {}, '{"error":{"message":"INVALID_PASSWORD"}}'] }

    fetcher = build_fetcher(stubs)
    stub_sleep_on(fetcher)

    error = assert_raises(FreelanceJobs::FetchError) do
      fetcher.post_json("https://example.com/signin", { "email" => "user@example.com" })
    end
    assert_includes error.message, "HTTP 400"
    assert_includes error.message, "https://example.com/signin"
  end

  def test_post_json_raises_access_blocked_error_on_waf_action_header
    stubs = Faraday::Adapter::Test::Stubs.new
    stubs.post("https://example.com/signin") do
      [405, { "x-amzn-waf-action" => "captcha" }, "Human Verification"]
    end

    fetcher = build_fetcher(stubs)
    stub_sleep_on(fetcher)

    error = assert_raises(FreelanceJobs::AccessBlockedError) { fetcher.post_json("https://example.com/signin", {}) }
    assert_includes error.message, "アクセス制限"
    assert_includes error.message, "HTTP 405"
  end

  # 例外メッセージにメール・パスワードが写ってログ等に漏れないことを確認する
  # （raise_for_failure!はgetと同じくステータス・URLだけを使う想定）。
  def test_post_json_error_message_does_not_leak_request_body
    stubs = Faraday::Adapter::Test::Stubs.new
    stubs.post("https://example.com/signin") { [401, {}, "unauthorized"] }

    fetcher = build_fetcher(stubs)
    stub_sleep_on(fetcher)

    error = assert_raises(FreelanceJobs::FetchError) do
      fetcher.post_json(
        "https://example.com/signin",
        { "email" => "secret-user@example.com", "password" => "dummy-password-for-test" }
      )
    end

    refute_includes error.message, "secret-user@example.com"
    refute_includes error.message, "dummy-password-for-test"
  end

  def test_post_json_sleeps_interval_before_second_request_but_not_before_first
    stubs = Faraday::Adapter::Test::Stubs.new
    stubs.post("https://example.com/a") { [200, {}, "a"] }
    stubs.post("https://example.com/b") { [200, {}, "b"] }

    fetcher = build_fetcher(stubs, interval: 1.5)
    recorded_sleeps = stub_sleep_on(fetcher)

    fetcher.post_json("https://example.com/a", {})
    assert_equal [], recorded_sleeps, "first request should not sleep"

    fetcher.post_json("https://example.com/b", {})
    assert_equal [1.5], recorded_sleeps, "second request should sleep for the configured interval"
  end

  # getとpost_jsonが同じ@requested_onceを共有し、通算2回目以降のリクエストで間隔sleepが効くことを確認する。
  def test_post_json_shares_request_interval_state_with_get
    stubs = Faraday::Adapter::Test::Stubs.new
    stubs.get("https://example.com/a") { [200, {}, "a"] }
    stubs.post("https://example.com/b") { [200, {}, "b"] }

    fetcher = build_fetcher(stubs, interval: 1.5)
    recorded_sleeps = stub_sleep_on(fetcher)

    fetcher.get("https://example.com/a")
    fetcher.post_json("https://example.com/b", {})

    assert_equal [1.5], recorded_sleeps, "getの後のpost_jsonでも間隔sleepが効くはず"
  end

  def test_post_json_retries_once_on_connection_failed_then_succeeds
    stubs = Faraday::Adapter::Test::Stubs.new
    attempt = 0
    stubs.post("https://example.com/flaky") do
      attempt += 1
      raise Faraday::ConnectionFailed, "boom" if attempt == 1

      [200, {}, "recovered"]
    end

    fetcher = build_fetcher(stubs)
    recorded_sleeps = stub_sleep_on(fetcher)

    assert_equal "recovered", fetcher.post_json("https://example.com/flaky", {})
    assert_equal 2, attempt
    assert_includes recorded_sleeps, FreelanceJobs::HttpFetcher::RETRY_WAIT_SECONDS
  end

  # === AC-07: 例外メッセージ・ログにAPIキー入りURLが出ない ===
  #
  # re:shineのサインインURLはクエリにFirebase APIキーを含む
  # （https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=<APIキー>）。
  # 接続エラー時のリトライwarnログ・非200時の例外メッセージの両方でクエリを落とすことを確認する。

  DUMMY_API_KEY = "dummy-api-key-for-test"
  SIGN_IN_URL_WITHOUT_QUERY = "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword"
  SIGN_IN_URL_WITH_QUERY = "#{SIGN_IN_URL_WITHOUT_QUERY}?key=#{DUMMY_API_KEY}".freeze

  def test_post_json_retry_warn_log_does_not_include_query_with_api_key
    stubs = Faraday::Adapter::Test::Stubs.new
    attempt = 0
    stubs.post(SIGN_IN_URL_WITH_QUERY) do
      attempt += 1
      raise Faraday::ConnectionFailed, "boom" if attempt == 1

      [200, {}, '{"idToken":"stub-id-token"}']
    end

    warn_logger = RecordingWarnLogger.new
    fetcher = build_fetcher(stubs, logger: warn_logger)
    stub_sleep_on(fetcher)

    fetcher.post_json(SIGN_IN_URL_WITH_QUERY, { "email" => "user@example.com" })

    assert_equal 1, warn_logger.warn_messages.size
    warn_message = warn_logger.warn_messages.first
    refute_includes warn_message, "key=", "warnログにクエリ文字列そのもの（key=...）が含まれてはいけない"
    refute_includes warn_message, DUMMY_API_KEY, "warnログにAPIキーの値が含まれてはいけない"
    assert_includes warn_message, SIGN_IN_URL_WITHOUT_QUERY, "warnログにはクエリを落としたURLは残る想定"
  end

  def test_get_fetch_error_message_does_not_include_query_with_api_key
    stubs = Faraday::Adapter::Test::Stubs.new
    stubs.get(SIGN_IN_URL_WITH_QUERY) { [400, {}, "bad request"] }

    fetcher = build_fetcher(stubs)
    stub_sleep_on(fetcher)

    error = assert_raises(FreelanceJobs::FetchError) { fetcher.get(SIGN_IN_URL_WITH_QUERY) }

    refute_includes error.message, "key=", "例外メッセージにクエリ文字列そのもの（key=...）が含まれてはいけない"
    refute_includes error.message, DUMMY_API_KEY, "例外メッセージにAPIキーの値が含まれてはいけない"
    assert_includes error.message, SIGN_IN_URL_WITHOUT_QUERY, "例外メッセージにはクエリを落としたURLは残る想定"
  end

  def test_post_json_fetch_error_message_does_not_include_query_with_api_key
    stubs = Faraday::Adapter::Test::Stubs.new
    stubs.post(SIGN_IN_URL_WITH_QUERY) { [400, {}, '{"error":{"message":"INVALID_PASSWORD"}}'] }

    fetcher = build_fetcher(stubs)
    stub_sleep_on(fetcher)

    error = assert_raises(FreelanceJobs::FetchError) do
      fetcher.post_json(SIGN_IN_URL_WITH_QUERY, { "email" => "user@example.com" })
    end

    refute_includes error.message, "key=", "例外メッセージにクエリ文字列そのもの（key=...）が含まれてはいけない"
    refute_includes error.message, DUMMY_API_KEY, "例外メッセージにAPIキーの値が含まれてはいけない"
    assert_includes error.message, SIGN_IN_URL_WITHOUT_QUERY, "例外メッセージにはクエリを落としたURLは残る想定"
  end

  def test_post_json_access_blocked_error_message_does_not_include_query_with_api_key
    stubs = Faraday::Adapter::Test::Stubs.new
    stubs.post(SIGN_IN_URL_WITH_QUERY) do
      [405, { "x-amzn-waf-action" => "captcha" }, "Human Verification"]
    end

    fetcher = build_fetcher(stubs)
    stub_sleep_on(fetcher)

    error = assert_raises(FreelanceJobs::AccessBlockedError) do
      fetcher.post_json(SIGN_IN_URL_WITH_QUERY, { "email" => "user@example.com" })
    end

    refute_includes error.message, "key=", "WAFブロック時の例外メッセージにクエリ文字列が含まれてはいけない"
    refute_includes error.message, DUMMY_API_KEY, "WAFブロック時の例外メッセージにAPIキーの値が含まれてはいけない"
    assert_includes error.message, SIGN_IN_URL_WITHOUT_QUERY
  end

  def test_get_fetch_error_message_does_not_include_fragment
    url_without_fragment = "https://example.com/callback"
    url_with_fragment = "#{url_without_fragment}#access_token=#{DUMMY_API_KEY}"

    stubs = Faraday::Adapter::Test::Stubs.new
    stubs.get(url_without_fragment) { [500, {}, "server error"] }

    fetcher = build_fetcher(stubs)
    stub_sleep_on(fetcher)

    error = assert_raises(FreelanceJobs::FetchError) { fetcher.get(url_with_fragment) }

    refute_includes error.message, DUMMY_API_KEY, "フラグメントに含まれる値が例外メッセージに出てはいけない"
    refute_includes error.message, "#access_token", "フラグメントそのものが例外メッセージに残ってはいけない"
  end

  # URI.parseに失敗するようなURL（空白入り）は、そもそもFaraday自身がリクエスト構築時に
  # URI::InvalidURIErrorを送出してしまい、raise_for_failure!やrequest_with_retryのwarnまで
  # 到達できない（つまりget/post_json経由では境界ケースを再現できない）。
  # そのためloggable_url（private）を直接呼び出し、クエリの値が漏れないことだけを確認する。
  # 戻り値の厳密な形式（ホスト名かどうか）には依存しない。
  def test_loggable_url_does_not_leak_query_value_when_url_is_unparsable
    unparsable_url_with_query = "http://exa mple.com/a?key=#{DUMMY_API_KEY}"

    fetcher = FreelanceJobs::HttpFetcher.new(logger: @silent_logger)
    loggable = fetcher.send(:loggable_url, unparsable_url_with_query)

    refute_includes loggable, DUMMY_API_KEY, "URI.parseに失敗するURLでもクエリの値が漏れてはいけない"
    refute_includes loggable, "key=", "URI.parseに失敗するURLでもクエリ文字列そのものが漏れてはいけない"
  end

  # URI.parseにもホスト名の取得にも失敗するケース（制御文字入り）では"<invalid url>"になる想定。
  def test_loggable_url_becomes_invalid_url_placeholder_when_totally_unparsable
    totally_unparsable_url = "http://\x00example.com/a?key=#{DUMMY_API_KEY}"

    fetcher = FreelanceJobs::HttpFetcher.new(logger: @silent_logger)
    loggable = fetcher.send(:loggable_url, totally_unparsable_url)

    refute_includes loggable, DUMMY_API_KEY, "解析不能なURLでもクエリの値が漏れてはいけない"
    assert_equal "<invalid url>", loggable,
                 "URI.parseにもホスト名取得にも失敗する場合は<invalid url>になる想定"
  end

  def test_get_fetch_error_message_keeps_url_without_query_unchanged
    plain_url = "https://example.com/plain/path"

    stubs = Faraday::Adapter::Test::Stubs.new
    stubs.get(plain_url) { [404, {}, "not found"] }

    fetcher = build_fetcher(stubs)
    stub_sleep_on(fetcher)

    error = assert_raises(FreelanceJobs::FetchError) { fetcher.get(plain_url) }

    assert_includes error.message, plain_url, "クエリの無いURLはそのまま出るはず"
  end
end
