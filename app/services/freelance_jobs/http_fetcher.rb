# frozen_string_literal: true

require "faraday"

module FreelanceJobs
  # 案件一覧サイトへのGETリクエストをまとめる。
  # User-Agent固定・タイムアウト・接続エラー時の1回だけの自動リトライ・
  # リクエスト間隔のsleep（サイトへの配慮）を担当する。
  class HttpFetcher
    USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " \
                 "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
    ACCEPT_LANGUAGE = "ja,en;q=0.8"
    RETRY_WAIT_SECONDS = 2

    def initialize(interval: 1.5, timeout: 20, open_timeout: 10, logger: FreelanceJobs.logger)
      @interval = interval
      @timeout = timeout
      @open_timeout = open_timeout
      @logger = logger
      @requested_once = false
    end

    # 通信あり。2回目以降のリクエスト前に interval 秒 sleep する。
    # リダイレクトは追わない（呼び出し側は最終URLを渡すこと）。
    def get(url, headers: {})
      sleep(@interval) if @requested_once
      @requested_once = true

      response = request_with_retry(url, headers)
      raise FreelanceJobs::FetchError, "HTTP #{response.status} #{url}" unless response.status == 200

      response.body
    end

    private

    def request_with_retry(url, headers)
      perform_request(url, headers)
    rescue Faraday::ConnectionFailed, Faraday::TimeoutError => error
      @logger.warn("[FreelanceJobs::HttpFetcher] #{error.class} on #{url}. retrying once...")
      sleep(RETRY_WAIT_SECONDS)
      perform_request(url, headers)
    end

    def perform_request(url, headers)
      connection.get(url) do |request|
        request.headers["User-Agent"] = USER_AGENT
        request.headers["Accept-Language"] = ACCEPT_LANGUAGE
        headers.each { |header_name, header_value| request.headers[header_name] = header_value }
      end
    end

    def connection
      @connection ||= Faraday.new(request: { timeout: @timeout, open_timeout: @open_timeout }) do |builder|
        builder.adapter Faraday.default_adapter
      end
    end
  end
end
