# frozen_string_literal: true

require "faraday"
require "json"
require "uri"

module FreelanceJobs
  # 案件一覧サイトへのGET/POSTリクエストをまとめる。
  # User-Agent固定・タイムアウト・接続エラー時の1回だけの自動リトライ・
  # リクエスト間隔のsleep（サイトへの配慮）を担当する。
  class HttpFetcher
    USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " \
                 "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
    ACCEPT_LANGUAGE = "ja,en;q=0.8"
    RETRY_WAIT_SECONDS = 2
    WAF_ACTION_HEADER = "x-amzn-waf-action"
    # loggable_urlでURI.parseに失敗したURLからホスト名だけを拾うための正規表現。
    # ホスト名に使われうる文字（英数字・ハイフン・ドット・コロン(ポート)）だけを許可し、
    # 空白や制御文字が混じっている箇所より先には進まない。
    HOST_NAME_PATTERN = %r{\Ahttps?://([A-Za-z0-9\-.:]+)}

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
      wait_for_request_interval!

      response = request_with_retry(url) { perform_get(url, headers) }
      raise_for_failure!(response, url) unless response.status == 200

      response.body
    end

    # 通信あり。JSONをPOSTする（re:shineのサインインAPI等、GETでは足りない取得元向け）。
    # interval・User-Agent固定・接続エラー時の1回リトライ・非200時のエラー分岐はgetと共有する。
    def post_json(url, payload, headers: {})
      wait_for_request_interval!

      response = request_with_retry(url) { perform_post_json(url, payload, headers) }
      raise_for_failure!(response, url) unless response.status == 200

      response.body
    end

    private

    # get/post_json共通。2回目以降のリクエスト前にだけ interval 秒 sleep する。
    def wait_for_request_interval!
      sleep(@interval) if @requested_once
      @requested_once = true
    end

    # 非200時のエラー分岐。WAFのチャレンジ応答ヘッダがあればAccessBlockedError（リトライ不可）、
    # 無ければ従来どおりFetchErrorにする。get/post_jsonで共有し、リクエスト本文（メール・パスワード等）は
    # 一切含めない（ステータスとURLだけを使う。URLはloggable_urlでクエリ・フラグメントを落としたものを使う）。
    def raise_for_failure!(response, url)
      waf_action = response.headers[WAF_ACTION_HEADER]
      if waf_action
        raise FreelanceJobs::AccessBlockedError,
              "アクセス制限（WAF #{waf_action}）HTTP #{response.status} #{loggable_url(url)}"
      end

      raise FreelanceJobs::FetchError, "HTTP #{response.status} #{loggable_url(url)}"
    end

    # get/post_json共通のリトライ。接続エラー・タイムアウトのときだけ1回だけ取り直す。
    # ログに出すURLはloggable_urlでクエリ・フラグメントを落としたものを使う。
    def request_with_retry(url)
      yield
    rescue Faraday::ConnectionFailed, Faraday::TimeoutError => error
      @logger.warn("[FreelanceJobs::HttpFetcher] #{error.class} on #{loggable_url(url)}. retrying once...")
      sleep(RETRY_WAIT_SECONDS)
      yield
    end

    # ログ・例外メッセージに出すためのURL。クエリ文字列やフラグメントをそのまま出すと、
    # re:shineのサインインURL（`?key=<Firebase APIキー>`）のような秘匿情報入りの値が
    # ログ・例外メッセージに残ってしまうため、クエリ・フラグメントを落とした形にする。
    def loggable_url(url)
      # URI.parseが通る＝妥当なURLだと分かっているケースでは、再構築による表記ゆれを避けるため
      # 文字列操作でクエリ・フラグメントだけを落とす（クエリの無いURLはそのまま返る）。
      URI.parse(url)
      url.split(/[?#]/, 2).first
    rescue URI::InvalidURIError
      host_name = url[HOST_NAME_PATTERN, 1]
      host_name || "<invalid url>"
    end

    def perform_get(url, headers)
      connection.get(url) { |request| decorate_request(request, headers) }
    end

    def perform_post_json(url, payload, headers)
      connection.post(url) do |request|
        decorate_request(request, headers)
        request.headers["Content-Type"] = "application/json"
        request.body = JSON.generate(payload)
      end
    end

    # get/post_json共通のヘッダ付与。User-Agent・Accept-Languageを固定し、呼び出し側の
    # headers（Authorization等）を上乗せする。
    def decorate_request(request, headers)
      request.headers["User-Agent"] = USER_AGENT
      request.headers["Accept-Language"] = ACCEPT_LANGUAGE
      headers.each { |header_name, header_value| request.headers[header_name] = header_value }
    end

    def connection
      @connection ||= Faraday.new(request: { timeout: @timeout, open_timeout: @open_timeout }) do |builder|
        builder.adapter Faraday.default_adapter
      end
    end
  end
end
