# frozen_string_literal: true
# test/services/freelance_jobs/sources_reshine_test.rb
#
# re:shine（https://www.re-shine.jp/）はVite SPAでログイン必須のため、HTMLは読まず
# Firebase(identitytoolkit)でサインインしてidTokenを取得し、api.re-shine.jpのJSON一覧APIを叩く。
# このテストはまだ存在しない FreelanceJobs::Sources::Reshine に対する仕様（Red）テストであり、
# NameError: uninitialized constant で落ちるのが正しい状態。
#
# 秘密情報の扱い: ENVは全テストでサインインごとに退避・復元する。パスワードは
# 明らかにダミーと分かる "dummy-password-for-test" を使い、実際の認証情報は一切書かない。

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"
require "json"

class FreelanceJobsSourcesReshineTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 23)
  FIXTURE_NAME = "reshine_projects_page1.json"
  API_BASE_URL = "https://api.re-shine.jp/projects"

  def setup
    @original_env = {
      "RESHINE_EMAIL" => ENV["RESHINE_EMAIL"],
      "RESHINE_PASSWORD" => ENV["RESHINE_PASSWORD"],
      "RESHINE_FIREBASE_API_KEY" => ENV["RESHINE_FIREBASE_API_KEY"]
    }
    ENV.delete("RESHINE_EMAIL")
    ENV.delete("RESHINE_PASSWORD")
    ENV.delete("RESHINE_FIREBASE_API_KEY")
  end

  def teardown
    @original_env.each do |key, value|
      value.nil? ? ENV.delete(key) : (ENV[key] = value)
    end
  end

  # 取得APIを一切呼ばず、通信を記録するだけのFakeフェッチャー（通信しない）。
  # post_jsonはサインインAPI、getは一覧APIに対応する。
  class FakeFetcher
    attr_reader :post_json_calls, :get_calls

    def initialize(sign_in_response:, list_responses: {})
      @sign_in_response = sign_in_response
      @list_responses = list_responses
      @post_json_calls = []
      @get_calls = []
    end

    def post_json(url, payload, headers: {})
      @post_json_calls << { url: url, payload: payload, headers: headers }
      raise @sign_in_response if @sign_in_response.is_a?(StandardError)

      @sign_in_response
    end

    def get(url, headers: {})
      @get_calls << { url: url, headers: headers }
      response = @list_responses[url]
      raise FreelanceJobs::FetchError, "HTTP 404 #{url}" if response.nil?
      raise response if response.is_a?(StandardError)

      response
    end
  end

  # --- 定数 ---

  def test_site_name_request_interval_and_max_pages_constants
    assert_equal "re:shine", FreelanceJobs::Sources::Reshine::SITE_NAME
    assert_equal 1.5, FreelanceJobs::Sources::Reshine::REQUEST_INTERVAL
    assert_equal 10, FreelanceJobs::Sources::Reshine::MAX_PAGES
  end

  # --- ENV未設定時はサインインすら試みずに即エラー ---

  def test_fetch_raises_when_email_and_password_are_both_missing
    fetcher = FakeFetcher.new(sign_in_response: sign_in_success_body)

    error = assert_raises(FreelanceJobs::FetchError) { build_source(fetcher).fetch }

    assert_equal "RESHINE_EMAIL / RESHINE_PASSWORD / RESHINE_FIREBASE_API_KEY が未設定です", error.message
    assert_equal [], fetcher.post_json_calls, "認証情報が無ければサインインすら試みないはず"
  end

  def test_fetch_raises_when_only_password_is_missing
    ENV["RESHINE_EMAIL"] = "user@example.com"
    fetcher = FakeFetcher.new(sign_in_response: sign_in_success_body)

    error = assert_raises(FreelanceJobs::FetchError) { build_source(fetcher).fetch }

    assert_equal "RESHINE_EMAIL / RESHINE_PASSWORD / RESHINE_FIREBASE_API_KEY が未設定です", error.message
  end

  def test_fetch_raises_when_only_firebase_api_key_is_missing
    ENV["RESHINE_EMAIL"] = "user@example.com"
    ENV["RESHINE_PASSWORD"] = "dummy-password-for-test"
    fetcher = FakeFetcher.new(sign_in_response: sign_in_success_body)

    error = assert_raises(FreelanceJobs::FetchError) { build_source(fetcher).fetch }

    assert_equal "RESHINE_EMAIL / RESHINE_PASSWORD / RESHINE_FIREBASE_API_KEY が未設定です", error.message
    assert_equal [], fetcher.post_json_calls, "認証情報が無ければサインインすら試みないはず"
  end

  def test_fetch_raises_when_password_is_a_blank_string
    set_credentials!(password: "   ")
    fetcher = FakeFetcher.new(sign_in_response: sign_in_success_body)

    assert_raises(FreelanceJobs::FetchError) { build_source(fetcher).fetch }
  end

  # --- サインインは fetch 1回につき1回だけ ---

  def test_sign_in_is_called_only_once_per_fetch_even_with_multiple_pages
    set_credentials!
    fetcher = FakeFetcher.new(
      sign_in_response: sign_in_success_body,
      list_responses: {
        page_url(1) => JSON.generate([build_project("reshine-p1")]),
        page_url(2) => JSON.generate([build_project("reshine-p2")]),
        page_url(3) => "[]"
      }
    )

    build_source(fetcher).fetch

    assert_equal 1, fetcher.post_json_calls.size
  end

  # --- サインインAPIキー: ENVで指定した値がそのまま使われる ---

  def test_sign_in_uses_env_firebase_api_key
    set_credentials!(firebase_api_key: "env-overridden-key-for-test")
    fetcher = FakeFetcher.new(sign_in_response: sign_in_success_body, list_responses: { page_url(1) => "[]" })

    build_source(fetcher).fetch

    signin_url = fetcher.post_json_calls.first[:url]
    assert_includes signin_url, "key=env-overridden-key-for-test"
  end

  # --- サインインのリクエストボディ ---

  def test_sign_in_payload_contains_email_password_and_return_secure_token
    set_credentials!(email: "user@example.com", password: "dummy-password-for-test")
    fetcher = FakeFetcher.new(sign_in_response: sign_in_success_body, list_responses: { page_url(1) => "[]" })

    build_source(fetcher).fetch

    payload = fetcher.post_json_calls.first[:payload]
    assert_equal "user@example.com", payload["email"]
    assert_equal "dummy-password-for-test", payload["password"]
    assert_equal true, payload["returnSecureToken"]
  end

  # --- サインイン失敗時: ステータスだけを含む訳文にし、秘密情報を絶対に含めない ---

  def test_fetch_raises_translated_error_when_sign_in_fails
    set_credentials!(email: "secret-user@example.com", password: "dummy-password-for-test")
    underlying_error = FreelanceJobs::FetchError.new(
      "HTTP 400 https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=" \
      "dummy-firebase-key-for-test"
    )
    fetcher = FakeFetcher.new(sign_in_response: underlying_error)

    error = assert_raises(FreelanceJobs::FetchError) { build_source(fetcher).fetch }

    assert_equal "re:shine のサインインに失敗しました（HTTP 400）", error.message
    refute_includes error.message, "secret-user@example.com"
    refute_includes error.message, "dummy-password-for-test"
    refute_includes error.message, "idToken"
  end

  # --- 一覧APIのヘッダ ---

  def test_list_requests_send_authorization_bearer_and_accept_headers
    set_credentials!
    fetcher = FakeFetcher.new(
      sign_in_response: sign_in_success_body(id_token: "stub-id-token-xyz"),
      list_responses: { page_url(1) => "[]" }
    )

    build_source(fetcher).fetch

    headers = fetcher.get_calls.first[:headers]
    assert_equal "Bearer stub-id-token-xyz", headers["Authorization"]
    assert_equal "application/json", headers["Accept"]
  end

  # --- ページ送り: 空配列が返るまで進め、その先は取りに行かない ---

  def test_fetch_pages_until_an_empty_array_is_returned
    set_credentials!
    fetcher = FakeFetcher.new(
      sign_in_response: sign_in_success_body,
      list_responses: {
        page_url(1) => JSON.generate([build_project("reshine-p1")]),
        page_url(2) => JSON.generate([build_project("reshine-p2")]),
        page_url(3) => "[]"
      }
    )

    postings = build_source(fetcher).fetch

    assert_equal [page_url(1), page_url(2), page_url(3)], fetcher.get_calls.map { |call| call[:url] }
    assert_equal 2, postings.size
  end

  # --- MAX_PAGES == 10 の上限 ---

  def test_fetch_stops_at_max_pages_even_if_page_ten_still_has_content
    set_credentials!
    list_responses = (1..10).each_with_object({}) do |page_number, responses|
      responses[page_url(page_number)] = JSON.generate([build_project("reshine-page#{page_number}")])
    end
    fetcher = FakeFetcher.new(sign_in_response: sign_in_success_body, list_responses: list_responses)

    postings = build_source(fetcher).fetch

    assert_equal 10, fetcher.get_calls.size
    refute_includes fetcher.get_calls.map { |call| call[:url] }, page_url(11)
    assert_equal 10, postings.size
  end

  # --- 除外: status != "public" または published != true ---

  def test_parse_excludes_non_public_status
    postings = FreelanceJobs::Sources::Reshine.parse(
      JSON.generate([build_project("reshine-draft", "status" => "draft")]), today: TODAY
    )

    assert_equal [], postings
  end

  def test_parse_excludes_unpublished_projects
    postings = FreelanceJobs::Sources::Reshine.parse(
      JSON.generate([build_project("reshine-unpub", "published" => false)]), today: TODAY
    )

    assert_equal [], postings
  end

  def test_parse_fixture_excludes_draft_and_unpublished_entries
    postings = FreelanceJobs::Sources::Reshine.parse(read_fixture(FIXTURE_NAME), today: TODAY)
    urls = postings.map(&:url)

    refute_includes urls, "https://www.re-shine.jp/projects/reshine-sample-002/"
    refute_includes urls, "https://www.re-shine.jp/projects/reshine-sample-003/"
  end

  # --- フィールドマッピング（フィクスチャの1件目: reshine-sample-001） ---

  def test_parse_fixture_maps_expected_fields_for_first_posting
    posting = find_fixture_posting("reshine-sample-001")

    refute_nil posting
    assert_equal "re:shine", posting.site
    assert_equal "https://www.re-shine.jp/projects/reshine-sample-001/", posting.url
    assert_equal "【自社開発】バックエンドエンジニア募集（Ruby）", posting.title
    assert_equal "50,000〜70,000円／日", posting.reward
    assert_equal "日額制（業務委託）", posting.work_format
    assert_equal "募集中", posting.application_status
    assert_equal "-", posting.deadline_text
    assert_nil posting.deadline_on
    assert_equal ["Ruby", "Ruby on Rails"], posting.skills
    assert_equal "サンプル株式会社", posting.client
    assert_equal Date.new(2026, 9, 1), posting.posted_on
    assert_includes posting.tags, "自社開発"
    assert_includes posting.tags, "正社員転換相談可"
  end

  def test_parse_fixture_description_includes_expected_parts_for_first_posting
    posting = find_fixture_posting("reshine-sample-001")

    assert_includes posting.description, "職種: バックエンドエンジニア"
    assert_includes posting.description, "必須スキル: Ruby"
    assert_includes posting.description, "Ruby on Rails"
    assert_includes posting.description, "歓迎スキル: AWS"
    assert_includes posting.description, "稼働: 週3〜5日"
    assert_includes posting.description, "リモート: 可"
    assert_includes posting.description, "勤務地: 東京都"
    assert_includes posting.description, "本文:"
    assert_includes posting.description, "Railsを用いた自社サービスの機能開発"
  end

  # --- リモート表記のマッピング: possible→可 / maybe→相談 / impossible→不可 ---

  def test_remote_work_maybe_is_labeled_as_consultation
    posting = find_fixture_posting("reshine-sample-004")

    assert_includes posting.description, "リモート: 相談"
  end

  def test_remote_work_impossible_is_labeled_as_not_possible
    posting = find_fixture_posting("reshine-sample-005")

    assert_includes posting.description, "リモート: 不可"
  end

  # --- min_daily_price が無ければ reward は "要確認" ---

  def test_reward_falls_back_to_kakunin_when_min_daily_price_is_missing
    posting = find_fixture_posting("reshine-sample-004")

    assert_equal "要確認", posting.reward
  end

  # --- in_house_type / transition_recruitment_type の条件を満たさなければタグを付けない ---

  def test_tags_omit_in_house_and_transition_tags_when_conditions_are_not_met
    posting = find_fixture_posting("reshine-sample-004")

    refute_includes posting.tags, "自社開発"
    refute_includes posting.tags, "正社員転換相談可"
  end

  # --- corporationが無ければclientは空文字（nilにしない） ---

  def test_client_is_blank_string_when_corporation_is_missing
    posting = FreelanceJobs::Sources::Reshine.parse(
      JSON.generate([build_project("reshine-no-corp", "corporation" => nil)]), today: TODAY
    ).first

    assert_equal "", posting.client
  end

  # --- URL組み立て: labelをそのまま埋め込む（他ソースと異なりnormalize_urlで末尾スラッシュを落とさない） ---

  def test_url_is_built_from_label_with_trailing_slash
    posting = FreelanceJobs::Sources::Reshine.parse(
      JSON.generate([build_project("abc-DEF-123")]), today: TODAY
    ).first

    assert_equal "https://www.re-shine.jp/projects/abc-DEF-123/", posting.url
  end

  # --- 重複URLは1件に畳む ---

  def test_parse_deduplicates_same_label_within_a_single_page
    postings = FreelanceJobs::Sources::Reshine.parse(
      JSON.generate([build_project("reshine-dup"), build_project("reshine-dup", "name" => "別タイトル")]),
      today: TODAY
    )

    assert_equal 1, postings.size
  end

  def test_fetch_deduplicates_same_project_appearing_on_multiple_pages
    set_credentials!
    fetcher = FakeFetcher.new(
      sign_in_response: sign_in_success_body,
      list_responses: {
        page_url(1) => JSON.generate([build_project("reshine-dup")]),
        page_url(2) => JSON.generate([build_project("reshine-dup", "name" => "別タイトル")]),
        page_url(3) => "[]"
      }
    )

    postings = build_source(fetcher).fetch

    assert_equal 1, postings.size
  end

  private

  def build_source(fetcher)
    FreelanceJobs::Sources::Reshine.new(fetcher: fetcher, today: TODAY)
  end

  def set_credentials!(email: "user@example.com", password: "dummy-password-for-test",
                        firebase_api_key: "dummy-firebase-key-for-test")
    ENV["RESHINE_EMAIL"] = email
    ENV["RESHINE_PASSWORD"] = password
    ENV["RESHINE_FIREBASE_API_KEY"] = firebase_api_key
  end

  def page_url(page_number)
    "#{API_BASE_URL}?page=#{page_number}"
  end

  def sign_in_success_body(id_token: "stub-id-token")
    JSON.generate(
      {
        "idToken" => id_token,
        "email" => "user@example.com",
        "refreshToken" => "stub-refresh-token",
        "expiresIn" => "3600",
        "localId" => "stub-local-id"
      }
    )
  end

  # 一覧API 1要素分の案件Hash（フィクスチャのキー構成を模したもの）。上書きしたいキーだけ渡す。
  def build_project(label, overrides = {})
    {
      "label" => label,
      "status" => "public",
      "approval_status" => "approved",
      "name" => "テスト案件 #{label}",
      "description" => "案件内容\r\nテスト本文です。",
      "in_house_type" => "in_house",
      "transition_recruitment_type" => "can_be_considered",
      "min_daily_price" => 50_000,
      "max_daily_price" => 70_000,
      "min_working_days" => 3,
      "max_working_days" => 5,
      "remote_work" => "possible",
      "job_class" => { "name" => "バックエンドエンジニア" },
      "location" => "東京都",
      "published" => true,
      "required_skills" => [{ "name" => "Ruby" }],
      "desired_skills" => [{ "name" => "AWS" }],
      "display_skill" => { "name" => "Ruby" },
      "corporation" => { "name" => "サンプル株式会社" },
      "published_at" => "2026-09-01T00:00:00.000Z"
    }.merge(overrides)
  end

  def find_fixture_posting(label)
    postings = FreelanceJobs::Sources::Reshine.parse(read_fixture(FIXTURE_NAME), today: TODAY)
    postings.find { |candidate| candidate.url == "https://www.re-shine.jp/projects/#{label}/" }
  end
end
