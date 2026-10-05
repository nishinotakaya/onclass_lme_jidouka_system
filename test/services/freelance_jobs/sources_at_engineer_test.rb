# frozen_string_literal: true
# test/services/freelance_jobs/sources_at_engineer_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"
require "json"

class FreelanceJobsSourcesAtEngineerTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 10, 5)
  FIXTURE_NAME = "at_engineer_projects_page1.json"
  SOURCE = FreelanceJobs::Sources::AtEngineer
  PAGE1_URL = "https://api.at-engineer.jp/projects/?page=1"
  PAGE2_URL = "https://api.at-engineer.jp/projects/?page=2"

  def parse_fixture(category_hint: nil)
    SOURCE.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  # fixture の projects を書き換えた JSON 本文を返す。
  def fixture_body_with(&block)
    payload = JSON.parse(read_fixture(FIXTURE_NAME))
    block.call(payload["projects"])
    JSON.generate(payload)
  end

  # --- 件数・1件目 ---

  def test_parse_fixture_returns_ten_postings
    assert_equal 10, parse_fixture.size
  end

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "アットエンジニア", first.site
    assert_equal "https://at-engineer.jp/projects/13089", first.url
    assert_equal "リモート有 | 建築システム開発", first.title
    assert_equal "60万円〜70万円", first.reward
    assert_equal "週1〜2日リモートワーク", first.work_format
    assert_equal %w[Java Struts], first.skills
    assert_equal Date.new(2026, 10, 3), first.posted_on
    assert_equal "-", first.application_status
    assert_nil first.category_hint
    assert_nil first.client
  end

  def test_parse_first_posting_tags_and_description
    first = parse_fixture.first

    assert_equal %w[PG 長期 東京都], first.tags.first(3)
    refute_includes first.tags, "週1〜2日リモートワーク", "働き方に使った特徴はtagsに重ねないはず"
    assert_includes first.description, "建築業向けシステム"
    assert_includes first.description, "Java開発経験5年以上"
    assert_includes first.description, "Spring Bootでの開発経験"
  end

  def test_work_format_is_default_when_no_remote_characteristic
    assert_equal "要確認", parse_fixture[1].work_format
  end

  def test_category_hint_is_propagated
    assert(parse_fixture(category_hint: "Java").all? { |posting| posting.category_hint == "Java" })
  end

  # --- 募集終了・審査中 ---

  def test_closed_project_becomes_closed_status
    body = fixture_body_with { |projects| projects[0]["is_closed"] = true }

    first = SOURCE.parse(body, today: TODAY).first

    assert_equal FreelanceJobs::JobPosting::CLOSED_STATUS, first.application_status
    assert first.closed?
  end

  def test_in_review_project_is_excluded
    body = fixture_body_with { |projects| projects[0]["is_in_review"] = true }

    postings = SOURCE.parse(body, today: TODAY)

    assert_equal 9, postings.size
    refute_includes postings.map(&:url), "https://at-engineer.jp/projects/13089"
  end

  def test_invalid_created_at_becomes_nil_posted_on
    body = fixture_body_with { |projects| projects[0]["created_at"] = "not-a-date" }

    assert_nil SOURCE.parse(body, today: TODAY).first.posted_on
  end

  def test_parse_empty_projects_returns_empty_array
    assert_equal [], SOURCE.parse('{"projects":[]}', today: TODAY)
  end

  def test_parse_invalid_json_raises_parser_error
    assert_raises(JSON::ParserError) { SOURCE.parse("<html>", today: TODAY) }
  end

  # --- 定数 ---

  def test_constants
    assert_equal "アットエンジニア", SOURCE::SITE_NAME
    assert_equal 1.5, SOURCE::REQUEST_INTERVAL
    assert_equal "https://api.at-engineer.jp", SOURCE::API_BASE_URL
    assert_equal "https://at-engineer.jp", SOURCE::SITE_BASE_URL
    assert_equal 5, SOURCE::DEFAULT_MAX_PAGES
  end

  # --- fetch ---

  # URL ごとの本文を返し、リクエストURLとヘッダーを記録するフェイク。未登録URLは空の projects。
  class RecordingFetcher
    def initialize(bodies_by_url:)
      @bodies_by_url = bodies_by_url
      @requests = []
    end

    attr_reader :requests

    def get(url, headers: {})
      @requests << { url: url, headers: headers }
      @bodies_by_url.fetch(url, '{"projects":[]}')
    end
  end

  def test_fetch_sends_accept_json_header
    fetcher = RecordingFetcher.new(bodies_by_url: { PAGE1_URL => read_fixture(FIXTURE_NAME) })

    SOURCE.new(fetcher: fetcher, today: TODAY).fetch

    assert(fetcher.requests.all? { |request| request[:headers]["Accept"] == "application/json" })
  end

  def test_fetch_stops_when_a_page_is_empty
    fetcher = RecordingFetcher.new(bodies_by_url: { PAGE1_URL => read_fixture(FIXTURE_NAME) })

    postings = SOURCE.new(fetcher: fetcher, today: TODAY).fetch

    assert_equal [PAGE1_URL, PAGE2_URL], fetcher.requests.map { |request| request[:url] }
    assert_equal 10, postings.size
  end

  def test_fetch_requests_up_to_max_pages_and_deduplicates
    body = read_fixture(FIXTURE_NAME)
    fetcher = RecordingFetcher.new(bodies_by_url: { PAGE1_URL => body, PAGE2_URL => body })

    postings = SOURCE.new(fetcher: fetcher, today: TODAY, max_pages: 2).fetch

    assert_equal 2, fetcher.requests.size
    assert_equal 10, postings.size, "同じ案件が複数ページに出てもURLで重複排除されるはず"
  end
end
