# frozen_string_literal: true
# test/services/freelance_jobs/sources_coconala_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesCoconalaTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 4)

  def test_parse_input_fixture_returns_expected_count_and_first_fields
    body = read_fixture("coconala_input.html")
    postings = FreelanceJobs::Sources::Coconala.parse(body, today: TODAY)

    # fixtureは先頭6タイル中4件が「募集終了」で除外され、2件だけ残る。
    assert_equal 2, postings.size

    first = postings.first
    assert_equal "https://coconala.com/requests/5251556", first.url
    assert_equal "［完全在宅！］ECサイトBUYMAにて簡単な画像加工、入力作業", first.title
    assert_equal "5千円〜1万円", first.reward
    assert_equal "公開依頼（提案制）", first.work_format
    assert_equal "ココナラ（公開依頼）", first.site
    assert_equal "応募者 0人", first.application_status
    assert_instance_of Date, first.deadline_on
    assert_equal Date.new(2026, 9, 11), first.deadline_on
    assert_equal "あと7日（2026-09-11）", first.deadline_text
    assert_equal "ルシファP", first.client
    assert_equal Date.new(2026, 9, 4), first.posted_on
    refute_match(/\s{2,}/, first.description, "descriptionの連続空白は1つに畳まれているはず")
  end

  # --- 除外ルール: 募集終了のタイルは除外される ---

  def test_parse_excludes_tiles_marked_as_closed
    body = read_fixture("coconala_input.html")
    postings = FreelanceJobs::Sources::Coconala.parse(body, today: TODAY)

    excluded_ids = %w[5250586 5250376 5249411 5248544]
    excluded_ids.each do |id|
      refute(postings.any? { |posting| posting.url.end_with?("/#{id}") },
             "id=#{id}は募集終了のため除外されるはず")
    end
  end

  def test_parse_returns_empty_array_when_all_tiles_are_closed
    body = read_fixture("coconala_html.html")
    postings = FreelanceJobs::Sources::Coconala.parse(body, today: TODAY)

    assert_equal [], postings
  end

  def test_parse_returns_empty_array_when_no_tiles_present
    postings = FreelanceJobs::Sources::Coconala.parse("<html><body>該当なし</body></html>", today: TODAY)

    assert_equal [], postings
  end

  # === D2: initialize のオプション（keywords）が fetch に反映される ===

  # urlをそのままキーに本文を返すFakeフェッチャー（呼び出されたURLを記録する）。
  class RecordingFetcher
    def initialize(body_by_url:)
      @body_by_url = body_by_url
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      @body_by_url.fetch(url) { raise "no fixture stubbed for #{url}" }
    end
  end

  def test_fetch_with_custom_keywords_requests_one_page_per_keyword_in_order
    empty_body = "<html><body></body></html>"
    expected_urls = ["Ruby", "TypeScript", "React"].map { |keyword| "https://coconala.com/requests?keyword=#{CGI.escape(keyword)}&page=1" }
    fetcher = RecordingFetcher.new(body_by_url: expected_urls.to_h { |url| [url, empty_body] })
    source = FreelanceJobs::Sources::Coconala.new(fetcher: fetcher, today: TODAY, keywords: ["Ruby", "TypeScript", "React"])

    source.fetch

    assert_equal expected_urls, fetcher.requested_urls
  end

  def test_fetch_with_default_keywords_requests_the_default_keyword_list
    empty_body = "<html><body></body></html>"
    expected_urls = FreelanceJobs::Sources::Coconala::KEYWORDS.map { |keyword| "https://coconala.com/requests?keyword=#{CGI.escape(keyword)}&page=1" }
    fetcher = RecordingFetcher.new(body_by_url: expected_urls.to_h { |url| [url, empty_body] })
    source = FreelanceJobs::Sources::Coconala.new(fetcher: fetcher, today: TODAY)

    source.fetch

    assert_equal expected_urls, fetcher.requested_urls, "keywords省略時は既定のKEYWORDS定数の順で従来通りリクエストする"
  end
end
