# frozen_string_literal: true
# test/services/freelance_jobs/sources_remogu_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesRemoguTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 10, 5)
  FIXTURE_NAME = "remogu_react.html"
  SOURCE = FreelanceJobs::Sources::Remogu
  REACT_URL = "https://remogu.jp/T12/"

  def parse_fixture(category_hint: "React")
    SOURCE.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  # 一覧カード1件分のHTML片（実データのDOM構造を模したもの）。
  def build_remogu_card_html(href: "/job/1/", title: "テスト案件", reward: "~ <em><strong>800,000</strong> 円</em> ／月")
    anchor = href ? %(<a href="#{href}">#{title}</a>) : title
    <<~HTML
      <dl class="jobCard">
        <dt class="jobTitle"><span>#{anchor}の案件</span></dt>
        <dd class="categories">
          <ul><li class="reward"><span>#{reward}</span></li><li class="place">東京都</li></ul>
          <p class="workStyle"><span><a href="/W1/">フルリモート</a></span></p>
        </dd>
      </dl>
    HTML
  end

  def wrap_remogu_list(fragment)
    wrap_html(%(<ul>#{fragment}</ul>))
  end

  # --- 件数・1件目 ---

  def test_parse_fixture_returns_ten_postings
    assert_equal 10, parse_fixture.size
  end

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "Remogu", first.site
    assert_equal "https://remogu.jp/job/821942942183", first.url
    assert first.title.start_with?("【週5日・CTO/テックリード候補募集】"), first.title
    assert_equal "〜1,300,000円／月", first.reward
    assert_equal "フルリモート", first.work_format
    assert_equal "React", first.category_hint
    assert_equal "-", first.application_status
    assert_nil first.client
    assert_nil first.deadline_on
    assert_nil first.posted_on
    assert_includes first.skills, "AWS"
    assert_includes first.skills, "TypeScript"
    assert_equal first.skills.uniq, first.skills
  end

  def test_parse_first_posting_tags_and_description
    first = parse_fixture.first

    assert_includes first.tags, "CTO/VPoE/テックリード"
    assert_includes first.tags, "全国（フルリモートのため）", "勤務地(li.place)はclientではなくtagsに入るはず"
    assert_includes first.description, "事業部を横断した開発の支援を行う部署です。"
    assert_includes first.description, "必須スキル", "求めるスキルがdescriptionに追記されるはず"
    refute_match(/\n/, first.description)
  end

  def test_urls_are_absolute_job_urls
    parse_fixture.each { |posting| assert_match %r{\Ahttps://remogu\.jp/job/\d+\z}, posting.url }
  end

  # --- 欠けたカード・重複 ---

  def test_parse_skips_cards_without_link
    assert_equal [], SOURCE.parse(wrap_remogu_list(build_remogu_card_html(href: nil)), today: TODAY)
  end

  def test_parse_defaults_reward_when_missing
    html = wrap_remogu_list(build_remogu_card_html.sub(%r{<li class="reward">.*?</li>}m, ""))

    assert_equal "要確認", SOURCE.parse(html, today: TODAY).first.reward
  end

  def test_parse_deduplicates_postings_with_the_same_url
    fragment = build_remogu_card_html(title: "A") + build_remogu_card_html(title: "B")

    postings = SOURCE.parse(wrap_remogu_list(fragment), today: TODAY)

    assert_equal ["A"], postings.map(&:title)
  end

  def test_parse_empty_page_returns_empty_array
    assert_equal [], SOURCE.parse(wrap_remogu_list(""), today: TODAY)
  end

  # --- 定数 ---

  def test_constants
    assert_equal "Remogu", SOURCE::SITE_NAME
    assert_equal 1.5, SOURCE::REQUEST_INTERVAL
    assert_equal "https://remogu.jp", SOURCE::BASE_URL
    assert_equal 2, SOURCE::DEFAULT_PAGES_PER_TARGET
    assert_equal [
      { path: "/T1/", hint: "Ruby" },
      { path: "/T29/", hint: "TypeScript" },
      { path: "/T12/", hint: "React" }
    ], SOURCE::DEFAULT_SEARCH_TARGETS
  end

  # --- fetch ---

  # URL -> body / 例外 のフェイク。未登録URLは空文字（0件ページ）。
  class MapFetcher
    def initialize(bodies_by_url: {}, errors_by_url: {})
      @bodies_by_url = bodies_by_url
      @errors_by_url = errors_by_url
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      raise @errors_by_url[url] if @errors_by_url.key?(url)

      @bodies_by_url.fetch(url, "")
    end
  end

  def build_source(fetcher, targets: [{ path: "/T12/", hint: "React" }], pages: 2)
    SOURCE.new(fetcher: fetcher, today: TODAY, search_targets: targets, pages_per_target: pages)
  end

  def test_fetch_builds_bare_first_page_and_page_query
    body = read_fixture(FIXTURE_NAME)
    fetcher = MapFetcher.new(bodies_by_url: { REACT_URL => body, "#{REACT_URL}?page=2" => body })

    build_source(fetcher).fetch

    assert_equal [REACT_URL, "#{REACT_URL}?page=2"], fetcher.requested_urls
  end

  def test_fetch_stops_when_a_page_has_no_cards
    fetcher = MapFetcher.new(bodies_by_url: { REACT_URL => read_fixture(FIXTURE_NAME) })

    postings = build_source(fetcher, pages: 3).fetch

    assert_equal 2, fetcher.requested_urls.size
    assert_equal 10, postings.size
  end

  def test_fetch_treats_404_on_later_page_as_end_of_pages
    fetcher = MapFetcher.new(
      bodies_by_url: { REACT_URL => read_fixture(FIXTURE_NAME) },
      errors_by_url: { "#{REACT_URL}?page=2" => FreelanceJobs::FetchError.new("HTTP 404 #{REACT_URL}?page=2") }
    )

    postings = build_source(fetcher, pages: 3).fetch

    assert_equal 10, postings.size
    assert_equal 2, fetcher.requested_urls.size
  end

  def test_fetch_raises_when_first_page_is_404
    fetcher = MapFetcher.new(errors_by_url: { REACT_URL => FreelanceJobs::FetchError.new("HTTP 404 #{REACT_URL}") })

    assert_raises(FreelanceJobs::FetchError) { build_source(fetcher).fetch }
  end

  def test_fetch_deduplicates_across_targets_and_keeps_first_hint
    body = read_fixture(FIXTURE_NAME)
    fetcher = MapFetcher.new(bodies_by_url: { "https://remogu.jp/T1/" => body, REACT_URL => body })
    targets = [{ path: "/T1/", hint: "Ruby" }, { path: "/T12/", hint: "React" }]

    postings = build_source(fetcher, targets: targets, pages: 1).fetch

    assert_equal 10, postings.size
    assert(postings.all? { |posting| posting.category_hint == "Ruby" })
  end
end
