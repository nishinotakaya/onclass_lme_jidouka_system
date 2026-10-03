# frozen_string_literal: true
# test/services/freelance_jobs/sources_workship_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesWorkshipTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 10, 3)
  FIXTURE_NAME = "workship_ruby.html"
  SOURCE = FreelanceJobs::Sources::Workship

  def parse_fixture(category_hint: "Ruby")
    SOURCE.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  # 一覧カード1件分のHTML片（実データのDOM構造を模したもの）。
  def build_workship_card_html(href: "/portal/sample-company/job/1", title: "テスト案件",
                               profession: "バックエンドエンジニア", description: "テストの業務内容です。",
                               client: "株式会社テスト")
    <<~HTML
      <li class="projects_item">
        <a href="#{href}">
          <h3 class="projects_item_ttl">#{title}</h3>
          <div class="projects_item_profession"><span>#{profession}</span></div>
          <p class="projects_item_description">#{description}</p>
        </a>
        <div class="projects_item_icon">
          <a href="/portal/sample-company"><span class="name">#{client}</span></a>
        </div>
      </li>
    HTML
  end

  def wrap_workship_list(fragment)
    wrap_html(%(<ul class="projects">#{fragment}</ul>))
  end

  # --- 件数 ---

  def test_parse_fixture_returns_twenty_postings
    assert_equal 20, parse_fixture.size
  end

  # --- 1件目の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "Workship", first.site
    assert_equal "https://goworkship.com/portal/neontetra/job/208", first.url
    assert_equal "【React,Rails,GraphQL】toCコミュニティサービスのエンジニア募集！【リモート可】", first.title
    assert_equal "株式会社ネオンテトラ", first.client
    assert_equal "Ruby", first.category_hint
    assert_equal ["バックエンドエンジニア"], first.tags, "職種(.projects_item_profession span)がtagsに入るはず"
    assert_equal "要確認", first.reward, "一覧に単価が無いため固定値のはず"
    assert_equal "業務委託（フリーランス）", first.work_format
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_equal [], first.skills
    assert_nil first.posted_on
  end

  def test_parse_first_posting_description
    description = parse_fixture.first.description

    assert_includes description, "趣味をベースとしたtoCのコミュニティサービスを作っています。"
    assert_includes description, "チームは、スタートアップ役員やメガベンチャーで勤務している複業のエンジニア"
    refute_match(/\n/, description, "normalize_descriptionで改行が畳まれているはず")
  end

  def test_urls_are_absolute_portal_job_urls
    parse_fixture.each do |posting|
      assert_match %r{\Ahttps://goworkship\.com/portal/[^/]+/job/\d+\z}, posting.url
    end
  end

  def test_category_hint_is_propagated_to_every_posting
    postings = parse_fixture(category_hint: "React")

    assert(postings.all? { |posting| posting.category_hint == "React" })
  end

  # 会社ページへのリンク(/portal/<slug>)ではなく、案件リンクの方を拾う。
  def test_url_uses_first_portal_link_in_card
    posting = SOURCE.parse(wrap_workship_list(build_workship_card_html(href: "/portal/abc/job/77")),
                           today: TODAY, category_hint: "Ruby").first

    assert_equal "https://goworkship.com/portal/abc/job/77", posting.url
    assert_equal "株式会社テスト", posting.client
  end

  # 会社リンク(/portal/<slug>)がカード内で案件リンクより先に来ても、案件パス(/job/)のリンクだけを採る。
  # 会社リンクしか無いカードは案件URLが取れないため除外する。
  def test_url_requires_job_path_even_when_company_link_comes_first
    fragment = <<~HTML
      <li class="projects_item">
        <a href="/portal/first-company"><span class="name">先に来る会社リンク</span></a>
        <a href="/portal/first-company/job/5"><h3 class="projects_item_ttl">会社リンクが先のカード</h3></a>
      </li>
      <li class="projects_item">
        <a href="/portal/only-company"><h3 class="projects_item_ttl">会社リンクだけのカード</h3></a>
      </li>
    HTML

    postings = SOURCE.parse(wrap_workship_list(fragment), today: TODAY, category_hint: "Ruby")

    assert_equal ["https://goworkship.com/portal/first-company/job/5"], postings.map(&:url)
  end

  # --- 欠けたカード・重複 ---

  def test_parse_skips_cards_without_link_or_title
    fragment = <<~HTML
      <li class="projects_item"><h3 class="projects_item_ttl">リンクなし</h3></li>
    HTML
    fragment += build_workship_card_html(title: "")

    assert_equal [], SOURCE.parse(wrap_workship_list(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_deduplicates_postings_with_the_same_url
    fragment = build_workship_card_html(href: "/portal/x/job/1", title: "A") +
               build_workship_card_html(href: "/portal/x/job/1", title: "B")

    postings = SOURCE.parse(wrap_workship_list(fragment), today: TODAY, category_hint: "Ruby")

    assert_equal 1, postings.size
    assert_equal "A", postings.first.title
  end

  def test_parse_empty_page_returns_empty_array
    assert_equal [], SOURCE.parse(wrap_workship_list(""), today: TODAY, category_hint: "Ruby")
  end

  # --- 定数 ---

  def test_constants
    assert_equal "Workship", SOURCE::SITE_NAME
    assert_equal "https://goworkship.com", SOURCE::BASE_URL
    assert_equal 1.5, SOURCE::REQUEST_INTERVAL
    assert_equal 3, SOURCE::DEFAULT_PAGES_PER_KEYWORD
    assert_equal [
      { keyword: "ruby", hint: "Ruby" },
      { keyword: "typescript", hint: "TypeScript" },
      { keyword: "react", hint: "React" }
    ], SOURCE::DEFAULT_SEARCH_TARGETS
  end

  # --- fetch ---

  class RecordingFetcher
    def initialize(body:)
      @body = body
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      @body
    end
  end

  # URL -> body のHashで返すフェイク。未登録URLは空文字（0件ページ）。
  class UrlMapFetcher
    def initialize(bodies_by_url:)
      @bodies_by_url = bodies_by_url
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      @bodies_by_url.fetch(url, "")
    end
  end

  def test_fetch_builds_keyword_url_and_bare_first_page
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    source = SOURCE.new(fetcher: fetcher, today: TODAY,
                        search_targets: [{ keyword: "ruby", hint: "Ruby" }], pages_per_keyword: 2)

    source.fetch

    assert_equal [
      "https://goworkship.com/portal/keyword-ruby",
      "https://goworkship.com/portal/keyword-ruby?page=2"
    ], fetcher.requested_urls
  end

  def test_fetch_requests_default_three_pages_per_keyword
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    source = SOURCE.new(fetcher: fetcher, today: TODAY,
                        search_targets: [{ keyword: "react", hint: "React" }])

    source.fetch

    assert_equal [
      "https://goworkship.com/portal/keyword-react",
      "https://goworkship.com/portal/keyword-react?page=2",
      "https://goworkship.com/portal/keyword-react?page=3"
    ], fetcher.requested_urls
  end

  def test_fetch_stops_paging_when_a_page_has_no_cards
    fetcher = UrlMapFetcher.new(bodies_by_url: {
      "https://goworkship.com/portal/keyword-ruby" => read_fixture(FIXTURE_NAME)
    })
    source = SOURCE.new(fetcher: fetcher, today: TODAY,
                        search_targets: [{ keyword: "ruby", hint: "Ruby" }], pages_per_keyword: 3)

    postings = source.fetch

    assert_equal 2, fetcher.requested_urls.size, "0件の2ページ目で打ち切り、3ページ目は取らないはず"
    assert_equal 20, postings.size
  end

  # 指定URLで例外を投げ、それ以外は bodies_by_url の本文を返すフェイク。
  class RaisingFetcher
    def initialize(bodies_by_url:, errors_by_url:)
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

  def test_fetch_treats_404_on_later_page_as_end_of_pages
    first_url = "https://goworkship.com/portal/keyword-ruby"
    fetcher = RaisingFetcher.new(
      bodies_by_url: { first_url => read_fixture(FIXTURE_NAME) },
      errors_by_url: { "#{first_url}?page=2" => FreelanceJobs::FetchError.new("HTTP 404 #{first_url}") }
    )
    source = SOURCE.new(fetcher: fetcher, today: TODAY,
                        search_targets: [{ keyword: "ruby", hint: "Ruby" }], pages_per_keyword: 3)

    postings = source.fetch

    assert_equal 20, postings.size
    assert_equal 2, fetcher.requested_urls.size, "2ページ目の404で終端とみなし、3ページ目は取らないはず"
  end

  def test_fetch_raises_when_first_page_is_404
    first_url = "https://goworkship.com/portal/keyword-ruby"
    fetcher = RaisingFetcher.new(
      bodies_by_url: {},
      errors_by_url: { first_url => FreelanceJobs::FetchError.new("HTTP 404 #{first_url}") }
    )
    source = SOURCE.new(fetcher: fetcher, today: TODAY,
                        search_targets: [{ keyword: "ruby", hint: "Ruby" }], pages_per_keyword: 3)

    assert_raises(FreelanceJobs::FetchError) { source.fetch }
  end

  def test_fetch_deduplicates_across_keywords_and_pages
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    source = SOURCE.new(fetcher: fetcher, today: TODAY,
                        search_targets: [{ keyword: "ruby", hint: "Ruby" }, { keyword: "react", hint: "React" }],
                        pages_per_keyword: 2)

    postings = source.fetch

    assert_equal 4, fetcher.requested_urls.size
    assert_equal 20, postings.size
    assert_equal "Ruby", postings.first.category_hint, "先に出たキーワードのhintを残すはず"
  end

  def test_fetch_passes_each_keyword_hint_as_category_hint
    ruby_body = wrap_workship_list(build_workship_card_html(href: "/portal/a/job/1"))
    react_body = wrap_workship_list(build_workship_card_html(href: "/portal/b/job/2"))
    fetcher = UrlMapFetcher.new(bodies_by_url: {
      "https://goworkship.com/portal/keyword-ruby" => ruby_body,
      "https://goworkship.com/portal/keyword-react" => react_body
    })
    source = SOURCE.new(fetcher: fetcher, today: TODAY,
                        search_targets: [{ keyword: "ruby", hint: "Ruby" }, { keyword: "react", hint: "React" }],
                        pages_per_keyword: 1)

    hints_by_url = source.fetch.to_h { |posting| [posting.url, posting.category_hint] }

    assert_equal({ "https://goworkship.com/portal/a/job/1" => "Ruby",
                   "https://goworkship.com/portal/b/job/2" => "React" }, hints_by_url)
  end
end
