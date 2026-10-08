# frozen_string_literal: true
# test/services/freelance_jobs/sources_kyujin_box_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"
require "cgi"

class FreelanceJobsSourcesKyujinBoxTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 10, 8)
  FIXTURE_NAME = "kyujin_box_ruby_hokkaido.html"
  SOURCE = FreelanceJobs::Sources::KyujinBox
  BASE_URL = "https://xn--pckua2a7gp15o89zb.com"
  RUBY_LIST_URL = "#{BASE_URL}/#{CGI.escape('Rubyエンジニアの仕事-北海道')}"
  RUBY_TARGET = { path_keyword: "Rubyエンジニアの仕事-北海道", hint: "Ruby" }.freeze

  def parse_fixture
    SOURCE.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: "Ruby")
  end

  # 一覧カード1件分のHTML片。data-func-show-arg は「JSON の json キーにさらに JSON 文字列」という二重構造。
  def build_card_html(job_hash: { "uniqueId" => "abc123", "title" => "テスト求人" }, raw_attribute: nil)
    attribute_value = raw_attribute || JSON.generate({ "json" => JSON.generate(job_hash) })
    anchor = %(<a class="p-result_title_link" data-func-show-arg="#{CGI.escapeHTML(attribute_value)}">x</a>)
    %(<section class="p-result_card">#{anchor}</section>)
  end

  def build_job_hash(overrides = {})
    {
      "uniqueId" => "16a7a452875733fc7b812ed1f75686c8", "title" => "Rubyエンジニア", "company" => "株式会社テスト",
      "workArea" => "北海道 札幌市", "employType" => "正社員", "payment" => "月給30万円～", "siteName" => "マイナビ転職",
      "url" => "https://example.com/x", "updatedAt" => "2026-10-03 20:59:33.000",
      "allFeatureTags" => ["未経験OK", "MySQL", "AWS"], "jobType" => "", "firstYamlHead" => "", "firstYamlContent" => ""
    }.merge(overrides)
  end

  # --- 件数・1件目 ---

  def test_parse_fixture_returns_twenty_four_postings
    assert_equal 24, parse_fixture.size
  end

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "Green", first.site
    assert_match %r{\Ahttps://www\.green-japan\.com/company/8534/job/161472}, first.url
    assert_equal "【株式会社リファルケ】Rubyエンジニア/システムインテグレータ・ソフトハウス", first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "年収450万円～1,260万円 / 賞与あり", first.reward
    assert_equal "正社員", first.work_format
    assert_equal "-", first.application_status
    assert_equal "株式会社リファルケ", first.client
    assert_nil first.deadline_on
    assert_equal Date.new(2026, 10, 8), first.posted_on
    assert_includes first.skills, "MySQL"
    assert_includes first.skills, "AWS"
    refute_includes first.skills, "未経験OK"
    assert_includes first.tags, "未経験OK"
  end

  def test_parse_first_posting_description_has_location_readable_by_hokkaido_classifier
    description = parse_fixture.first.description

    assert_includes description, "会社: 株式会社リファルケ"
    assert_includes description, "掲載元: Green"
    assert_match(/勤務地: 北海道 札幌市 札幌駅 徒歩2分 \/ 雇用形態: 正社員\z/, description)
    refute_match(/\n/, description)
  end

  # --- 掲載サイト名 ---

  def parse_site_of(job_overrides)
    SOURCE.parse(build_card_html(job_hash: build_job_hash(job_overrides)), today: TODAY).first.site
  end

  def test_site_strips_registration_entry_suffix_with_spacing_variants
    assert_equal "ビズリーチ", parse_site_of("uniqueId" => "lbiz", "siteName" => "ビズリーチ - 登録エントリー")
    assert_equal "マイナビエージェント", parse_site_of("uniqueId" => "lmai", "siteName" => "マイナビエージェント- 登録エントリー")
    assert_equal "Green", parse_site_of("uniqueId" => "lgre", "siteName" => "Green")
  end

  def test_site_falls_back_to_kyujin_box_when_site_name_is_blank
    assert_equal "求人ボックス", parse_site_of("siteName" => "")
    assert_equal "求人ボックス", parse_site_of("siteName" => " - 登録エントリー")
  end

  def test_site_is_kyujin_box_for_direct_listing_whose_site_name_is_the_company
    assert_equal "求人ボックス", parse_site_of("siteName" => "株式会社テスト")
  end

  def test_site_keeps_site_name_for_aggregated_card_even_if_it_equals_company
    assert_equal "株式会社テスト", parse_site_of("uniqueId" => "lsame", "siteName" => "株式会社テスト")
  end

  def test_title_has_no_company_prefix_when_company_is_blank
    html = build_card_html(job_hash: build_job_hash("company" => ""))

    assert_equal "Rubyエンジニア", SOURCE.parse(html, today: TODAY).first.title
  end

  def test_fixture_site_tally_and_bizreach_site
    sites = parse_fixture.map(&:site)

    puts "SITE TALLY: #{sites.tally.inspect}"
    refute_includes sites.first(1), "求人ボックス"
    assert_includes sites, "ビズリーチ"
    refute(sites.any? { |site| site.include?("登録エントリー") })
    bizreach = parse_fixture.find { |posting| posting.url.start_with?("https://www.bizreach.jp/") }
    assert_equal "ビズリーチ", bizreach.site
  end

  def test_reports_original_site_names_marker
    assert SOURCE.reports_original_site_names?
  end

  # --- URL ---

  def test_direct_listing_card_uses_kyujin_box_job_url
    posting = parse_fixture.find { |candidate| candidate.url.include?("16a7a452875733fc7b812ed1f75686c8") }

    assert_equal "#{BASE_URL}/jb/16a7a452875733fc7b812ed1f75686c8", posting.url
  end

  def test_bizreach_postings_get_distinct_keys_per_job_id
    bizreach_urls = parse_fixture.map(&:url).select { |url| url.start_with?("https://www.bizreach.jp/") }
    keys = bizreach_urls.map { |url| FreelanceJobs::JobPosting.normalize_url(url) }

    assert_operator keys.size, :>=, 2
    assert_equal keys.size, keys.uniq.size
    assert(keys.all? { |key| key.include?("?job_id=") })
  end

  def test_parse_skips_aggregated_card_without_source_url
    html = build_card_html(job_hash: build_job_hash("uniqueId" => "lzzz", "url" => ""))

    assert_equal [], SOURCE.parse(html, today: TODAY)
  end

  # --- 欠けた・壊れたカード ---

  def test_parse_skips_cards_with_broken_attribute
    html = build_card_html(raw_attribute: "{not json") +
           build_card_html(raw_attribute: JSON.generate({ "json" => "{broken" })) +
           %(<section class="p-result_card"><a class="p-result_title_link">no attr</a></section>) +
           build_card_html(job_hash: build_job_hash)

    postings = SOURCE.parse(html, today: TODAY)

    assert_equal ["【株式会社テスト】Rubyエンジニア"], postings.map(&:title)
  end

  def test_parse_defaults_reward_and_work_format_when_blank_and_tolerates_bad_date
    html = build_card_html(job_hash: build_job_hash("payment" => "", "employType" => "", "updatedAt" => "不明"))

    posting = SOURCE.parse(html, today: TODAY).first

    assert_equal "要確認", posting.reward
    assert_equal "要確認", posting.work_format
    assert_nil posting.posted_on
  end

  def test_parse_includes_yaml_content_and_job_type_in_description
    job = build_job_hash("firstYamlHead" => "仕事内容", "firstYamlContent" => "Rails開発\nです", "jobType" => "ITエンジニア")

    description = SOURCE.parse(build_card_html(job_hash: job), today: TODAY).first.description

    assert description.start_with?("仕事内容: Rails開発 です"), description
    assert_includes description, "職種: ITエンジニア"
  end

  def test_parse_replaces_slash_in_work_area_so_classifier_can_read_it
    job = build_job_hash("workArea" => "北海道 札幌市/江別市")

    description = SOURCE.parse(build_card_html(job_hash: job), today: TODAY).first.description

    assert_match(/勤務地: 北海道 札幌市・江別市 \//, description)
  end

  def test_parse_empty_page_returns_empty_array
    assert_equal [], SOURCE.parse("<html></html>", today: TODAY)
  end

  # --- 定数 ---

  def test_constants
    assert_equal "求人ボックス", SOURCE::SITE_NAME
    assert_equal 1.5, SOURCE::REQUEST_INTERVAL
    assert_equal BASE_URL, SOURCE::BASE_URL
    assert_equal 30, SOURCE::MAX_PAGES
    assert_equal [RUBY_TARGET], SOURCE::DEFAULT_SEARCH_TARGETS
  end

  def test_list_url_has_no_plus_sign
    refute_includes RUBY_LIST_URL, "+"
  end

  # --- fetch ---

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

  def build_source(fetcher, max_pages: 2)
    SOURCE.new(fetcher: fetcher, today: TODAY, search_targets: [RUBY_TARGET], max_pages: max_pages)
  end

  def test_fetch_builds_bare_first_page_and_pg_query
    body = read_fixture(FIXTURE_NAME)
    fetcher = MapFetcher.new(bodies_by_url: { RUBY_LIST_URL => body, "#{RUBY_LIST_URL}?pg=2" => body })

    build_source(fetcher).fetch

    assert_equal [RUBY_LIST_URL, "#{RUBY_LIST_URL}?pg=2"], fetcher.requested_urls
  end

  def test_fetch_deduplicates_same_cards_across_pages
    body = read_fixture(FIXTURE_NAME)
    fetcher = MapFetcher.new(bodies_by_url: { RUBY_LIST_URL => body, "#{RUBY_LIST_URL}?pg=2" => body })

    assert_equal 24, build_source(fetcher).fetch.size
  end

  def test_fetch_treats_404_on_later_page_as_end_of_pages
    fetcher = MapFetcher.new(
      bodies_by_url: { RUBY_LIST_URL => read_fixture(FIXTURE_NAME) },
      errors_by_url: { "#{RUBY_LIST_URL}?pg=2" => FreelanceJobs::FetchError.new("HTTP 404 #{RUBY_LIST_URL}?pg=2") }
    )

    postings = build_source(fetcher, max_pages: 5).fetch

    assert_equal 24, postings.size
    assert_equal 2, fetcher.requested_urls.size
  end

  def test_fetch_stops_when_a_page_has_no_cards
    fetcher = MapFetcher.new(bodies_by_url: { RUBY_LIST_URL => read_fixture(FIXTURE_NAME) })

    build_source(fetcher, max_pages: 5).fetch

    assert_equal 2, fetcher.requested_urls.size
  end

  def test_fetch_raises_when_first_page_is_404
    fetcher = MapFetcher.new(errors_by_url: { RUBY_LIST_URL => FreelanceJobs::FetchError.new("HTTP 404 #{RUBY_LIST_URL}") })

    assert_raises(FreelanceJobs::FetchError) { build_source(fetcher).fetch }
  end
end
