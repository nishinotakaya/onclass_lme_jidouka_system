# frozen_string_literal: true
# test/services/freelance_jobs/sources_tech_reach_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesTechReachTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 10, 8)
  FIXTURE_NAME = "tech_reach_ruby.html"
  SOURCE = FreelanceJobs::Sources::TechReach
  RUBY_URL = "https://tech-reach.jp/jobs/s-ruby"

  def parse_fixture(category_hint: "Ruby")
    SOURCE.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  # 一覧カード1件分のHTML片（実フィクスチャのDOM構造を模したもの）。
  # 詳細リンクは実ページ同様 PC用とSP用の2回出す。
  def build_tech_reach_card_html(href: "/jobs/1/", title: "テスト案件", price: "65万 ~ 70万",
                                 position: "バックエンドエンジニア", location: "渋谷",
                                 employment: "業務委託(準委任)", tag_labels: [])
    link = href ? %(<a href="#{href}" class="m-result__btn"><span>案件の詳細をみる</span></a>) : ""
    tag_items = tag_labels.map { |label| %(<li><span class="label_new">#{label}</span></li>) }.join
    position_html = position ? %(<dd class="oc-pos">#{position}</dd>) : ""
    price_html = price ? %(<em class="oc-price">#{price}</em>) : ""
    <<~HTML
      <section class="m-result">
        <ul class="m-result__tags m-result-tags">#{tag_items}</ul>
        <h2 class="m-result__ttl">#{title}</h2>
        #{price_html}
        <ul class="oc-skills"><li><a href="/jobs/s-ruby">Ruby</a></li></ul>
        <dd class="oc-loc">#{location}</dd>
        <dd class="oc-emp_st">#{employment}</dd>
        #{position_html}
        <dd class="oc-desc"><p>本文1行目<br>本文2行目</p></dd>
        <div>#{link}</div>
        <div>#{link}</div>
      </section>
    HTML
  end

  # --- 件数・1件目 ---

  def test_parse_fixture_returns_fifty_postings
    assert_equal 50, parse_fixture.size
  end

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "テックリーチ", first.site
    # 末尾スラッシュは normalize_url が除去する。
    assert_equal "https://tech-reach.jp/jobs/51314", first.url
    assert_equal "寄添伴走！【Ruby/基本リモート】【業務委託（準委任）】データ×AIサービス運営企業におけるフルスタックエンジニア", first.title
    assert_equal "65万〜70万円／月", first.reward
    assert_equal %w[CircleCI Nuxt.js Docker github Vue.js MySQL Ruby AWS GCP], first.skills
    assert_equal "業務委託(準委任)", first.work_format
    assert_equal "Ruby", first.category_hint
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.client
    assert_nil first.deadline_on
    assert_nil first.posted_on
  end

  def test_parse_titles_do_not_end_with_seo_suffix
    titles = parse_fixture.map(&:title)

    assert_equal 50, titles.size
    titles.each { |title| refute title.end_with?("の案件・求人"), title }
  end

  def test_parse_first_posting_tags_are_new_and_recommend
    assert_equal %w[NEW おすすめ], parse_fixture.first.tags
  end

  def test_parse_first_posting_description_has_body_location_and_contract_without_position
    description = parse_fixture.first.description

    assert_includes description, "データ×AIサービスを運営する事業会社のプロダクト開発をご担当いただきます。"
    assert_includes description, "★面談回数：1回（2回の可能性あり）"
    # 1件目のカードには募集職種(oc-pos)が無いので、ラベルも出さない。
    refute_includes description, "募集職種:"
    assert_operator description.index("★面談回数"), :<, description.index("勤務地: 銀座")
    assert_operator description.index("勤務地: 銀座"), :<, description.index("契約形態: 業務委託(準委任)")
    refute_match(/\n/, description)
  end

  def test_parse_description_orders_body_position_location_contract
    third = parse_fixture[2]
    description = third.description

    assert_equal "https://tech-reach.jp/jobs/50601", third.url
    positions = ["団体運営における", "募集職種: バックエンドエンジニア", "勤務地: 池袋", "契約形態: 業務委託(準委任)"].map do |segment|
      description.index(segment)
    end
    refute_includes positions, nil, description
    assert_equal positions.sort, positions
  end

  def test_parse_tags_only_recommend_when_not_new
    tag_sets = parse_fixture.map(&:tags).uniq

    assert_includes tag_sets, ["おすすめ"]
    assert(tag_sets.all? { |tags| (tags - %w[NEW おすすめ]).empty? })
  end

  # --- 重複・欠けたカード ---

  def test_parse_deduplicates_pc_and_sp_links
    urls = parse_fixture.map(&:url)

    assert_equal urls.uniq, urls
    assert_equal 50, urls.uniq.size
    urls.each { |url| assert_match %r{\Ahttps://tech-reach\.jp/jobs/\d+\z}, url }
  end

  def test_parse_deduplicates_cards_with_the_same_url
    fragment = build_tech_reach_card_html(title: "A") + build_tech_reach_card_html(title: "B")

    assert_equal ["A"], SOURCE.parse(wrap_html(fragment), today: TODAY).map(&:title)
  end

  def test_parse_skips_cards_without_link
    assert_equal [], SOURCE.parse(wrap_html(build_tech_reach_card_html(href: nil)), today: TODAY)
  end

  def test_parse_empty_page_returns_empty_array
    assert_equal [], SOURCE.parse(wrap_html(""), today: TODAY)
  end

  # --- 報酬の正規化 ---

  def reward_of(price)
    SOURCE.parse(wrap_html(build_tech_reach_card_html(price: price)), today: TODAY).first.reward
  end

  def test_reward_is_normalized_to_range_per_month
    assert_equal "70万〜75万円／月", reward_of(" 70万 ~ 75万 ")
    assert_equal "100万〜105万円／月", reward_of("\n 100万 ~ 105万\n")
  end

  def test_reward_accepts_fullwidth_tildes
    assert_equal "70万〜75万円／月", reward_of("70万 〜 75万")
    assert_equal "70万〜75万円／月", reward_of("70万 ～ 75万")
  end

  # 実データに上限下限が逆転した「95万 ~ 90万」がある。サイト表記を尊重し、並べ替えずそのまま出す。
  def test_reward_keeps_reversed_range_as_published
    assert_equal "95万〜90万円／月", reward_of("95万 ~ 90万")
  end

  def test_reward_with_only_lower_bound_is_needs_confirmation
    assert_equal "要確認", reward_of("65万 ~")
  end

  def test_reward_with_only_upper_bound_is_needs_confirmation
    assert_equal "要確認", reward_of("~ 70万")
  end

  def test_reward_empty_is_needs_confirmation
    assert_equal "要確認", reward_of("  ")
  end

  def test_reward_missing_element_is_needs_confirmation
    assert_equal "要確認", reward_of(nil)
  end

  def test_work_format_empty_is_needs_confirmation
    html = wrap_html(build_tech_reach_card_html(employment: ""))

    assert_equal "要確認", SOURCE.parse(html, today: TODAY).first.work_format
  end

  # --- 定数 ---

  def test_constants
    assert_equal "テックリーチ", SOURCE::SITE_NAME
    assert_equal 2, SOURCE::MAX_PAGES
    assert_equal [
      { skill_slug: "ruby", hint: "Ruby" },
      { skill_slug: "typescript", hint: "TypeScript" },
      { skill_slug: "react", hint: "React" }
    ], SOURCE::DEFAULT_SEARCH_TARGETS
    assert_respond_to SOURCE::REQUEST_INTERVAL, :to_f
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

  def build_source(fetcher, targets: [{ skill_slug: "ruby", hint: "Ruby" }])
    SOURCE.new(fetcher: fetcher, today: TODAY, search_targets: targets)
  end

  def test_fetch_builds_bare_first_page_and_page_query
    body = read_fixture(FIXTURE_NAME)
    fetcher = MapFetcher.new(bodies_by_url: { RUBY_URL => body, "#{RUBY_URL}?page=2" => body })

    build_source(fetcher).fetch

    assert_equal [RUBY_URL, "#{RUBY_URL}?page=2"], fetcher.requested_urls
  end

  def test_fetch_treats_404_on_later_page_as_end_of_pages
    fetcher = MapFetcher.new(
      bodies_by_url: { RUBY_URL => read_fixture(FIXTURE_NAME) },
      errors_by_url: { "#{RUBY_URL}?page=2" => FreelanceJobs::FetchError.new("HTTP 404 #{RUBY_URL}?page=2") }
    )

    postings = build_source(fetcher).fetch

    assert_equal 50, postings.size
    assert_equal 2, fetcher.requested_urls.size
  end

  def test_fetch_raises_when_first_page_is_404
    fetcher = MapFetcher.new(errors_by_url: { RUBY_URL => FreelanceJobs::FetchError.new("HTTP 404 #{RUBY_URL}") })

    assert_raises(FreelanceJobs::FetchError) { build_source(fetcher).fetch }
  end

  def test_fetch_does_not_swallow_non_404_errors_on_later_page
    fetcher = MapFetcher.new(
      bodies_by_url: { RUBY_URL => read_fixture(FIXTURE_NAME) },
      errors_by_url: { "#{RUBY_URL}?page=2" => FreelanceJobs::FetchError.new("HTTP 500 #{RUBY_URL}?page=2") }
    )

    assert_raises(FreelanceJobs::FetchError) { build_source(fetcher).fetch }
  end

  def test_fetch_does_not_request_second_page_when_first_page_is_empty
    fetcher = MapFetcher.new

    assert_equal [], build_source(fetcher).fetch
    assert_equal [RUBY_URL], fetcher.requested_urls
  end

  def test_fetch_stops_at_max_pages
    body = read_fixture(FIXTURE_NAME)
    fetcher = MapFetcher.new(bodies_by_url: { RUBY_URL => body, "#{RUBY_URL}?page=2" => body, "#{RUBY_URL}?page=3" => body })

    build_source(fetcher).fetch

    assert_equal [RUBY_URL, "#{RUBY_URL}?page=2"], fetcher.requested_urls
  end

  def test_fetch_deduplicates_across_targets_and_keeps_first_hint
    body = read_fixture(FIXTURE_NAME)
    fetcher = MapFetcher.new(bodies_by_url: { RUBY_URL => body, "https://tech-reach.jp/jobs/s-react" => body })
    targets = [{ skill_slug: "ruby", hint: "Ruby" }, { skill_slug: "react", hint: "React" }]

    postings = build_source(fetcher, targets: targets).fetch

    assert_equal 50, postings.size
    assert(postings.all? { |posting| posting.category_hint == "Ruby" })
  end
end
