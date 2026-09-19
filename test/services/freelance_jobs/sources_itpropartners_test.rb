# frozen_string_literal: true
# test/services/freelance_jobs/sources_itpropartners_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesItpropartnersTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 19)
  FIXTURE_NAME = "itpp_ruby.html"

  def parse_fixture(category_hint: "Ruby")
    FreelanceJobs::Sources::Itpropartners.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  # 一覧カード1件分のHTML片（実データの構造を模したもの）。
  # 単価は実物どおり全角チルダ「〜」・ASCIIスラッシュ「円/月」のまま。
  def build_itpropartners_card_html(href: "https://itpropartners.com/job/detail/99999", title: "テスト案件",
                                      price: "〜600,000円/月", contract_type: "業務委託",
                                      work_style: "フルリモート 週3日〜5日", location: nil,
                                      required_skill: "テスト開発経験（3年以上） / チーム開発経験",
                                      tech_environment: %w[Ruby], tags: %w[Ruby フルリモート],
                                      agent_comment: "担当エージェントのコメントです。")
    location_row = location ? <<~ROW : ""
      <div class="itp-job-card__detail-row">
        <div class="itp-job-card__detail-label">場所</div>
        <div class="itp-job-card__detail-value">#{location}</div>
      </div>
    ROW
    tech_environment_links = tech_environment.map { |name| "<a href=\"/job/engineer/dummy\" class=\"itp-job-card__detail-link\">#{name}</a>" }.join
    tag_spans = tags.map { |tag| "<span class=\"itp-job-card__tag\">#{tag}</span>" }.join

    <<~HTML
      <article class="itp-job-card">
        <div class="itp-job-card__header">
          <a href="#{href}" target="_blank">
            <h2 class="itp-job-card__title">#{title}</h2>
          </a>
          <div class="itp-job-card__price-row">
            <span class="itp-job-card__price">#{price}</span>
            <div class="itp-job-card__contract-type"><span>#{contract_type}</span></div>
          </div>
        </div>
        <div class="itp-job-card__tags">#{tag_spans}</div>
        <div class="itp-job-card__details">
          <div class="itp-job-card__detail-row">
            <div class="itp-job-card__detail-label">働き方</div>
            <div class="itp-job-card__detail-value">#{work_style}</div>
          </div>
          #{location_row}
          <div class="itp-job-card__detail-row">
            <div class="itp-job-card__detail-label">開発環境</div>
            <div class="itp-job-card__detail-value">#{tech_environment_links}</div>
          </div>
          <div class="itp-job-card__detail-row">
            <div class="itp-job-card__detail-label">求めるスキル</div>
            <div class="itp-job-card__detail-value">#{required_skill}</div>
          </div>
        </div>
        <div class="itp-job-card__agent">
          <p class="itp-job-card__agent-text">#{agent_comment}</p>
        </div>
      </article>
    HTML
  end

  # --- 件数 ---

  def test_parse_fixture_returns_forty_postings
    assert_equal 40, parse_fixture.size, "1ページ40件固定のはず"
  end

  # --- 1件目の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "ITプロパートナーズ", first.site
    assert_equal "https://itpropartners.com/job/detail/22748", first.url
    assert_equal "【Ruby/Next.js】株主総会支援におけるフルスタックの業務委託案件・フリーランス求人", first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "〜600,000円／月", first.reward
    assert_equal "業務委託", first.work_format
    assert_equal ["Ruby", "Next.js"], first.skills
    assert_equal ["Ruby", "Next.js", "フロントエンドエンジニア", "バックエンドエンジニア", "フルリモート"], first.tags
    assert_equal "", first.client
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_equal Date.new(2026, 9, 1), first.posted_on, "最終更新日をposted_onとして使うはず"
  end

  # descriptionは「働き方 / (場所) / 求めるスキル / エージェントより」の順に連結される。
  # 「開発環境」欄はskillsに構造化するため、descriptionには重複させない。
  def test_parse_first_posting_description_joins_detail_rows_and_agent_comment
    description = parse_fixture.first.description

    assert description.start_with?("働き方: フルリモート 週3日〜5日"), "先頭は働き方のはず: #{description}"
    assert_includes description, "求めるスキル: Ruby on Rails×Next.jsでのフルスタック開発（3年以上）"
    assert_includes description, "エージェントより: 現任業務委託の交代（リプレイス）枠です。"
    refute_includes description, "開発環境", "開発環境はskillsに入るためdescriptionには重複させないはず"
    refute_match(/\n/, description, "normalize_descriptionで改行が畳まれているはず")
  end

  # 経験年数がdescriptionに残っていることを検証する（EngineerClassifierのレベル判定に使うため）。
  def test_description_keeps_experience_years_for_level_classification
    parse_fixture.each do |posting|
      next unless posting.description.include?("年以上") || posting.description.include?("年程度")

      assert_match(/\d+\s*年(?:以上|程度)/, posting.description)
    end

    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/22357") }
    assert_includes posting.description, "3年程度", "「目安3年程度」の経験年数が残っているはず"
  end

  # --- 場所が有るカード / 無いカード ---

  def test_description_includes_location_when_present
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/22357") }

    assert_includes posting.description, "場所: 東京都 築地駅"
  end

  def test_description_omits_location_when_absent
    first = parse_fixture.first

    refute_includes first.description, "場所:"
  end

  # --- 単価 ---

  def test_reward_is_displayed_with_fullwidth_tilde_and_slash
    parse_fixture.each do |posting|
      assert_match(/\A〜[\d,]+円／月\z/, posting.reward)
    end
  end

  def test_posting_without_price_falls_back_to_placeholder_reward
    fragment = build_itpropartners_card_html(price: "")
    posting = FreelanceJobs::Sources::Itpropartners.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    assert_equal "要確認", posting.reward
  end

  # --- URL正規化 ---

  def test_urls_are_normalized_absolute_without_trailing_slash_or_query
    parse_fixture.each do |posting|
      assert_match %r{\Ahttps://itpropartners\.com/job/detail/\d+\z}, posting.url
    end
  end

  # --- category_hint が引数どおり全件に伝わる ---

  def test_category_hint_is_propagated_to_every_posting
    postings = parse_fixture(category_hint: "TypeScript")

    assert(postings.all? { |posting| posting.category_hint == "TypeScript" },
           "全件のcategory_hintが引数の値になるはず")
  end

  # --- 必須要素が欠けたカードは黙って除外する ---

  def test_parse_skips_cards_without_a_title_link
    fragment = <<~HTML
      <article class="itp-job-card">
        <h2 class="itp-job-card__title">リンクなし（詳細ページに出るカードと同じ形を模擬）</h2>
      </article>
    HTML

    assert_equal [], FreelanceJobs::Sources::Itpropartners.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_skips_cards_whose_title_link_has_empty_href
    fragment = build_itpropartners_card_html(href: "")

    assert_equal [], FreelanceJobs::Sources::Itpropartners.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  # ランキングカルーセルの偽カード（div.itp-job-card、タグがarticleではない）は無視される。
  def test_parse_ignores_ranking_carousel_fake_cards
    fragment = <<~HTML
      <div class="itp-job-card">
        <div class="itp-job-card__header">
          <p class="itp-job-card__title">【コンサルタント】</p>
        </div>
        <div class="itp-job-card__body">
          <span class="itp-job-card__price">〜？？円/月</span>
        </div>
      </div>
    HTML

    assert_equal [], FreelanceJobs::Sources::Itpropartners.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  # --- 同一URLの重複は畳む ---

  def test_parse_deduplicates_postings_with_the_same_url
    fragment = build_itpropartners_card_html(href: "https://itpropartners.com/job/detail/1", title: "A") +
               build_itpropartners_card_html(href: "https://itpropartners.com/job/detail/1", title: "B")

    postings = FreelanceJobs::Sources::Itpropartners.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")

    assert_equal 1, postings.size
    assert_equal "A", postings.first.title, "先に出てきた方を残すはず"
  end

  # --- 単価・スキル・場所が欠けても落ちない ---

  def test_parse_does_not_raise_when_price_and_skills_are_missing
    fragment = build_itpropartners_card_html(price: "", tech_environment: [], tags: [], required_skill: "")

    postings = FreelanceJobs::Sources::Itpropartners.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")

    assert_equal 1, postings.size
    posting = postings.first
    assert_equal "要確認", posting.reward
    assert_equal [], posting.skills
    assert_equal [], posting.tags
  end

  # --- fetch: 通信せずにリクエスト設計を検証する ---

  # 要求されたURLを記録し、常に同じbodyを返すFakeフェッチャー。
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

  # ページ番号ごとに返すbodyを切り替えるFakeフェッチャー（該当件数が少ないスラッグの再現用）。
  class PagedFetcher
    def initialize(bodies_by_page:)
      @bodies_by_page = bodies_by_page
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      page_number = url[/[?&]page=(\d+)/, 1]&.to_i || 1
      @bodies_by_page.fetch(page_number, "")
    end
  end

  def test_fetch_requests_each_slug_and_page_and_deduplicates_urls
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    search_targets = [
      { slug: "ruby", hint: "Ruby" },
      { slug: "typescript", hint: "TypeScript" }
    ]
    source = FreelanceJobs::Sources::Itpropartners.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, pages_per_slug: 2
    )

    postings = source.fetch

    assert_equal 4, fetcher.requested_urls.size, "2スラッグ×2ページぶん取得するはず"
    assert_equal 40, postings.size, "同じ案件が複数スラッグ・複数ページで返っても重複排除されるはず"
  end

  def test_fetch_builds_path_style_slug_url_and_bare_page_query
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    source = FreelanceJobs::Sources::Itpropartners.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ slug: "ruby-on-rails", hint: "Ruby" }], pages_per_slug: 2
    )

    source.fetch

    assert_equal [
      "https://itpropartners.com/job/engineer/ruby-on-rails",
      "https://itpropartners.com/job/engineer/ruby-on-rails?page=2"
    ], fetcher.requested_urls,
       "1ページ目はpageを付けず、2ページ目は robots.txt の Allow: /job*?page= に合わせて \"?page=2\" 単独にするはず"
  end

  def test_fetch_stops_paging_when_a_page_has_no_cards
    fetcher = PagedFetcher.new(bodies_by_page: { 1 => read_fixture(FIXTURE_NAME), 2 => wrap_html("<p>該当する案件はありません</p>") })
    source = FreelanceJobs::Sources::Itpropartners.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ slug: "ruby", hint: "Ruby" }], pages_per_slug: 3
    )

    postings = source.fetch

    assert_equal 2, fetcher.requested_urls.size, "0件ページに当たったら3ページ目は取りに行かないはず"
    assert_equal 40, postings.size
  end

  def test_fetch_requests_only_pages_per_slug_pages
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    source = FreelanceJobs::Sources::Itpropartners.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ slug: "ruby", hint: "Ruby" }]
    )

    source.fetch

    assert_equal 2, fetcher.requested_urls.size, "既定のpages_per_slugは2のはず（=80件に抑える）"
  end

  # --- 分類器に渡したときRuby案件・中級判定できる（descriptionの作りの検証）---

  def test_first_posting_is_classified_as_ruby
    result = FreelanceJobs::EngineerClassifier.classify(parse_fixture.first, today: TODAY)

    assert_equal "Ruby", result.category
  end

  def test_first_posting_is_classified_with_experience_years_from_description
    result = FreelanceJobs::EngineerClassifier.classify(parse_fixture.first, today: TODAY)

    assert_match(/実務経験3年以上/, result.memo)
  end
end
