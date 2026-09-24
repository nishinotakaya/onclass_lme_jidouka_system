# frozen_string_literal: true
# test/services/freelance_jobs/sources_levtech_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesLevtechTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 12)

  def test_parse_fixture_returns_ten_postings
    body = read_fixture("levtech_search_ruby.html")
    postings = FreelanceJobs::Sources::Levtech.parse(body, today: TODAY, category_hint: "Ruby")

    assert_equal 10, postings.size
  end

  # --- 1件目の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    body = read_fixture("levtech_search_ruby.html")
    first = FreelanceJobs::Sources::Levtech.parse(body, today: TODAY, category_hint: "Ruby").first

    assert_equal "レバテックフリーランス", first.site
    assert_equal "https://freelance.levtech.jp/project/detail/608122", first.url
    assert_equal "【Ruby】動画配信プラットフォーム開発案件※アダルト含むのフリーランス求人・案件", first.title
    assert_equal "〜7,680円／時", first.reward
    assert_equal "時間単価制", first.work_format
    assert_equal ["Ruby", "Rails", "AWS", "GitHub", "React", "Google Cloud Platform", "Vue.js"], first.skills
    assert_equal "", first.client
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_equal ["New", "リモートOK", "20代活躍中", "30代活躍中", "参画実績あり"], first.tags
    assert_nil first.posted_on
  end

  # --- 単価の単位で work_format が分岐する ---

  def test_work_format_is_hourly_when_reward_includes_per_hour_unit
    body = read_fixture("levtech_search_ruby.html")
    postings = FreelanceJobs::Sources::Levtech.parse(body, today: TODAY, category_hint: "Ruby")

    hourly_posting = postings.find { |posting| posting.url.end_with?("/608122") }

    refute_nil hourly_posting
    assert_includes hourly_posting.reward, "／時"
    assert_equal "時間単価制", hourly_posting.work_format
  end

  def test_work_format_is_monthly_when_reward_includes_per_month_unit
    body = read_fixture("levtech_search_ruby.html")
    postings = FreelanceJobs::Sources::Levtech.parse(body, today: TODAY, category_hint: "Ruby")

    monthly_posting = postings.find { |posting| posting.url.end_with?("/608121") }

    refute_nil monthly_posting
    assert_includes monthly_posting.reward, "／月"
    assert_equal "月額制（業務委託）", monthly_posting.work_format
  end

  # --- URL正規化 ---

  def test_urls_are_normalized_absolute_without_trailing_slash_or_query
    body = read_fixture("levtech_search_ruby.html")
    postings = FreelanceJobs::Sources::Levtech.parse(body, today: TODAY, category_hint: "Ruby")

    postings.each do |posting|
      assert_match %r{\Ahttps://freelance\.levtech\.jp/project/detail/\d+\z}, posting.url,
                   "末尾スラッシュなし・クエリなしの正規化されたURLのはず"
    end
  end

  # --- category_hint が引数どおり全件に伝わる ---

  def test_category_hint_is_propagated_to_every_posting
    body = read_fixture("levtech_search_ruby.html")
    postings = FreelanceJobs::Sources::Levtech.parse(body, today: TODAY, category_hint: "Ruby")

    assert(postings.all? { |posting| posting.category_hint == "Ruby" },
           "全件のcategory_hintが引数のRubyになるはず")
  end

  # --- skillsが配列で、開発環境の値が「/」区切りで分解される ---

  def test_skills_are_split_by_slash_from_development_environment
    body = read_fixture("levtech_search_ruby.html")
    postings = FreelanceJobs::Sources::Levtech.parse(body, today: TODAY, category_hint: "Ruby")

    second_posting = postings.find { |posting| posting.url.end_with?("/608121") }

    assert_instance_of Array, second_posting.skills
    assert_equal ["Python", "Ruby", "Rails"], second_posting.skills
  end

  # --- hrefが無いカードは黙って除外される ---

  def test_parse_skips_cards_without_a_job_link
    fragment = <<~HTML
      <article class="projectCard">
        <h3 class="nameGroup"><span class="name">リンクなし</span></h3>
      </article>
    HTML
    postings = FreelanceJobs::Sources::Levtech.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")

    assert_equal [], postings
  end

  # --- fetch: search_targetsの数だけ取得し、URL重複を排除する ---

  # keywordを問わず常に同じbodyを返すFakeフェッチャー（呼び出されたURLを記録する）。
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

  def test_fetch_requests_each_search_target_and_deduplicates_urls_across_keywords
    body = read_fixture("levtech_search_ruby.html")
    fetcher = RecordingFetcher.new(body: body)
    search_targets = [
      { keyword: "Ruby", hint: "Ruby" },
      { keyword: "Rails", hint: "Ruby" }
    ]
    source = FreelanceJobs::Sources::Levtech.new(fetcher: fetcher, today: TODAY, search_targets: search_targets)

    postings = source.fetch

    assert_equal 2, fetcher.requested_urls.size, "search_targetsの数だけ取得するはず"
    assert_equal 10, postings.size, "同じURLの案件が2キーワード分返っても重複排除され10件のままのはず"
  end

  # === AC-01: 募集終了カード（h3.nameGroup > span.closedLabel）の扱い ===
  # fixtureは実HTML(levtech_search_ruby_live_2026-09-23.html)から募集中2件・募集終了2件を
  # 抜き出したもの。637377と607916が募集終了カード由来。

  CLOSED_CARD_URLS = [
    "https://freelance.levtech.jp/project/detail/637377",
    "https://freelance.levtech.jp/project/detail/607916"
  ].freeze

  def test_parse_fixture_with_closed_cards_does_not_drop_closed_cards
    body = read_fixture("levtech_search_with_closed.html")
    postings = FreelanceJobs::Sources::Levtech.parse(body, today: TODAY, category_hint: "Ruby")

    assert_equal 4, postings.size, "募集終了カード2件を含めて4件返るはず（落とさない）"
  end

  def test_closed_cards_have_closed_application_status
    body = read_fixture("levtech_search_with_closed.html")
    postings = FreelanceJobs::Sources::Levtech.parse(body, today: TODAY, category_hint: "Ruby")

    closed_postings = postings.select { |posting| CLOSED_CARD_URLS.include?(posting.url) }

    assert_equal 2, closed_postings.size
    closed_postings.each do |posting|
      assert_equal "募集終了", posting.application_status,
                   "span.closedLabelを持つカード由来のpostingは応募状況が「募集終了」のはず"
    end
  end

  # 現行実装は application_status: "-" 固定だが、"-"固定を期待値にせず
  # 「募集終了ではないこと」だけをassertする（将来 "-" 以外の表示に変わっても壊れない）。
  def test_open_cards_application_status_is_not_closed
    body = read_fixture("levtech_search_with_closed.html")
    postings = FreelanceJobs::Sources::Levtech.parse(body, today: TODAY, category_hint: "Ruby")

    open_postings = postings.reject { |posting| CLOSED_CARD_URLS.include?(posting.url) }

    assert_equal 2, open_postings.size
    open_postings.each do |posting|
      refute_equal "募集終了", posting.application_status, "募集中カード由来のpostingは募集終了扱いにならないはず"
    end
  end

  def test_closed_cards_still_have_correct_url_and_title
    body = read_fixture("levtech_search_with_closed.html")
    postings = FreelanceJobs::Sources::Levtech.parse(body, today: TODAY, category_hint: "Ruby")

    ec_platform_posting = postings.find { |posting| posting.url == "https://freelance.levtech.jp/project/detail/637377" }
    infra_maintenance_posting = postings.find { |posting| posting.url == "https://freelance.levtech.jp/project/detail/607916" }

    refute_nil ec_platform_posting
    assert_equal "【PHP/Java/Ruby】ECプラットフォーム開発のフリーランス求人・案件", ec_platform_posting.title

    refute_nil infra_maintenance_posting
    assert_equal "【Java】基幹システム運用維持保守業務のフリーランス求人・案件", infra_maintenance_posting.title
  end

  # --- Profile::ENGINEER にLevtechが含まれる（BEGINNERには含まれない） ---

  def test_engineer_profile_includes_levtech_source
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_includes source_classes, FreelanceJobs::Sources::Levtech
  end

  def test_beginner_profile_does_not_include_levtech_source
    source_classes = FreelanceJobs::Profile::BEGINNER.source_specs.map(&:first)

    refute_includes source_classes, FreelanceJobs::Sources::Levtech
  end
end
