# frozen_string_literal: true
# test/services/freelance_jobs/sources_hipro_tech_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesHiproTechTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 12)
  # fixtureは https://tech.hipro-job.jp/job-search?pg_skill_id%5B9%5D=9 の無加工の保存。
  # 1ページ20件固定・うち募集中1件(/job/51779)・時単価2件。
  FIXTURE_NAME = "hipro_tech_ruby.html"

  def parse_fixture(category_hint: "Ruby")
    FreelanceJobs::Sources::HiproTech.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  def test_parse_fixture_returns_twenty_postings
    assert_equal 20, parse_fixture.size
  end

  # --- 1件目の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "HiPro Tech", first.site
    assert_equal "https://tech.hipro-job.jp/job/51779", first.url
    assert_equal "【フルスタック(React・Ruby)／一部リモート有】インフルエンサーマーケティングSaaSの設計・開発・テスト", first.title
    assert_equal "募集職種: フロントエンドエンジニア・バックエンドエンジニア / 開発環境: Ruby / " \
                 "稼働: 週3日・週4日・週5日 / 案件区分: 受託サービス / 特徴: 一部リモート可・事業会社",
                 first.description
    assert_equal "Ruby", first.category_hint
    assert_equal "450,000〜550,000円／月", first.reward
    assert_equal "月額制（業務委託）", first.work_format
    assert_equal "募集中", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_equal ["Ruby"], first.skills
    assert_equal "", first.client
    assert_equal ["受託サービス", "一部リモート可", "事業会社"], first.tags
    assert_equal Date.new(2026, 3, 3), first.posted_on
  end

  # --- 単価種別で reward の単位と work_format が分岐する ---

  def test_monthly_salary_card_has_monthly_reward_unit_and_work_format
    monthly_posting = parse_fixture.find { |posting| posting.url.end_with?("/job/56981") }

    refute_nil monthly_posting
    assert_equal "800,000〜1,000,000円／月", monthly_posting.reward
    assert_equal "月額制（業務委託）", monthly_posting.work_format
  end

  def test_hourly_salary_card_has_hourly_reward_unit_and_work_format
    hourly_posting = parse_fixture.find { |posting| posting.url.end_with?("/job/55348") }

    refute_nil hourly_posting
    assert_equal "4,500〜6,000円／時", hourly_posting.reward
    assert_equal "時間単価制", hourly_posting.work_format
    assert_equal ["Go言語", "Ruby"], hourly_posting.skills
    assert_equal Date.new(2026, 5, 1), hourly_posting.posted_on
  end

  # 実測360カードには 月単価/時単価 のほかに 日単価5件・週単価1件が存在する。
  # fixtureには含まれないため断片HTMLで確認する。金額だけを単位なしで出すと
  # 月額か日額か読み手に判別できないので、"／日" のような単位が必ず付くこと。
  def build_salary_card(salary_system)
    <<~HTML
      <div class="job-card" about="/job/99999">
        <span class="job-list-title">単価種別の確認用案件</span>
        <div class="money">
          <div class="field--name-field-job-salary-low">250,000円</div>
          <div class="field--name-field-job-salary-high">450,000円</div>
          <div class="field--name-field-job-salary-system">#{salary_system}</div>
        </div>
      </div>
    HTML
  end

  def parse_salary_card(salary_system)
    FreelanceJobs::Sources::HiproTech.parse(
      wrap_html(build_salary_card(salary_system)), today: TODAY, category_hint: "Ruby"
    ).first
  end

  def test_daily_salary_card_has_daily_reward_unit
    posting = parse_salary_card("日単価")

    assert_equal "250,000〜450,000円／日", posting.reward
    assert_equal "業務委託（フリーランス）", posting.work_format
  end

  def test_weekly_salary_card_has_weekly_reward_unit
    posting = parse_salary_card("週単価")

    assert_equal "250,000〜450,000円／週", posting.reward
    assert_equal "業務委託（フリーランス）", posting.work_format
  end

  # 「◯単価」の形でない未知の種別に推測で単位を付けると誤った金額表記になるため、
  # そのときだけは単位なしにする。
  def test_unknown_salary_system_card_has_no_reward_unit
    posting = parse_salary_card("成果報酬")

    assert_equal "250,000〜450,000円", posting.reward
    assert_equal "業務委託（フリーランス）", posting.work_format
  end

  # --- 応募状態は div.entry-link のclassで分岐する ---

  def test_application_status_is_taken_from_entry_link_class
    postings = parse_fixture

    open_posting = postings.find { |posting| posting.url.end_with?("/job/51779") }
    expired_posting = postings.find { |posting| posting.url.end_with?("/job/55348") }

    assert_equal "募集中", open_posting.application_status
    assert_equal "募集期間外", expired_posting.application_status
    assert_equal 1, postings.count { |posting| posting.application_status == "募集中" }
    assert_equal 19, postings.count { |posting| posting.application_status == "募集期間外" }
  end

  # --- skills は 言語 + フレームワーク + インフラ の3欄を連結したもの ---

  def test_skills_concatenate_language_framework_and_infrastructure_fields
    posting = parse_fixture.find { |posting| posting.url.end_with?("/job/48817") }

    refute_nil posting
    # 言語 Java/Python/Ruby ＋ フレームワーク Ruby on Rails ＋ インフラ Linux/CentOS/Ubuntu
    assert_equal ["Java", "Python", "Ruby", "Ruby on Rails", "Linux", "CentOS", "Ubuntu"], posting.skills
    assert_includes posting.description, "開発環境: Java・Python・Ruby・Ruby on Rails・Linux・CentOS・Ubuntu"
  end

  # --- URL正規化 ---

  def test_urls_are_normalized_absolute_without_trailing_slash_or_query
    parse_fixture.each do |posting|
      assert_match %r{\Ahttps://tech\.hipro-job\.jp/job/\d+\z}, posting.url,
                   "末尾スラッシュなし・クエリなしの正規化されたURLのはず"
    end
  end

  # --- category_hint が引数どおり全件に伝わる ---

  def test_category_hint_is_propagated_to_every_posting
    postings = parse_fixture(category_hint: "React")

    assert_equal 20, postings.size
    assert(postings.all? { |posting| posting.category_hint == "React" },
           "全件のcategory_hintが引数のReactになるはず")
  end

  # --- description は EngineerClassifier に効く形になっている ---
  # 分類器の判定テキストは title + description + skills だけで、tagsは渡らない。
  # 稼働頻度とこだわり条件をdescriptionに入れている意図が壊れたらここで落ちる。
  def test_description_feeds_remote_and_long_term_signals_to_engineer_classifier
    first = parse_fixture.first
    result = FreelanceJobs::EngineerClassifier.classify(first, today: TODAY)

    assert_equal "Ruby", result.category
    assert_includes result.memo, "リモート可", "こだわり条件がdescriptionに入っていないと出ないmemo"
    assert_includes result.memo, "長期・継続あり", "稼働頻度(週3日)がdescriptionに入っていないと出ないmemo"
  end

  # --- 必須要素が欠けたカードは黙って除外される ---

  # 詳細ページ(/job/{id})にも div.job-card が1個あるが about 属性を持たない。
  def test_parse_skips_card_without_about_attribute
    fragment = <<~HTML
      <div class="node node--type-job job-details job list clearfix job-card">
        <span class="job-list-title">詳細ページ側のカード</span>
      </div>
    HTML

    assert_equal [], FreelanceJobs::Sources::HiproTech.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_skips_card_whose_about_is_not_a_job_path
    fragment = <<~HTML
      <div class="job-card" about="/company/123">
        <span class="job-list-title">案件ではないカード</span>
      </div>
    HTML

    assert_equal [], FreelanceJobs::Sources::HiproTech.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_skips_card_without_title
    fragment = <<~HTML
      <div class="job-card" about="/job/99999">
        <div class="money"><div class="field--name-field-job-salary-low">450,000円</div></div>
      </div>
    HTML

    assert_equal [], FreelanceJobs::Sources::HiproTech.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  # 単価が欠けたカードは除外せず "要確認" で残す（案件としては成立しているため）。
  def test_parse_keeps_card_without_salary_as_unconfirmed_reward
    fragment = <<~HTML
      <div class="job-card" about="/job/99999">
        <span class="job-list-title">単価未掲載の案件</span>
      </div>
    HTML
    posting = FreelanceJobs::Sources::HiproTech.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    refute_nil posting
    assert_equal "要確認", posting.reward
    assert_equal "業務委託（フリーランス）", posting.work_format
    assert_equal "-", posting.application_status
    assert_equal [], posting.skills
    assert_equal "", posting.description
    assert_nil posting.posted_on
  end

  # --- fetch（通信しないFakeフェッチャー） ---

  # 指定したURLごとに本文を返し、呼び出されたURLを記録するFakeフェッチャー。
  # 未登録のURLには既定の本文（同じfixture）を返す。
  class RecordingFetcher
    def initialize(body:, bodies_by_url: {})
      @body = body
      @bodies_by_url = bodies_by_url
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      @bodies_by_url.fetch(url, @body)
    end
  end

  SEARCH_TARGETS = [
    { query: "pg_skill_id%5B9%5D=9", hint: "Ruby" },
    { query: "framework_id%5B125%5D=125", hint: "React" }
  ].freeze

  def test_fetch_requests_each_search_target_and_page_and_deduplicates_urls
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    source = FreelanceJobs::Sources::HiproTech.new(
      fetcher: fetcher, today: TODAY, search_targets: SEARCH_TARGETS, pages: 2, include_expired: true
    )

    postings = source.fetch

    assert_equal [
      "https://tech.hipro-job.jp/job-search?pg_skill_id%5B9%5D=9",
      "https://tech.hipro-job.jp/job-search?pg_skill_id%5B9%5D=9&page=1",
      "https://tech.hipro-job.jp/job-search?framework_id%5B125%5D=125",
      "https://tech.hipro-job.jp/job-search?framework_id%5B125%5D=125&page=1"
    ], fetcher.requested_urls, "絞り込み2件×2ページ＝4リクエスト。1ページ目はpageパラメータ無し"
    assert_equal 20, postings.size, "4ページとも同じ案件が返っても重複排除され20件のままのはず"
  end

  def test_fetch_excludes_expired_postings_by_default
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    source = FreelanceJobs::Sources::HiproTech.new(
      fetcher: fetcher, today: TODAY, search_targets: SEARCH_TARGETS, pages: 1
    )

    postings = source.fetch

    assert_equal ["https://tech.hipro-job.jp/job/51779"], postings.map(&:url),
                 "既定では募集中の案件だけを採用するはず"
  end

  def test_fetch_stops_paging_when_a_page_has_no_card
    empty_page_url = "https://tech.hipro-job.jp/job-search?pg_skill_id%5B9%5D=9&page=1"
    fetcher = RecordingFetcher.new(
      body: read_fixture(FIXTURE_NAME),
      bodies_by_url: { empty_page_url => wrap_html("<p>該当する案件はありません</p>") }
    )
    source = FreelanceJobs::Sources::HiproTech.new(
      fetcher: fetcher, today: TODAY,
      search_targets: [{ query: "pg_skill_id%5B9%5D=9", hint: "Ruby" }], pages: 3, include_expired: true
    )

    postings = source.fetch

    assert_equal [
      "https://tech.hipro-job.jp/job-search?pg_skill_id%5B9%5D=9",
      empty_page_url
    ], fetcher.requested_urls, "カードが0件のページに当たったら以降のページは取りに行かないはず"
    assert_equal 20, postings.size
  end

  # --- Profile::ENGINEER にHiproTechが含まれる（BEGINNERには含まれない） ---

  def test_engineer_profile_includes_hipro_tech_source
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_includes source_classes, FreelanceJobs::Sources::HiproTech
  end

  def test_beginner_profile_does_not_include_hipro_tech_source
    source_classes = FreelanceJobs::Profile::BEGINNER.source_specs.map(&:first)

    refute_includes source_classes, FreelanceJobs::Sources::HiproTech
  end
end
