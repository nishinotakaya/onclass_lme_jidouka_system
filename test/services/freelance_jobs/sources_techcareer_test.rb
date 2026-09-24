# frozen_string_literal: true
# test/services/freelance_jobs/sources_techcareer_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesTechcareerTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 23)
  FIXTURE_NAME = "techcareer_ruby.html"

  def parse_fixture(category_hint: "Ruby")
    FreelanceJobs::Sources::Techcareer.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  # 一覧カード1件分のHTML片（実データ 2026-09-23 実測のDOM構造を模したもの）。
  # 募集終了カードは h3.new-card__title の中に div.title-tag が入る（募集中カードには無い）。
  def build_techcareer_card_html(id: 99999, title: "テスト案件", closed: false,
                                  tags: %w[リモート可], reward_amount: "〜880,000", reward_unit: "円/月",
                                  reward_time_span: "（140時間 ~ 180時間）",
                                  job_class: "サーバーサイドエンジニア", contract_type: "業務委託（フリーランス）",
                                  expected_annual_income: "10,560,000円",
                                  skills: %w[Ruby TypeScript], business_content: "テストの業務内容です。",
                                  required_skill: "・テストに必要なスキルです。")
    title_tag_html = closed ? '<div class="title-tag">募集終了</div>' : ""
    tag_html = tags.map { |tag| "<a class=\"textGroup tag-label js--tag-title\" href=\"javascript:void(0)\">#{tag}</a>" }.join
    skill_html = skills.map { |skill| "<span class=\"languages\">#{skill}</span>" }.join

    <<~HTML
      <div class="pageResult mb40 new-card">
        <div class="card-job-tag"></div>
        <div class="card-job-title">
          <a target="_blank" href="/projects/detail/#{id}/">
            <h3 class="new-card__title">
              #{title_tag_html}
              <div class="content-title">
                <span class="title-job">#{title}</span><span class="title-desc">の案件・求人</span>
              </div>
            </h3>
          </a>
        </div>
        <div class="card-job-body">
          <div class="pageResult_header"><div class="pageResult_top"><div class="pageResult_top_groupLeft">#{tag_html}</div></div></div>
          <div class="new-card-tbl">
            <div class="new-card-tbl__item">
              <div class="new-card-tbl__item__icon">単価(税込)</div>
              <div class="new-card-tbl__item__text"><strong><span class="text-golden"><span class="text-amount">#{reward_amount}</span><span class="text-salary">#{reward_unit}</span></span></strong><span class="time_span">#{reward_time_span}</span></div>
            </div>
            <div class="new-card-tbl__item">
              <div class="new-card-tbl__item__icon"><span class="text-item">想定年収(税込)</span></div>
              <div class="new-card-tbl__item__text">#{expected_annual_income}</div>
            </div>
            <div class="new-card-tbl__item">
              <div class="new-card-tbl__item__icon">契約形態</div>
              <div class="new-card-tbl__item__text">#{contract_type}</div>
            </div>
            <div class="new-card-tbl__item">
              <div class="new-card-tbl__item__icon">職種</div>
              <div class="new-card-tbl__item__text">#{job_class}</div>
            </div>
            <div class="new-card-content" id="#{id}">
              <div class="new-card-tbl__item full">
                <div class="new-card-tbl__item__icon">開発環境</div>
                <div class="new-card-tbl__item__text">#{skill_html}</div>
              </div>
              <div class="new-card-tbl__item full">
                <div class="new-card-tbl__item__icon">業務内容</div>
                <div class="new-card-tbl__item__text"><p>#{business_content}</p></div>
              </div>
              <div class="new-card-tbl__item full last">
                <div class="new-card-tbl__item__icon">必須スキル</div>
                <div class="new-card-tbl__item__text"><p>#{required_skill}</p></div>
              </div>
            </div>
          </div>
        </div>
      </div>
    HTML
  end

  # --- 件数 ---

  def test_parse_fixture_returns_four_postings
    assert_equal 4, parse_fixture.size, "フィクスチャは募集中3件+募集終了1件の計4カードに切り詰めてあるはず"
  end

  # --- 1件目（募集中）の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "テクフリ", first.site
    assert_equal "https://freelance.techcareer.jp/projects/detail/34846", first.url
    assert_equal "Ruby/Java/大手銀行向けアプリ開発のPL", first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "〜880,000円/月（140時間 ~ 180時間）", first.reward
    assert_equal "月額制（業務委託）", first.work_format
    assert_equal ["Java", "JavaScript", "Ruby", "TypeScript"], first.skills
    assert_equal ["フレックス制", "リモート可"], first.tags
    assert_equal "", first.client
    assert_equal "募集中", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_nil first.posted_on, "一覧に掲載日が無いためnilのはず"
  end

  def test_parse_first_posting_description_includes_labeled_parts
    description = parse_fixture.first.description

    assert_includes description, "職種: サーバーサイドエンジニア"
    assert_includes description, "契約形態: 業務委託（フリーランス）"
    assert_includes description, "想定年収: 10,560,000円"
    assert_includes description, "業務内容: 大手銀行向けの資産情報を統合・可視化するシステム構築プロジェクトにて"
    assert_includes description, "必須スキル: ・PLとしての実務経験"
    refute_match(/\n/, description, "normalize_descriptionで改行が畳まれているはず")
  end

  # --- 募集終了カード（AC-02: 落とさずCLOSED_STATUSで返す）---

  def test_parse_includes_closed_posting_with_closed_status
    closed = parse_fixture.find { |posting| posting.url.end_with?("/34953") }

    refute_nil closed, "募集終了カードも除外せずに返すはず"
    assert_equal FreelanceJobs::JobPosting::CLOSED_STATUS, closed.application_status
    assert closed.closed?
  end

  def test_parse_open_postings_have_open_status
    parse_fixture.reject { |posting| posting.url.end_with?("/34953") }.each do |posting|
      assert_equal "募集中", posting.application_status
    end
  end

  # --- 単価・タグ ---

  def test_reward_includes_digits_and_unit
    parse_fixture.each do |posting|
      assert_match(/\d/, posting.reward)
    end
  end

  def test_tags_include_remote_label_for_every_posting
    parse_fixture.each do |posting|
      assert_includes posting.tags, "リモート可"
    end
  end

  # --- 形式（work_format） ---

  def test_work_format_is_monthly_when_reward_has_month_unit
    parse_fixture.each do |posting|
      assert_equal "月額制（業務委託）", posting.work_format
    end
  end

  def test_work_format_is_hourly_when_reward_has_hourly_unit
    fragment = build_techcareer_card_html(id: 11111, reward_amount: "〜4,700", reward_unit: "円/時",
                                           reward_time_span: "")
    posting = FreelanceJobs::Sources::Techcareer.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    refute_nil posting
    assert_equal "時間単価制", posting.work_format
  end

  def test_work_format_falls_back_when_reward_unit_is_unrecognized
    fragment = build_techcareer_card_html(id: 22222, reward_amount: "応相談", reward_unit: "", reward_time_span: "")
    posting = FreelanceJobs::Sources::Techcareer.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    refute_nil posting
    assert_equal "業務委託（フリーランス）", posting.work_format
  end

  # --- URL正規化 ---

  def test_urls_are_normalized_absolute_project_detail_urls
    parse_fixture.each do |posting|
      assert_match %r{\Ahttps://freelance\.techcareer\.jp/projects/detail/\d+\z}, posting.url
    end
  end

  # --- category_hint が引数どおり全件に伝わる ---

  def test_category_hint_is_propagated_to_every_posting
    postings = parse_fixture(category_hint: "TypeScript")

    assert(postings.all? { |posting| posting.category_hint == "TypeScript" },
           "全件のcategory_hintが引数の値になるはず")
  end

  # --- 必須要素が欠けたカードは黙って除外する ---

  def test_parse_skips_cards_without_a_title
    fragment = <<~HTML
      <div class="pageResult mb40 new-card">
        <div class="card-job-title"><a href="/projects/detail/1/"><h3 class="new-card__title"><div class="content-title"></div></h3></a></div>
      </div>
    HTML

    assert_equal [], FreelanceJobs::Sources::Techcareer.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_skips_cards_without_a_detail_url
    fragment = <<~HTML
      <div class="pageResult mb40 new-card">
        <div class="card-job-title"><h3 class="new-card__title"><div class="content-title"><span class="title-job">URLなし案件</span></div></h3></div>
      </div>
    HTML

    assert_equal [], FreelanceJobs::Sources::Techcareer.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  # --- 同一URLの重複は1件に畳む ---

  def test_parse_deduplicates_postings_with_the_same_url
    fragment = build_techcareer_card_html(id: 33333, title: "A") + build_techcareer_card_html(id: 33333, title: "B")

    postings = FreelanceJobs::Sources::Techcareer.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")

    assert_equal 1, postings.size
    assert_equal "A", postings.first.title, "先に出てきた方を残すはず"
  end

  # --- タグ・スキルが欠けても落ちない ---

  def test_parse_does_not_raise_when_tags_and_skills_are_missing
    fragment = build_techcareer_card_html(id: 44444, tags: [], skills: [])

    postings = FreelanceJobs::Sources::Techcareer.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")

    assert_equal 1, postings.size
    posting = postings.first
    assert_equal [], posting.tags
    assert_equal [], posting.skills
  end

  # --- 定数 ---

  def test_site_name_constant
    assert_equal "テクフリ", FreelanceJobs::Sources::Techcareer::SITE_NAME
  end

  def test_request_interval_constant
    assert_equal 1.5, FreelanceJobs::Sources::Techcareer::REQUEST_INTERVAL
  end

  # 2026-09-23実測: Ruby/TypeScriptに加えReact単体の一覧ページも200で存在するため3ターゲット。
  def test_default_search_targets_constant
    assert_equal [
      { skill_slug: "ruby", hint: "Ruby" },
      { skill_slug: "typescript", hint: "TypeScript" },
      { skill_slug: "react", hint: "React" }
    ], FreelanceJobs::Sources::Techcareer::DEFAULT_SEARCH_TARGETS
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

  # パス末尾のページ番号(/pN/)ごとに返すbodyを切り替えるFakeフェッチャー（該当0件ページ到達の再現用）。
  class PagedFetcher
    def initialize(bodies_by_page:)
      @bodies_by_page = bodies_by_page
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      page_number = url[%r{/p(\d+)/\z}, 1]&.to_i || 1
      @bodies_by_page.fetch(page_number, "")
    end
  end

  def test_fetch_requests_each_skill_and_page_and_deduplicates_urls
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    search_targets = [
      { skill_slug: "ruby", hint: "Ruby" },
      { skill_slug: "typescript", hint: "TypeScript" }
    ]
    source = FreelanceJobs::Sources::Techcareer.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, pages_per_skill: 2
    )

    postings = source.fetch

    assert_equal 4, fetcher.requested_urls.size, "2スキル×2ページぶん取得するはず"
    assert_equal 4, postings.size, "同じ案件が複数スキル・複数ページで返っても重複排除されるはず"
  end

  def test_fetch_builds_path_style_skill_and_page_url
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    source = FreelanceJobs::Sources::Techcareer.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ skill_slug: "ruby", hint: "Ruby" }], pages_per_skill: 2
    )

    source.fetch

    assert_equal [
      "https://freelance.techcareer.jp/projects/skills/ruby/",
      "https://freelance.techcareer.jp/projects/skills/ruby/p2/"
    ], fetcher.requested_urls, "1ページ目はp2等を付けず、2ページ目以降は/p<N>/というパス型にするはず"
  end

  def test_fetch_stops_paging_when_a_page_has_no_cards
    fetcher = PagedFetcher.new(
      bodies_by_page: { 1 => read_fixture(FIXTURE_NAME), 2 => wrap_html("<p>該当する案件はありません</p>") }
    )
    source = FreelanceJobs::Sources::Techcareer.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ skill_slug: "ruby", hint: "Ruby" }], pages_per_skill: 3
    )

    postings = source.fetch

    assert_equal 2, fetcher.requested_urls.size, "0件ページに当たったら3ページ目は取りに行かないはず"
    assert_equal 4, postings.size
  end

  def test_fetch_requests_only_pages_per_skill_pages
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    source = FreelanceJobs::Sources::Techcareer.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ skill_slug: "ruby", hint: "Ruby" }]
    )

    source.fetch

    assert_equal 2, fetcher.requested_urls.size, "既定のpages_per_skillは2のはず（=1スキル2ページまで）"
  end

  # --- 分類器に渡したときRuby案件として判定できる（descriptionの作りの検証）---

  def test_first_posting_is_classified_as_ruby
    result = FreelanceJobs::EngineerClassifier.classify(parse_fixture.first, today: TODAY)

    assert_equal "Ruby", result.category
  end
end
