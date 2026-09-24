# frozen_string_literal: true
# test/services/freelance_jobs/sources_midworks_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesMidworksTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 23)
  FIXTURE_NAME = "midworks_ruby.html"

  def parse_fixture(category_hint: "Ruby")
    FreelanceJobs::Sources::Midworks.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  # 一覧カード1件分のHTML片（実データ 2026-09-23 実測のDOM構造を模したもの）。
  # h2.p-jobSummaryBoard__title の中にはNEWラベル(span)と案件名のa要素が1つだけあり、
  # カード先頭にも同じhrefを指す空のa要素が別途ある（実データどおり）。
  def build_midworks_card_html(href: "/projects/99999", title: "テスト案件", new_label: true,
                                salary_min_man: 70, salary_max_man: 100,
                                workplace: "東京都港区 / 六本木", skills: %w[Ruby TypeScript],
                                description: "【案件概要】テストの案件概要です。")
    new_label_html = new_label ? '<span class="c-newLabel p-jobSummaryBoard__newLabel">NEW</span>' : ""
    salary_html = if salary_min_man && salary_max_man
                    "<span><b>#{salary_min_man}</b>万</span>〜<span><b>#{salary_max_man}</b>万</span>円/月"
                  else
                    ""
                  end
    skill_items = skills.map { |skill| "<li><a href=\"/projects/skills/1\">#{skill}</a></li>" }.join

    <<~HTML
      <li>
        <div class="p-jobSummaryBoard">
          <a href="#{href}"></a>
          <h2 class="p-jobSummaryBoard__title">
            #{new_label_html}
            <a href="#{href}" style="text-decoration: none; color: #22252A;">#{title}</a>
          </h2>
          <p class="p-jobSummaryBoard__salary">#{salary_html}</p>
          <ul class="p-jobSummaryBoard__hotTagList"></ul>
          <dl class="p-jobSummaryBoard__descriptionList">
            <div>
              <dt class="-workplace"><span class="md:u-hidden">勤務地</span></dt>
              <dd>#{workplace}</dd>
            </div>
            <div>
              <dt class="-developmentEnvironment"><span class="md:u-hidden">スキル</span></dt>
              <dd class="-workplace"><ul class="-workplace__list__dev">#{skill_items}</ul></dd>
            </div>
            <div>
              <dt class="-business"><span class="md:u-hidden">業務内容</span></dt>
              <dd class="p-jobSummaryBoard__descriptionListLastChild">#{description}</dd>
            </div>
          </dl>
        </div>
      </li>
    HTML
  end

  def wrap_midworks_list(fragment)
    wrap_html(%(<ul id="projectIndexList" class="p-projects__archivesList">#{fragment}</ul>))
  end

  # --- 件数 ---

  def test_parse_fixture_returns_four_postings
    assert_equal 4, parse_fixture.size, "フィクスチャは4カードに切り詰めてあるはず"
  end

  # --- 1件目の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "Midworks", first.site
    assert_equal "https://mid-works.com/projects/57455", first.url
    assert_equal "【Ruby】タレントマネジメント向けスキル・資格・研修管理Webアプリケーション開発", first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "700,000〜1,000,000円／月", first.reward, "70万〜100万円/月を万→円に変換するはず"
    assert_equal "月額制（業務委託）", first.work_format
    assert_equal ["Ruby", "JavaScript(React)", "TypeScript"], first.skills
    assert_equal ["NEW"], first.tags, "NEWラベル付きカードはtagsに\"NEW\"が入るはず"
    assert_equal "", first.client
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_nil first.posted_on, "一覧に掲載日が無いためnilのはず"
  end

  # 業務内容欄（改行入りの長文）がdescriptionに入り、normalize_descriptionで改行が畳まれる。
  def test_parse_first_posting_description_includes_business_content
    description = parse_fixture.first.description

    assert_includes description, "バックオフィス領域の事業拡大に伴い"
    assert_includes description, "タレントマネジメントプロダクトのバックエンド開発"
    refute_match(/\n/, description, "normalize_descriptionで改行が畳まれているはず")
  end

  # 勤務地はdescriptionに含める（他サイトと同じくEngineerClassifierの判定材料にするため）。
  def test_parse_first_posting_description_includes_workplace
    description = parse_fixture.first.description

    assert_includes description, "東京都港区 / 六本木"
  end

  # --- NEWラベルの有無 ---

  def test_parse_postings_without_new_label_have_empty_tags
    postings = parse_fixture
    without_new_label = postings.find { |posting| posting.url.end_with?("/57300") }

    refute_nil without_new_label
    assert_equal [], without_new_label.tags
  end

  # --- 単価（万→円変換） ---

  def test_reward_converts_man_yen_to_yen_for_every_posting
    parse_fixture.each do |posting|
      assert_match(/\A[\d,]+〜[\d,]+円／月\z/, posting.reward)
    end
  end

  def test_posting_without_salary_falls_back_to_placeholder_reward
    fragment = build_midworks_card_html(salary_min_man: nil, salary_max_man: nil)
    posting = FreelanceJobs::Sources::Midworks.parse(wrap_midworks_list(fragment), today: TODAY, category_hint: "Ruby").first

    refute_nil posting
    assert_equal "要確認", posting.reward
  end

  # --- URL正規化 ---

  def test_urls_are_normalized_absolute_project_urls
    parse_fixture.each do |posting|
      assert_match %r{\Ahttps://mid-works\.com/projects/\d+\z}, posting.url
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
      <li>
        <div class="p-jobSummaryBoard">
          <h2 class="p-jobSummaryBoard__title">リンクなし案件</h2>
        </div>
      </li>
    HTML

    assert_equal [], FreelanceJobs::Sources::Midworks.parse(wrap_midworks_list(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_skips_cards_whose_title_link_has_empty_href
    fragment = build_midworks_card_html(href: "")

    assert_equal [], FreelanceJobs::Sources::Midworks.parse(wrap_midworks_list(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_skips_cards_with_blank_title_text
    fragment = build_midworks_card_html(title: "")

    assert_equal [], FreelanceJobs::Sources::Midworks.parse(wrap_midworks_list(fragment), today: TODAY, category_hint: "Ruby")
  end

  # --- 同一URLの重複は1件に畳む ---

  def test_parse_deduplicates_postings_with_the_same_url
    fragment = build_midworks_card_html(href: "/projects/1", title: "A") +
               build_midworks_card_html(href: "/projects/1", title: "B")

    postings = FreelanceJobs::Sources::Midworks.parse(wrap_midworks_list(fragment), today: TODAY, category_hint: "Ruby")

    assert_equal 1, postings.size
    assert_equal "A", postings.first.title, "先に出てきた方を残すはず"
  end

  # --- 単価・スキルが欠けても落ちない ---

  def test_parse_does_not_raise_when_salary_and_skills_are_missing
    fragment = build_midworks_card_html(salary_min_man: nil, salary_max_man: nil, skills: [])

    postings = FreelanceJobs::Sources::Midworks.parse(wrap_midworks_list(fragment), today: TODAY, category_hint: "Ruby")

    assert_equal 1, postings.size
    posting = postings.first
    assert_equal "要確認", posting.reward
    assert_equal [], posting.skills
  end

  # --- 定数 ---

  def test_site_name_constant
    assert_equal "Midworks", FreelanceJobs::Sources::Midworks::SITE_NAME
  end

  def test_request_interval_constant
    assert_equal 1.5, FreelanceJobs::Sources::Midworks::REQUEST_INTERVAL
  end

  def test_default_search_targets_constant
    assert_equal [
      { skill_id: 7, hint: "Ruby" },
      { skill_id: 45, hint: "TypeScript" },
      { skill_id: 78, hint: "React" }
    ], FreelanceJobs::Sources::Midworks::DEFAULT_SEARCH_TARGETS
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

  # ページ番号ごとに返すbodyを切り替えるFakeフェッチャー（該当0件ページ到達の再現用）。
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

  def test_fetch_requests_each_skill_and_page_and_deduplicates_urls
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    search_targets = [
      { skill_id: 7, hint: "Ruby" },
      { skill_id: 45, hint: "TypeScript" }
    ]
    source = FreelanceJobs::Sources::Midworks.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, pages_per_skill: 2
    )

    postings = source.fetch

    assert_equal 4, fetcher.requested_urls.size, "2スキル×2ページぶん取得するはず"
    assert_equal 4, postings.size, "同じ案件が複数スキル・複数ページで返っても重複排除されるはず"
  end

  def test_fetch_builds_skill_id_path_url_and_bare_page_query
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    source = FreelanceJobs::Sources::Midworks.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ skill_id: 7, hint: "Ruby" }], pages_per_skill: 2
    )

    source.fetch

    assert_equal [
      "https://mid-works.com/projects/skills/7",
      "https://mid-works.com/projects/skills/7?page=2"
    ], fetcher.requested_urls, "1ページ目はpageを付けず、2ページ目は\"?page=2\"を付けるはず"
  end

  def test_fetch_stops_paging_when_a_page_has_no_cards
    fetcher = PagedFetcher.new(
      bodies_by_page: { 1 => read_fixture(FIXTURE_NAME), 2 => wrap_midworks_list("") }
    )
    source = FreelanceJobs::Sources::Midworks.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ skill_id: 7, hint: "Ruby" }], pages_per_skill: 3
    )

    postings = source.fetch

    assert_equal 2, fetcher.requested_urls.size, "0件ページに当たったら3ページ目は取りに行かないはず"
    assert_equal 4, postings.size
  end

  def test_fetch_requests_only_pages_per_skill_pages
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    source = FreelanceJobs::Sources::Midworks.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ skill_id: 7, hint: "Ruby" }]
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
