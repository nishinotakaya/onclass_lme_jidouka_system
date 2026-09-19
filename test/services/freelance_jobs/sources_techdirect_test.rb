# frozen_string_literal: true
# test/services/freelance_jobs/sources_techdirect_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesTechdirectTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 19)
  FIXTURE_NAME = "techdirect_ruby.html"

  def parse_fixture(category_hint: "Ruby")
    FreelanceJobs::Sources::Techdirect.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  # テックダイレクトの一覧カード1件分のHTML片（実データの構造を模したもの）。
  def build_techdirect_card_html(href: "/jobs/99999", title: "テスト案件", organization_name: "株式会社テスト",
                                  reward_text: "4,700 ～ 5,000円/時", business_category: "システム開発・運用、SES",
                                  working_days: "週4日/週5日", work_location: "フルリモート", skills: %w[Ruby AWS],
                                  work_note: "【内 容】テストの業務内容です。")
    skill_badges = skills.map do |skill|
      "<div class=\"skill-badge\"><button type=\"button\"><span>#{skill}</span></button></div>"
    end.join

    <<~HTML
      <section class="list-job-card">
        <div class="job-title"><h3 class="h size-lg"><a href="#{href}">#{title}</a></h3></div>
        <div class="work-note"><section class="codeal-markdown">#{work_note}</section></div>
        <div class="job-requirements">
          <div class="job-requirement-item item"><h4 class="h label">報酬例</h4><div class="value">#{reward_text}</div></div>
          <div class="job-requirement-item item"><h4 class="h label">業務内容</h4><div class="value">#{business_category}</div></div>
          <div class="job-requirement-item item"><h4 class="h label">稼働時間目安</h4><div class="value">#{working_days}</div></div>
          <div class="job-requirement-item item"><h4 class="h label">はたらく場所</h4><div class="value">#{work_location}</div></div>
        </div>
        <div class="job-requirement-item mb-c20"><h4 class="h label">スキル</h4><div class="value"><div class="skill-container">#{skill_badges}</div></div></div>
        <div class="job-org"><a href="/orgs/1/jobs"><span class="label">#{organization_name}</span></a></div>
      </section>
    HTML
  end

  # --- 件数 ---

  def test_parse_fixture_returns_ten_postings
    assert_equal 10, parse_fixture.size, "1ページ10件固定のはず"
  end

  # --- 1件目の全フィールド ---
  # 1件目は keyword=Ruby の検索結果だが、必須スキルはReactで尚可スキルにRubyがあるだけの案件。
  # あいまい一致の性質どおり弾かれずに返ってくることの確認も兼ねる。

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "テックダイレクト", first.site
    assert_equal "https://techdirect.jp/jobs/89942", first.url
    assert_equal "システム移行に伴うフロントエンドエンジニア（React）", first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "時給 4,700〜5,000円", first.reward
    assert_equal "時間単価制", first.work_format,
                 "形式列は他サイトと同じ契約形態の語彙に揃える（稼働日数を入れない）"
    assert_includes first.description, "稼働: 週4日/週5日",
                    "稼働日数はdescriptionに残す（EngineerClassifierの週\\d日判定がここを見る）"
    assert_equal ["React", "技術選定", "Ruby", "AWS", "フロントエンド開発"], first.skills
    assert_equal "株式会社ACWEB", first.client
    assert_equal [], first.tags, "特徴タグに相当する項目がサイトに無いので空配列のはず"
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_nil first.posted_on
  end

  # descriptionは「業務内容（本文ブロック） / 案件区分 / 稼働 / 勤務地」の順に連結される。
  def test_parse_first_posting_description_joins_work_note_and_requirement_values
    description = parse_fixture.first.description

    assert description.start_with?("業務内容: 【案件名】 フロントエンド案件"),
           "先頭は本文ブロックのはず: #{description}"
    assert_includes description, "案件区分: システム開発・運用、SES"
    assert_includes description, "稼働: 週4日/週5日"
    assert_includes description, "勤務地: リモート不可（常駐）/東京都"
    refute_match(/\n/, description, "normalize_descriptionで改行が畳まれているはず")
  end

  # --- 単価が範囲なしのカード ---

  def test_reward_without_a_range_still_parses
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/94257") }

    refute_nil posting
    assert_equal "時給 5,900円", posting.reward
  end

  # --- URL正規化 ---

  def test_urls_are_normalized_absolute_job_detail_urls
    parse_fixture.each do |posting|
      assert_match %r{\Ahttps://techdirect\.jp/jobs/\d+\z}, posting.url,
                   "絶対URL・/jobs/<ID>形式に正規化されているはず"
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
      <section class="list-job-card">
        <div class="job-title"><h3 class="h size-lg">リンクなし</h3></div>
      </section>
    HTML

    assert_equal [], FreelanceJobs::Sources::Techdirect.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_skips_cards_whose_title_link_is_not_a_job_detail_path
    fragment = build_techdirect_card_html(href: "/jobs/search?keyword=Ruby")

    assert_equal [], FreelanceJobs::Sources::Techdirect.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  # --- 報酬・企業名が欠けても落ちない ---

  def test_posting_without_reward_falls_back_to_placeholder
    fragment = build_techdirect_card_html(href: "/jobs/11111", reward_text: "")

    posting = FreelanceJobs::Sources::Techdirect.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    refute_nil posting
    assert_equal "要確認", posting.reward
    assert_equal "業務委託（フリーランス）", posting.work_format,
                 "単価が読めないカードは時間単価制と断定せず、既定の契約形態にする"
  end

  def test_posting_without_organization_name_has_empty_client
    fragment = <<~HTML
      <section class="list-job-card">
        <div class="job-title"><h3 class="h size-lg"><a href="/jobs/22222">企業名なし案件</a></h3></div>
        <div class="job-requirements">
          <div class="job-requirement-item item"><h4 class="h label">報酬例</h4><div class="value">4,000円/時</div></div>
        </div>
      </section>
    HTML

    posting = FreelanceJobs::Sources::Techdirect.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    refute_nil posting
    assert_equal "", posting.client
    assert_equal [], posting.skills
    assert_equal "時間単価制", posting.work_format,
                 "企業名が無くても、単価が時給建てなら形式は時間単価制になる"
  end

  # --- 同一URLの重複は1件に畳む ---

  def test_parse_deduplicates_same_url
    fragment = build_techdirect_card_html(href: "/jobs/33333") + build_techdirect_card_html(href: "/jobs/33333")

    postings = FreelanceJobs::Sources::Techdirect.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")

    assert_equal 1, postings.size
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
      page_number = url[/[?&]p=(\d+)/, 1]&.to_i || 1
      @bodies_by_page.fetch(page_number, "")
    end
  end

  def test_fetch_requests_each_keyword_and_page_and_deduplicates_urls
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    search_targets = [
      { keyword: "Ruby", hint: "Ruby" },
      { keyword: "TypeScript", hint: "TypeScript" }
    ]
    source = FreelanceJobs::Sources::Techdirect.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, pages_per_keyword: 3
    )

    postings = source.fetch

    assert_equal 6, fetcher.requested_urls.size, "2キーワード×3ページぶん取得するはず"
    assert_equal 10, postings.size, "同じ案件が複数キーワード・複数ページで返っても重複排除されるはず"
  end

  def test_fetch_builds_keyword_and_p_query_from_page_one
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    source = FreelanceJobs::Sources::Techdirect.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ keyword: "Ruby on Rails", hint: "Ruby" }], pages_per_keyword: 2
    )

    source.fetch

    assert_equal [
      "https://techdirect.jp/jobs?keyword=Ruby+on+Rails&p=1",
      "https://techdirect.jp/jobs?keyword=Ruby+on+Rails&p=2"
    ], fetcher.requested_urls, "page=ではなくp=で、1ページ目からp=1を付けるはず"
  end

  def test_fetch_stops_paging_when_a_page_has_no_cards
    fetcher = PagedFetcher.new(bodies_by_page: { 1 => read_fixture(FIXTURE_NAME), 2 => wrap_html("<p>該当する案件はありません</p>") })
    source = FreelanceJobs::Sources::Techdirect.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ keyword: "Ruby", hint: "Ruby" }], pages_per_keyword: 3
    )

    postings = source.fetch

    assert_equal 2, fetcher.requested_urls.size, "0件ページに当たったら3ページ目は取りに行かないはず"
    assert_equal 10, postings.size
  end

  # --- 分類器に渡したときReact案件として判定できる（あいまい一致の性質どおり分類は後段任せ）---

  def test_first_posting_is_classified_as_react
    result = FreelanceJobs::EngineerClassifier.classify(parse_fixture.first, today: TODAY)

    assert_equal "React", result.category
  end
end
