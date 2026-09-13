# frozen_string_literal: true
# test/services/freelance_jobs/sources_potepan_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesPotepanTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 12)
  FIXTURE_NAME = "potepan_ruby.html"

  def parse_fixture(category_hint: "Ruby")
    FreelanceJobs::Sources::Potepan.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  # ポテパンの一覧カード1件分のHTML片（実データの構造を模したもの）。
  # 単価の「~」「/」はASCII、単位が<span>で分離されている点まで実物に合わせている。
  def build_potepan_card_html(href: "/projects/dummy-card", title: "テスト案件", price_html: "<span class=\"single-project__price--prefix\">~</span>600,000<span class=\"single-project__price--unit\">円/月</span>",
                              skills: [], features: [], work_location: "フルリモート駅")
    skill_links = skills.map { |skill| "<a href=\"/project/skill-1\">#{skill}</a>" }.join(" / ")
    feature_links = features.map { |feature| "<a href=\"/projects?feature=1\">#{feature}</a>" }.join(" / ")
    price_area = price_html ? "<p class=\"single-project__price\">#{price_html}</p>" : ""

    <<~HTML
      <section class="single-project">
        <h3><a class="single-project__title" href="#{href}">
          #{title}
        </a></h3>
        <div class="single-project__price-area">#{price_area}</div>
        <div class="single-project__data-area">
          <dl><dt class="single-project__content">業務内容</dt><dd>テストの業務内容です。</dd></dl>
          <dl><dt class="single-project__location">勤務地</dt><dd>#{work_location}</dd></dl>
          <dl><dt class="single-project__required">必須スキル</dt><dd>・テストの必須スキル</dd></dl>
          <dl><dt class="single-project__feature">特徴</dt><dd>#{feature_links}</dd></dl>
          <dl><dt class="single-project__skill">キーワード</dt><dd>#{skill_links}</dd></dl>
        </div>
      </section>
    HTML
  end

  # ページャ1つ分のHTML片。実DOMどおり現在ページはspan、他ページはaで出し、
  # 1ページ目だけpageパラメータが付かない点まで合わせている。
  def build_potepan_pagination_html(keyword: "Ruby", current_page: 1, linked_pages: [2])
    links = linked_pages.map do |page_number|
      href = page_number == 1 ? "/projects?keyword=#{keyword}" : "/projects?keyword=#{keyword}&amp;page=#{page_number}"
      "<a class=\"page-numbers\" href=\"#{href}\">#{page_number}</a>"
    end.join

    "<nav class=\"pagination\" role=\"navigation\" aria-label=\"pager\">" \
      "<span class=\"page-numbers current\">#{current_page}</span>#{links}</nav>"
  end

  # --- 件数 ---

  def test_parse_fixture_returns_ten_postings
    assert_equal 10, parse_fixture.size, "1ページ10件固定のはず"
  end

  # --- 1件目の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "ポテパンフリーランス", first.site
    assert_equal "https://freelance.potepan.com/projects/92c42bc2-5ac9-42fb", first.url
    assert_equal "AIサービス開発におけるWebエンジニア募集（Ruby / Vue）", first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "〜600,000円／月", first.reward
    assert_equal "月額制（業務委託）", first.work_format
    assert_equal ["Ruby", "Vue", "Nuxt", "AI"], first.skills
    assert_equal ["リモート勤務可"], first.tags
    assert_equal "", first.client
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_nil first.posted_on
  end

  # descriptionは「業務内容 / 必須スキル / 勤務地 / 担当者コメント」の順に連結される。
  # 必須スキル欄の技術名がEngineerClassifierの判定材料になるため、含まれることを保証する。
  def test_parse_first_posting_description_joins_definition_list_and_recommend_comment
    description = parse_fixture.first.description

    assert description.start_with?("業務内容: FoodTech系のAIプロダクト開発をご担当いただきます。"),
           "先頭は業務内容のはず: #{description}"
    assert_includes description, "必須スキル: ・Ruby on RailsでのWebアプリケーション開発経験（3年以上）"
    assert_includes description, "勤務地: フルリモート駅"
    assert_includes description, "担当者コメント: ★フルリモート（地方在住の方も大歓迎）"
    refute_match(/\n/, description, "normalize_descriptionで改行が畳まれているはず")
  end

  # --- 単価が無いカード（単価欄ごと空の実データ）---

  def test_posting_without_price_falls_back_to_placeholder_reward
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/7b3b222a-c760-4bbc") }

    refute_nil posting
    assert_equal "受託開発案件", posting.title
    assert_equal "要確認", posting.reward
    assert_equal "業務委託（フリーランス）", posting.work_format
    assert_equal [], posting.tags, "特徴の定義リストが無いカードは空配列になるはず"
  end

  # --- work_format が単価の単位で分岐する ---

  def test_work_format_is_monthly_when_price_unit_is_per_month
    postings = parse_fixture

    assert(postings.select { |posting| posting.reward != "要確認" }.all? { |posting| posting.work_format == "月額制（業務委託）" },
           "フィクスチャの単価付きカードは全て月額表記のはず")
  end

  def test_work_format_is_hourly_when_price_unit_is_per_hour
    fragment = build_potepan_card_html(
      href: "/projects/hourly-card",
      price_html: "<span class=\"single-project__price--prefix\">~</span>7,000<span class=\"single-project__price--unit\">円/時</span>"
    )
    posting = FreelanceJobs::Sources::Potepan.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    assert_equal "時間単価制", posting.work_format
    assert_equal "〜7,000円／時", posting.reward, "表示用の単価は全角の「〜」「／」に揃えるはず"
  end

  # --- URL正規化 ---

  def test_urls_are_normalized_absolute_without_trailing_slash_or_query
    parse_fixture.each do |posting|
      assert_match %r{\Ahttps://freelance\.potepan\.com/projects/[A-Za-z0-9-]+\z}, posting.url,
                   "絶対URL・末尾スラッシュなし・クエリなしに正規化されているはず"
    end
  end

  # 案件IDはUUID風(8-4-4)とSalesforce風18桁が混在する。正規表現で絞らずhrefをそのまま使う。
  def test_urls_keep_both_uuid_style_and_salesforce_style_ids
    urls = parse_fixture.map(&:url)

    assert_includes urls, "https://freelance.potepan.com/projects/92c42bc2-5ac9-42fb"
    assert_includes urls, "https://freelance.potepan.com/projects/a0K7F00001eEwSkUAK"
  end

  # --- category_hint が引数どおり全件に伝わる ---

  def test_category_hint_is_propagated_to_every_posting
    postings = parse_fixture(category_hint: "TypeScript")

    assert(postings.all? { |posting| posting.category_hint == "TypeScript" },
           "全件のcategory_hintが引数の値になるはず")
  end

  # --- skills / tags ---

  # ddのテキストは " / " 区切りだが、技術名自体が "/" を含む可能性があるためa要素を個別に読む。
  def test_skills_are_collected_from_anchor_elements
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/9658011a-c163-4f5c") }

    assert_equal ["Ruby", "Ruby on Rails", "TypeScript", "React"], posting.skills
  end

  def test_skills_are_empty_when_keyword_definition_list_has_no_links
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/a0K7F00001eEwSkUAK") }

    assert_equal [], posting.skills, "キーワード欄が空のカードは空配列になるはず"
    assert_includes posting.description, "必須スキル: ・RubyOnRailsで作られたWebアプリケーション",
                    "skillsが空でも必須スキルがdescriptionに残り分類できるはず"
  end

  def test_tags_are_collected_from_feature_definition_list
    posting = parse_fixture.first

    assert_equal ["リモート勤務可"], posting.tags
  end

  # 勤務地のddはリンク版と素テキスト版が混在するため、a要素ではなくdd全体のテキストを使う。
  def test_work_location_is_read_from_definition_text_even_without_links
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/a0K7F00001eEwSkUAK") }

    assert_includes posting.description, "勤務地: フルリモート"
  end

  # --- 必須要素が欠けたカードは黙って除外する ---

  def test_parse_skips_cards_without_a_title_link
    fragment = <<~HTML
      <section class="single-project">
        <h3 class="single-project__title">リンクなし（詳細ページのh3と同じ形）</h3>
        <div class="single-project__data-area">
          <dl><dt class="single-project__content">業務内容</dt><dd>本文</dd></dl>
        </div>
      </section>
    HTML

    assert_equal [], FreelanceJobs::Sources::Potepan.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_skips_cards_whose_title_link_has_empty_href
    fragment = build_potepan_card_html(href: "")

    assert_equal [], FreelanceJobs::Sources::Potepan.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
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

  # ページ番号ごとに返すbodyを切り替えるFakeフェッチャー（末尾ページ超過の再現用）。
  class PagedFetcher
    def initialize(bodies_by_page:)
      @bodies_by_page = bodies_by_page
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      page_number = url[/&page=(\d+)/, 1]&.to_i || 1
      @bodies_by_page.fetch(page_number, "")
    end
  end

  def test_fetch_requests_each_keyword_and_page_and_deduplicates_urls
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    search_targets = [
      { keyword: "Ruby", hint: "Ruby" },
      { keyword: "TypeScript", hint: "TypeScript" }
    ]
    source = FreelanceJobs::Sources::Potepan.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, pages_per_keyword: 3
    )

    postings = source.fetch

    assert_equal 6, fetcher.requested_urls.size, "2キーワード×3ページぶん取得するはず"
    assert_equal 10, postings.size, "同じ案件が複数キーワード・複数ページで返っても重複排除されるはず"
  end

  def test_fetch_builds_keyword_and_page_query
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    source = FreelanceJobs::Sources::Potepan.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ keyword: "Ruby on Rails", hint: "Ruby" }], pages_per_keyword: 2
    )

    source.fetch

    assert_equal [
      "https://freelance.potepan.com/projects?keyword=Ruby+on+Rails",
      "https://freelance.potepan.com/projects?keyword=Ruby+on+Rails&page=2"
    ], fetcher.requested_urls, "1ページ目はpageを付けず、2ページ目以降だけ&page=Nを付けるはず"
  end

  def test_fetch_stops_paging_when_a_page_has_no_cards
    fetcher = PagedFetcher.new(bodies_by_page: { 1 => read_fixture(FIXTURE_NAME), 2 => wrap_html("<p>該当する案件はありません</p>") })
    source = FreelanceJobs::Sources::Potepan.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ keyword: "Ruby", hint: "Ruby" }], pages_per_keyword: 3
    )

    postings = source.fetch

    assert_equal 2, fetcher.requested_urls.size, "0件ページに当たったら3ページ目は取りに行かないはず"
    assert_equal 10, postings.size
  end

  # --- ページャで末尾を判定する（存在しないページ番号はHTTP 302になるため踏んではいけない）---

  # 実フィクスチャ（1ページ目）のページャは2・3・4をリンクしている。
  # ページャは現在ページの近傍しか並べないので、隣のページ番号だけを見て末尾を判定する。
  def test_next_page_linked_reads_the_adjacent_page_number_from_the_pager
    body = read_fixture(FIXTURE_NAME)

    assert FreelanceJobs::Sources::Potepan.next_page_linked?(body, 1), "1ページ目のページャは2ページ目をリンクしているはず"
    assert_equal false, FreelanceJobs::Sources::Potepan.next_page_linked?(body, 4),
                 "リンクに無いページ番号の次（5）は存在しない扱いにするはず"
  end

  # 最終ページのページャは前のページしかリンクしない（実測: Flutter検索の3ページ目）。
  def test_next_page_linked_is_false_on_the_last_page
    body = wrap_html(build_potepan_pagination_html(current_page: 3, linked_pages: [1, 2]))

    assert_equal false, FreelanceJobs::Sources::Potepan.next_page_linked?(body, 3)
  end

  # 1ページに収まる検索結果ではnav.pagination自体が出力されない。
  def test_next_page_linked_is_false_when_the_page_has_no_pager
    body = wrap_html(build_potepan_card_html)

    assert_equal false, FreelanceJobs::Sources::Potepan.next_page_linked?(body, 1)
  end

  def test_fetch_stops_paging_at_the_last_page_even_when_it_still_has_cards
    last_page_body = wrap_html(
      build_potepan_card_html(href: "/projects/last-page-card") +
      build_potepan_pagination_html(current_page: 2, linked_pages: [1])
    )
    fetcher = PagedFetcher.new(bodies_by_page: { 1 => read_fixture(FIXTURE_NAME), 2 => last_page_body })
    source = FreelanceJobs::Sources::Potepan.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ keyword: "Ruby", hint: "Ruby" }], pages_per_keyword: 3
    )

    postings = source.fetch

    assert_equal 2, fetcher.requested_urls.size,
                 "最終ページのページャに次ページのリンクが無ければ3ページ目は要求しないはず（存在しないページはHTTP 302）"
    assert_equal 11, postings.size, "最終ページのカードは取り込んだうえで打ち切るはず"
  end

  def test_fetch_requests_only_one_page_when_the_result_fits_in_a_single_page
    fetcher = RecordingFetcher.new(body: wrap_html(build_potepan_card_html))
    source = FreelanceJobs::Sources::Potepan.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ keyword: "Ruby", hint: "Ruby" }], pages_per_keyword: 3
    )

    postings = source.fetch

    assert_equal ["https://freelance.potepan.com/projects?keyword=Ruby"], fetcher.requested_urls,
                 "ページャが無い＝1ページで終わりなので2ページ目は要求しないはず"
    assert_equal 1, postings.size
  end

  # --- 分類器に渡したときRuby案件として判定できる（descriptionの作りの検証）---

  def test_first_posting_is_classified_as_ruby
    result = FreelanceJobs::EngineerClassifier.classify(parse_fixture.first, today: TODAY)

    assert_equal "Ruby", result.category
  end

  # --- Profile::ENGINEER にPotepanが含まれる（BEGINNERには含まれない）---

  def test_engineer_profile_includes_potepan_source
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_includes source_classes, FreelanceJobs::Sources::Potepan
  end

  def test_beginner_profile_does_not_include_potepan_source
    source_classes = FreelanceJobs::Profile::BEGINNER.source_specs.map(&:first)

    refute_includes source_classes, FreelanceJobs::Sources::Potepan
  end
end
