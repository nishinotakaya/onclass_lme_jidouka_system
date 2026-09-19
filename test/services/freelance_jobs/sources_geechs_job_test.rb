# frozen_string_literal: true
# test/services/freelance_jobs/sources_geechs_job_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesGeechsJobTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 19)
  FIXTURE_NAME = "geechs_ruby.html"

  def parse_fixture(category_hint: "Ruby")
    FreelanceJobs::Sources::GeechsJob.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  # ギークスジョブの一覧カード1件分のHTML片（実データの構造を模したもの）。
  # 単価が複数spanに分かれている点、募集スキル／ポジションが同じclassの定義リストで
  # ラベルテキストでしか区別できない点まで実物に合わせている。
  def build_geechs_card_html(href: "https://geechs-job.com/project/details/99999", title: "テスト案件",
                              price_html: "<span class=\"project-label\">単価(税抜)</span>" \
                                           "<span class=\"c-text_price\">70</span>" \
                                           "<span class=\"u-text-coral\">〜</span>" \
                                           "<span class=\"c-text_price\">90</span>" \
                                           "<span class=\"c-text_price-unit\">万円/月</span>",
                              location: "四ツ谷", contract: "業務委託契約（フリーランス）",
                              tags: ["安定稼働"], skills: ["Ruby"], positions: ["システムエンジニア（SE）"])
    price_area = price_html ? "<li>#{price_html}</li>" : ""
    location_area = location ? "<li><i class=\"fas fa-map-marker-alt\"></i>#{location}</li>" : ""
    contract_area = contract ? "<li><i class=\"far fa-handshake\"></i>#{contract}</li>" : ""
    tag_links = tags.map { |tag| "<a class=\"p-preferenceIcon_link\" href=\"https://geechs-job.com/project/preferences/dummy\"><span class=\"p-preferenceIcon_text\">#{tag}</span></a>" }.join
    tag_area = tags.empty? ? "" : "<div class=\"project-preference\">#{tag_links}</div>"
    skill_links = skills.map { |skill| "<li class=\"c-category_tab\"><a href=\"https://geechs-job.com/project/dummy\" class=\"c-category_link\">#{skill}</a></li>" }.join
    position_links = positions.map { |position| "<li class=\"c-category_tab\"><a href=\"https://geechs-job.com/project/roles/dummy\" class=\"c-category_link\">#{position}</a></li>" }.join

    <<~HTML
      <li class="c-card p-card-project p-card-project-new">
        <h3 class="c-card_title c-card_block-title project-h3">
          <a class="c-card_title_link" href="#{href}">#{title}</a>
        </h3>
        <ul class="c-card-info01">
          #{price_area}
          #{location_area}
          #{contract_area}
        </ul>
        #{tag_area}
        <dl class="project-detail-table">
          <dt class="project-detail-dt"><span class="project-label">募集スキル</span></dt>
          <dd class="project-detail-dd"><ul class="c-category_tabs">#{skill_links}</ul></dd>
        </dl>
        <dl class="project-detail-table">
          <dt class="project-detail-dt"><span class="project-label">ポジション</span></dt>
          <dd class="project-detail-dd"><ul class="c-category_tabs">#{position_links}</ul></dd>
        </dl>
      </li>
    HTML
  end

  # --- 件数 ---

  def test_parse_fixture_returns_twenty_postings
    assert_equal 20, parse_fixture.size, "1ページ20件固定のはず"
  end

  # --- 1件目の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "ギークスジョブ", first.site
    assert_equal "https://geechs-job.com/project/details/12184", first.url
    assert_equal "Ruby／決済システム開発案件・求人", first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "70〜90万円／月", first.reward
    assert_equal "業務委託契約（フリーランス）", first.work_format
    assert_equal ["Ruby"], first.skills
    assert_equal ["安定稼働", "BtoB", "ベテラン歓迎"], first.tags
    assert_equal "", first.client
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_nil first.posted_on
  end

  # descriptionは「勤務地 / ポジション / 募集スキル」の順に連結される。
  def test_parse_first_posting_description_includes_location_and_position
    description = parse_fixture.first.description

    assert_includes description, "勤務地: 四ツ谷"
    assert_includes description, "ポジション: システムエンジニア（SE）、プログラマ（PG）"
    assert_includes description, "募集スキル: Ruby"
    refute_match(/\n/, description, "normalize_descriptionで改行が畳まれているはず")
  end

  # --- URL正規化 ---

  def test_urls_are_normalized_absolute_project_details_urls
    parse_fixture.each do |posting|
      assert_match %r{\Ahttps://geechs-job\.com/project/details/\d+\z}, posting.url
    end
  end

  # --- category_hint が引数どおり全件に伝わる ---

  def test_category_hint_is_propagated_to_every_posting
    postings = parse_fixture(category_hint: "TypeScript")

    assert(postings.all? { |posting| posting.category_hint == "TypeScript" },
           "全件のcategory_hintが引数の値になるはず")
  end

  # --- Crawl-Delay 5秒の回帰防止 ---

  def test_request_interval_respects_robots_txt_crawl_delay
    assert_operator FreelanceJobs::Sources::GeechsJob::REQUEST_INTERVAL, :>=, 5,
                     "robots.txtのCrawl-Delay: 5を守る間隔でなければならない"
  end

  # --- 必須要素が欠けたカードは黙って除外する ---

  def test_parse_skips_cards_without_a_title_link
    fragment = <<~HTML
      <li class="c-card p-card-project">
        <h3 class="c-card_title c-card_block-title project-h3">リンクなし</h3>
        <ul class="c-card-info01"></ul>
      </li>
    HTML

    assert_equal [], FreelanceJobs::Sources::GeechsJob.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_skips_cards_whose_title_link_has_empty_href
    fragment = build_geechs_card_html(href: "")

    assert_equal [], FreelanceJobs::Sources::GeechsJob.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  # --- 同一URLの重複は畳まれる ---

  def test_parse_dedupes_cards_with_the_same_url
    fragment = build_geechs_card_html(href: "https://geechs-job.com/project/details/1", title: "1件目") +
               build_geechs_card_html(href: "https://geechs-job.com/project/details/1", title: "2件目（同URL）")

    postings = FreelanceJobs::Sources::GeechsJob.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")

    assert_equal 1, postings.size
    assert_equal "1件目", postings.first.title, "先に出てきたカードが優先されるはず"
  end

  # --- 単価欄が無いカードでも落ちない ---

  def test_reward_falls_back_to_placeholder_when_price_is_missing
    fragment = build_geechs_card_html(price_html: nil)

    posting = FreelanceJobs::Sources::GeechsJob.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    assert_equal "要確認", posting.reward
  end

  # --- タグ・ポジションの定義リストが無いカードでも落ちない ---

  def test_tags_and_positions_are_empty_when_blocks_are_missing
    fragment = build_geechs_card_html(tags: [], positions: [])

    posting = FreelanceJobs::Sources::GeechsJob.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    assert_equal [], posting.tags
    refute_includes posting.description, "ポジション:"
  end

  # --- 契約形態欄が無いカードは既定値になる ---

  def test_work_format_defaults_when_contract_is_missing
    fragment = build_geechs_card_html(contract: nil)

    posting = FreelanceJobs::Sources::GeechsJob.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    assert_equal "業務委託（フリーランス）", posting.work_format
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
    source = FreelanceJobs::Sources::GeechsJob.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, pages_per_slug: 1
    )

    postings = source.fetch

    assert_equal 2, fetcher.requested_urls.size, "2スラッグ×1ページぶん取得するはず"
    assert_equal 20, postings.size, "同じ案件が複数スラッグで返っても重複排除されるはず"
  end

  def test_fetch_builds_slug_and_page_query
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    source = FreelanceJobs::Sources::GeechsJob.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ slug: "ruby", hint: "Ruby" }], pages_per_slug: 2
    )

    source.fetch

    assert_equal [
      "https://geechs-job.com/project/ruby",
      "https://geechs-job.com/project/ruby?page=2"
    ], fetcher.requested_urls, "1ページ目はpageを付けず、2ページ目以降だけ?page=Nを付けるはず"
  end

  def test_fetch_stops_paging_when_a_page_has_no_cards
    fetcher = PagedFetcher.new(bodies_by_page: { 1 => read_fixture(FIXTURE_NAME), 2 => wrap_html("<p>該当する案件はありません</p>") })
    source = FreelanceJobs::Sources::GeechsJob.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ slug: "ruby", hint: "Ruby" }], pages_per_slug: 3
    )

    postings = source.fetch

    assert_equal 2, fetcher.requested_urls.size, "0件ページに当たったら3ページ目は取りに行かないはず"
    assert_equal 20, postings.size
  end
end
