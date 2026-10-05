# frozen_string_literal: true
# test/services/freelance_jobs/sources_offers_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesOffersTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 10, 3)
  FIXTURE_NAME = "offers_sidejob_ruby.html"
  SKILL_FIXTURE_NAME = "offers_skill_typescript.html"
  SOURCE = FreelanceJobs::Sources::Offers

  def parse_fixture(category_hint: "Ruby")
    SOURCE.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  # fixture の実カード（n 枚目）の HTML 片を取り出す。クラス名はハッシュ付きなので
  # テスト側では固定せず、部分一致で拾った実物をそのまま使う。
  def extract_fixture_card_html(card_index)
    document = Nokogiri::HTML(read_fixture(FIXTURE_NAME))
    document.css('article[class*="JobWideCard-module"]')[card_index].to_html
  end

  # fixture に「募集停止」のカードは無い（唯一の出現はフィルタ UI の「募集停止を非表示」ラベル）。
  # そのため実カード 1 枚に「募集停止」表示を足した最小 HTML で closed 経路を検証する。
  def build_closed_card_html(card_index: 1)
    extract_fixture_card_html(card_index).sub(/>/, '><span class="closed-label">募集停止</span>')
  end

  def wrap_cards(fragment)
    wrap_html("<main>#{fragment}</main>")
  end

  # --- 件数 ---

  def test_parse_fixture_returns_twenty_postings
    assert_equal 20, parse_fixture.size
  end

  # --- 1件目（業務委託から正社員・時給）の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "Offers", first.site
    assert_equal "https://offers.jp/jobs/101383", first.url
    assert_equal "【副業スタート可】ゼネコン数十社が使う積算AIを開発するWebエンジニア募集", first.title
    assert_equal "株式会社CORDER", first.client
    assert_equal "Ruby", first.category_hint
    assert_equal "時給 3,000円 〜 6,000円", first.reward, "ASCII の ~ は 〜 に置換されるはず"
    assert_equal "業務委託から正社員", first.work_format, "「雇用形態: 」の接頭辞は除くはず"
    assert_equal Date.new(2026, 10, 2), first.posted_on
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    refute first.closed?
  end

  def test_parse_first_posting_skills_and_tags
    first = parse_fixture.first

    assert_equal ["Ruby on Rails", "TypeScript", "GoogleAppsScript", "Git", "Ruby", "AWS", "Docker",
                  "MySQL", "Heroku", "React", "AI"], first.skills
    assert_equal ["フルスタックエンジニア"], first.tags, "職種カテゴリリンクの文言がtagsに入るはず"
  end

  def test_parse_first_posting_description_joins_profession_location_and_work_format
    description = parse_fixture.first.description

    assert_includes description, "職種: フルスタックエンジニア"
    assert_includes description, "勤務地: 東京都"
    assert_includes description, "雇用形態: 業務委託から正社員"
    refute_match(/\n/, description)
  end

  # --- 他カードの書式ゆれ（月給・年収・職種なし） ---

  def test_parse_monthly_and_yearly_rewards
    rewards_by_url = parse_fixture.to_h { |posting| [posting.url, posting.reward] }

    assert_equal "月給 80万円 〜 95万円", rewards_by_url["https://offers.jp/jobs/89077"]
    assert_equal "年収 800万円 〜 1,500万円", rewards_by_url["https://offers.jp/jobs/85892"]
  end

  def test_parse_plain_gyomu_itaku_work_format_and_posted_on
    second = parse_fixture[1]

    assert_equal "https://offers.jp/jobs/98084", second.url
    assert_equal "業務委託", second.work_format
    assert_equal Date.new(2026, 6, 22), second.posted_on
    assert_equal "株式会社Y's", second.client
  end

  def test_parse_card_without_profession_link_has_empty_tags
    posting = parse_fixture.find { |candidate| candidate.url == "https://offers.jp/jobs/88579" }

    assert_equal [], posting.tags
    assert_equal "アイザック株式会社", posting.client
  end

  def test_no_reward_contains_ascii_tilde_and_all_are_not_closed
    postings = parse_fixture

    assert(postings.none? { |posting| posting.reward.include?("~") })
    assert(postings.none?(&:closed?), "fixtureに募集停止カードは無いので全件募集中扱いのはず")
    assert(postings.all? { |posting| posting.url.match?(%r{\Ahttps://offers\.jp/jobs/\d+\z}) })
  end

  def test_category_hint_is_propagated_to_every_posting
    postings = parse_fixture(category_hint: "React")

    assert(postings.all? { |posting| posting.category_hint == "React" })
  end

  # --- 募集停止 ---

  def test_closed_card_gets_closed_status
    posting = SOURCE.parse(wrap_cards(build_closed_card_html), today: TODAY, category_hint: "Ruby").first

    assert_equal FreelanceJobs::JobPosting::CLOSED_STATUS, posting.application_status
    assert posting.closed?
    assert_equal "https://offers.jp/jobs/98084", posting.url
  end

  def test_only_the_closed_card_is_closed_among_mixed_cards
    fragment = extract_fixture_card_html(0) + build_closed_card_html(card_index: 1)

    postings = SOURCE.parse(wrap_cards(fragment), today: TODAY, category_hint: "Ruby")

    assert_equal [false, true], postings.map(&:closed?)
  end

  # --- 欠けたカード・重複 ---

  def test_parse_deduplicates_postings_with_the_same_url
    fragment = extract_fixture_card_html(0) * 2

    assert_equal 1, SOURCE.parse(wrap_cards(fragment), today: TODAY, category_hint: "Ruby").size
  end

  def test_parse_skips_cards_without_link_or_title
    fragment = %(<article class="JobWideCard-module__abc__container"><p>リンクなし</p></article>)

    assert_equal [], SOURCE.parse(wrap_cards(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_empty_page_returns_empty_array
    assert_equal [], SOURCE.parse(wrap_cards(""), today: TODAY, category_hint: "Ruby")
  end

  # --- 定数 ---

  def test_constants
    assert_equal "Offers", SOURCE::SITE_NAME
    assert_equal "https://offers.jp", SOURCE::BASE_URL
    assert_equal 1.5, SOURCE::REQUEST_INTERVAL
    assert_equal [
      { skill_id: 229, hint: "Ruby" },
      { skill_id: 252, hint: "TypeScript" },
      { skill_id: 261, hint: "React" }
    ], SOURCE::DEFAULT_SEARCH_TARGETS
  end

  # --- fetch ---

  # URL -> body のHashで返すフェイク。未登録URLは空文字（0件ページ）。
  class UrlMapFetcher
    def initialize(bodies_by_url: {}, default_body: "")
      @bodies_by_url = bodies_by_url
      @default_body = default_body
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      @bodies_by_url.fetch(url, @default_body)
    end
  end

  def test_fetch_requests_the_all_and_remote_lists_for_each_default_skill_without_query
    fetcher = UrlMapFetcher.new(default_body: read_fixture(FIXTURE_NAME))

    SOURCE.new(fetcher: fetcher, today: TODAY).fetch

    assert_equal [
      "https://offers.jp/jobs/skills/229",
      "https://offers.jp/jobs/skills/229/remote",
      "https://offers.jp/jobs/skills/252",
      "https://offers.jp/jobs/skills/252/remote",
      "https://offers.jp/jobs/skills/261",
      "https://offers.jp/jobs/skills/261/remote"
    ], fetcher.requested_urls
    assert(fetcher.requested_urls.none? { |url| url.include?("?") }, "robots.txt が /jobs*? を禁止するためクエリ禁止")
  end

  def test_fetch_deduplicates_across_skills_and_keeps_first_hint
    fetcher = UrlMapFetcher.new(default_body: read_fixture(FIXTURE_NAME))

    postings = SOURCE.new(fetcher: fetcher, today: TODAY).fetch
    contract_count = parse_fixture.count { |posting| posting.work_format.include?(SOURCE::CONTRACT_WORK_MARK) }

    assert_equal contract_count, postings.size
    assert_equal "Ruby", postings.first.category_hint, "先に出たskillのhintを残すはず"
  end

  def test_fetch_passes_each_skill_hint_as_category_hint
    ruby_body = wrap_cards(extract_fixture_card_html(0))
    react_body = wrap_cards(extract_fixture_card_html(1))
    fetcher = UrlMapFetcher.new(bodies_by_url: {
      "https://offers.jp/jobs/skills/229" => ruby_body,
      "https://offers.jp/jobs/skills/261/remote" => react_body
    })

    hints_by_url = SOURCE.new(fetcher: fetcher, today: TODAY).fetch.to_h do |posting|
      [posting.url, posting.category_hint]
    end

    assert_equal({ "https://offers.jp/jobs/101383" => "Ruby", "https://offers.jp/jobs/98084" => "React" },
                 hints_by_url)
  end

  def test_fetch_with_custom_search_targets
    fetcher = UrlMapFetcher.new
    source = SOURCE.new(fetcher: fetcher, today: TODAY, search_targets: [{ skill_id: 999, hint: "Go" }])

    assert_equal [], source.fetch
    assert_equal ["https://offers.jp/jobs/skills/999", "https://offers.jp/jobs/skills/999/remote"],
                 fetcher.requested_urls
  end

  def test_fetch_keeps_only_contract_work_postings
    fetcher = UrlMapFetcher.new(default_body: read_fixture(SKILL_FIXTURE_NAME))

    postings = SOURCE.new(fetcher: fetcher, today: TODAY).fetch

    assert_equal 14, postings.size
    assert(postings.all? { |posting| posting.work_format.include?("業務委託") })
    refute(postings.any? { |posting| posting.work_format == "正社員" })
  end

  def test_parse_returns_regular_employee_cards_too
    postings = SOURCE.parse(read_fixture(SKILL_FIXTURE_NAME), today: TODAY, category_hint: "TypeScript")

    assert_equal 20, postings.size
    assert_equal 6, postings.count { |posting| posting.work_format.include?("正社員") && !posting.work_format.include?("業務委託") }
  end

  def test_fetch_deduplicates_between_all_and_remote_lists
    body = read_fixture(SKILL_FIXTURE_NAME)
    fetcher = UrlMapFetcher.new(bodies_by_url: {
      "https://offers.jp/jobs/skills/229" => body,
      "https://offers.jp/jobs/skills/229/remote" => body
    })
    source = SOURCE.new(fetcher: fetcher, today: TODAY, search_targets: [{ skill_id: 229, hint: "Ruby" }])

    assert_equal 14, source.fetch.size
  end
end
