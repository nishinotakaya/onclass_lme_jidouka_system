# frozen_string_literal: true
# test/services/freelance_jobs/sources_lancers_agent_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesLancersAgentTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 10, 8)
  FIXTURE_NAME = "lancers_agent_ruby.html"
  SOURCE = FreelanceJobs::Sources::LancersAgent
  RUBY_URL = "https://tech-agent.lancers.jp/project?q_skill%5B%5D=5"
  JAVASCRIPT_URL = "https://tech-agent.lancers.jp/project?q_skill%5B%5D=10"

  def parse_fixture(category_hint: "Ruby")
    SOURCE.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  # 一覧カード1件分のHTML片（実データのDOM構造を模したもの）。
  # 実サイトの一覧はページ送りが POST フォームのため、カードは form 要素で、詳細パスは action に入る。
  def build_lancers_agent_card_html(action: "/project/engineer/abc123", heading: "<small>【週5日/Rubyエンジニア】</small><br>Rails開発",
                                    compensation: %(<font size="3">週</font>5日 | 〜190,000<font size="3">円</font> <span>/ 月</span>))
    action_attribute = action ? %( action="#{action}") : ""
    compensation_html = compensation ? %(<div class="cp-projects-listItem__compensation">#{compensation}</div>) : ""
    <<~HTML
      <form class="project-list__item" method="post"#{action_attribute}>
        <li class="cp-projects-listItem">
          <h2 class="js__offerTitle">#{heading}</h2>
          #{compensation_html}
          <ul>
            <li class="item--language">Ruby・Rails</li>
            <li class="item--days">週5日</li>
            <li class="item--place">東京都</li>
          </ul>
          <div class="cp-projects-listItem__description__text">本文です。</div>
        </li>
      </form>
    HTML
  end

  # --- 件数・1件目 ---

  def test_parse_fixture_returns_ten_postings
    assert_equal 10, parse_fixture.size
  end

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "ランサーズエージェント", first.site
    assert_equal "https://tech-agent.lancers.jp/project/engineer/0060I00000UdimnQAB", first.url
    assert_equal "【サーバーサイドエンジニア｜フルリモート】自社サービスにおけるシステムの機能追加開発及び改善・運用業務", first.title
    # 実HTMLは「週5日 | 530,000円〜 / 月」。週n日の部分は reward に入れず、空白除去と / の全角化だけ行う。
    assert_equal "530,000円〜／月", first.reward
    assert_equal %w[JavaScript PHP Ruby Go Vue.js RubyonRails Symfony MySQL Apache AWS Git Github], first.skills
    assert_equal "月額制（業務委託）", first.work_format
    assert_equal "Ruby", first.category_hint
    assert_equal "-", first.application_status
    assert_nil first.client
    assert_nil first.deadline_on
    assert_nil first.posted_on
  end

  def test_parse_first_posting_description_has_body_then_place_then_days
    description = parse_fixture.first.description

    body_index = description.index("【案件概要】")
    place_index = description.index("勤務地: 東京都(23区内)（目黒駅）")
    days_index = description.index("稼働: 週5日")

    refute_nil body_index, description
    refute_nil place_index, description
    refute_nil days_index, description
    assert body_index < place_index, "本文 → 勤務地 の順で並ぶはず"
    assert place_index < days_index, "勤務地 → 稼働 の順で並ぶはず"
    refute_match(/\n/, description)
  end

  def test_parse_first_posting_tags_keep_small_prefix_and_days
    tags = parse_fixture.first.tags

    assert_includes tags, "【週5日/PHPエンジニア】", "title から外した small 接頭辞は tags に回るはず"
    assert_includes tags, "週5日"
  end

  # --- title / url / reward 全件 ---

  def test_titles_do_not_contain_small_prefix
    parse_fixture.each do |posting|
      refute_match(/【週[^】]*】/, posting.title, "small の接頭辞が title に残っている")
      refute_match(/<|>/, posting.title)
      refute_empty posting.title
    end
  end

  def test_urls_are_absolute_project_urls
    parse_fixture.each do |posting|
      assert_match %r{\Ahttps://tech-agent\.lancers\.jp/project/[a-z]+/[0-9A-Za-z]+\z}, posting.url
    end
  end

  def test_reward_never_contains_days_prefix_or_whitespace
    parse_fixture.each do |posting|
      refute_match(/週|\|/, posting.reward)
      refute_match(/[[:space:]]/, posting.reward)
      assert_match(/円.*／月\z/, posting.reward)
    end
  end

  def test_reward_keeps_upper_bound_form_and_handles_card_without_days
    rewards = parse_fixture.map(&:reward)

    assert_equal "〜550,000円／月", rewards[1]
    assert_equal "〜750,000円／月", rewards.last, "稼働日数の無いカード（「〜750,000円 / 月」のみ）でも読めるはず"
  end

  def test_skills_drop_empty_elements_from_broken_separators
    parse_fixture.each do |posting|
      refute_includes posting.skills, ""
      assert_equal posting.skills.map(&:strip), posting.skills
    end
    # 6件目は「Go・Ruby・on・Rails・・Laravel・・…」のように区切りが崩れているが、空要素は出さない。
    assert_includes parse_fixture[5].skills, "Laravel"
  end

  # --- 最小カード・欠けたカード ---

  def test_parse_minimal_card
    html = wrap_html(build_lancers_agent_card_html)

    posting = SOURCE.parse(html, today: TODAY, category_hint: "Ruby").first

    assert_equal "https://tech-agent.lancers.jp/project/engineer/abc123", posting.url
    assert_equal "Rails開発", posting.title
    assert_equal "〜190,000円／月", posting.reward
    assert_equal %w[Ruby Rails], posting.skills
    assert_includes posting.tags, "【週5日/Rubyエンジニア】"
  end

  def test_parse_defaults_reward_when_missing
    html = wrap_html(build_lancers_agent_card_html(compensation: nil))

    assert_equal "要確認", SOURCE.parse(html, today: TODAY).first.reward
  end

  def test_reward_is_default_when_amount_is_undisclosed_with_days_range
    compensation = %(<font size="3">週</font>3日･4日･5日 | 報酬額非公開 <span>/ 月</span>)
    html = wrap_html(build_lancers_agent_card_html(compensation: compensation))

    assert_equal "要確認", SOURCE.parse(html, today: TODAY).first.reward
  end

  def test_parse_card_without_days_element_still_returns_posting
    html = wrap_html(build_lancers_agent_card_html.sub(%(<li class="item--days">週5日</li>), ""))

    posting = SOURCE.parse(html, today: TODAY).first

    assert_equal 1, SOURCE.parse(html, today: TODAY).size
    refute_includes posting.description, "稼働:"
    assert_includes posting.description, "勤務地: 東京都"
  end

  def test_parse_card_without_place_element_still_returns_posting
    html = wrap_html(build_lancers_agent_card_html.sub(%(<li class="item--place">東京都</li>), ""))

    posting = SOURCE.parse(html, today: TODAY).first

    assert_equal 1, SOURCE.parse(html, today: TODAY).size
    refute_includes posting.description, "勤務地:"
    assert_includes posting.description, "稼働: 週5日"
  end

  def test_parse_card_without_description_element_still_returns_posting
    html = wrap_html(build_lancers_agent_card_html.sub(%(<div class="cp-projects-listItem__description__text">本文です。</div>), ""))

    posting = SOURCE.parse(html, today: TODAY).first

    assert_equal 1, SOURCE.parse(html, today: TODAY).size
    assert_equal "勤務地: 東京都 稼働: 週5日", posting.description
  end

  def test_parse_card_without_compensation_element_still_returns_posting
    html = wrap_html(build_lancers_agent_card_html(compensation: nil))

    assert_equal 1, SOURCE.parse(html, today: TODAY).size
  end

  def test_title_is_whole_heading_when_small_is_absent
    html = wrap_html(build_lancers_agent_card_html(heading: "小見出し無しの本題"))

    posting = SOURCE.parse(html, today: TODAY).first

    assert_equal "小見出し無しの本題", posting.title
    assert_equal ["週5日"], posting.tags
  end

  def test_parse_skips_card_whose_heading_has_only_small
    html = wrap_html(build_lancers_agent_card_html(heading: "<small>【週5日/Rubyエンジニア】</small>"))

    assert_equal [], SOURCE.parse(html, today: TODAY)
  end

  def test_title_after_small_with_br_is_the_main_text
    html = wrap_html(build_lancers_agent_card_html(heading: "<small>【週3日/】</small><br>本題"))

    assert_equal "本題", SOURCE.parse(html, today: TODAY).first.title
  end

  def test_skills_collapse_exact_duplicates_only
    html = wrap_html(build_lancers_agent_card_html.sub("Ruby・Rails", "Python・python・Python"))

    assert_equal %w[Python python], SOURCE.parse(html, today: TODAY).first.skills
  end

  def test_parse_skips_forms_without_action
    assert_equal [], SOURCE.parse(wrap_html(build_lancers_agent_card_html(action: nil)), today: TODAY)
  end

  def test_parse_deduplicates_postings_with_the_same_url
    fragment = build_lancers_agent_card_html(heading: "A") + build_lancers_agent_card_html(heading: "B")

    postings = SOURCE.parse(wrap_html(fragment), today: TODAY)

    assert_equal ["A"], postings.map(&:title)
  end

  def test_parse_empty_page_returns_empty_array
    assert_equal [], SOURCE.parse(wrap_html(""), today: TODAY)
  end

  # --- 定数 ---

  def test_constants
    assert_equal "ランサーズエージェント", SOURCE::SITE_NAME
    assert_equal "https://tech-agent.lancers.jp", SOURCE::BASE_URL
    assert_kind_of Numeric, SOURCE::REQUEST_INTERVAL
    assert_equal [{ skill_id: 5, hint: "Ruby" }, { skill_id: 10, hint: nil }], SOURCE::DEFAULT_SEARCH_TARGETS
  end

  # --- fetch ---

  # URL -> body のフェイク。未登録URLは空文字（0件ページ）。
  class MapFetcher
    def initialize(bodies_by_url: {})
      @bodies_by_url = bodies_by_url
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      @bodies_by_url.fetch(url, "")
    end
  end

  def test_fetch_requests_one_page_per_default_target
    body = read_fixture(FIXTURE_NAME)
    fetcher = MapFetcher.new(bodies_by_url: { RUBY_URL => body, JAVASCRIPT_URL => body })

    SOURCE.new(fetcher: fetcher, today: TODAY).fetch

    # ページ送りは POST フォームでしか動かず、GET の page=2 は1頁目と同じ内容を返すため、頁は辿らない。
    assert_equal [RUBY_URL, JAVASCRIPT_URL], fetcher.requested_urls
  end

  def test_fetch_deduplicates_across_targets_and_keeps_first_hint
    body = read_fixture(FIXTURE_NAME)
    fetcher = MapFetcher.new(bodies_by_url: { RUBY_URL => body, JAVASCRIPT_URL => body })
    targets = [{ skill_id: 5, hint: "Ruby" }, { skill_id: 10, hint: "JavaScript" }]

    postings = SOURCE.new(fetcher: fetcher, today: TODAY, search_targets: targets).fetch

    assert_equal 10, postings.size
    assert(postings.all? { |posting| posting.category_hint == "Ruby" })
  end
end
