# frozen_string_literal: true
# test/services/freelance_jobs/sources_sokudan_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"
require "json"
require "uri"
require "logger"

class FreelanceJobsSourcesSokudanTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 13)
  # 詳細(parse_detail)は据え置きのSSRページ。staticProjectSearchResultはあるがstaticProjectが無いので、
  # 「一覧ページを詳細として読むとnilになる」ことの確認にも使う。
  LIST_FIXTURE_NAME = "sokudan_ruby.html"
  DETAIL_FIXTURE_NAME = "sokudan_detail.html"
  FULL_REMOTE_DETAIL_FIXTURE_NAME = "sokudan_detail_full_remote.html"

  # 一覧(parse)はJSON API本文を読む。実際のAPI応答から件数を絞って作った小さなフィクスチャ。
  LIST_API_FIXTURE_NAME = "sokudan_api_page1.json"
  LIST_API_PAGE2_CLOSED_ONLY_FIXTURE_NAME = "sokudan_api_page2_closed_only.json"
  LIST_API_EMPTY_FIXTURE_NAME = "sokudan_api_empty.json"

  LIST_URL = "https://sokudan.work/top/projects"
  LIST_API_URL = "https://sokudan.work/api/v2/top/projects"
  LIST_API_PATH = "/api/v2/top/projects"

  def parse_api_fixture(category_hint: "Ruby")
    FreelanceJobs::Sources::Sokudan.parse(read_fixture(LIST_API_FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  def parse_detail_fixture(fixture_name, category_hint: "Ruby")
    FreelanceJobs::Sources::Sokudan.parse_detail(read_fixture(fixture_name), today: TODAY, category_hint: category_hint)
  end

  # --- parse: API JSON本文を読み、opened→closedの順を保ったまま募集終了も落とさない ---
  # （除外の判断はfetch側。ページ送りの打ち切り判定がstateを見るため、parseの段階で
  # 募集終了を落とすと「このページにopenedが1件も無い」を判定できなくなる）。

  def test_parse_api_fixture_returns_all_outsourcing_projects_including_closed_in_order
    postings = parse_api_fixture

    assert_equal(
      %w[
        https://sokudan.work/top/projects/21033
        https://sokudan.work/top/projects/20807
        https://sokudan.work/top/projects/20774
        https://sokudan.work/top/projects/20773
      ],
      postings.map(&:url),
      "フィクスチャのprojectListはopened→closedの順なので、その順のまま返すはず"
    )
    assert_equal %w[募集中 募集中 募集終了 募集終了], postings.map(&:application_status)
  end

  def test_parse_keeps_closed_side_job_projects_but_excludes_non_side_job_contract_types
    projects = [
      build_list_project(id: 501, title: "募集中の業務委託", state: "opened", contract_type: "outsourcing"),
      build_list_project(id: 502, title: "募集終了の業務委託", state: "closed", contract_type: "outsourcing"),
      build_list_project(id: 503, title: "募集中の正社員", state: "opened", contract_type: "full_time")
    ]
    postings = FreelanceJobs::Sources::Sokudan.parse(build_list_body(projects), today: TODAY)

    assert_equal(
      ["https://sokudan.work/top/projects/501", "https://sokudan.work/top/projects/502"],
      postings.map(&:url),
      "正社員(full_time)は除外するが、募集終了(closed)の業務委託は除外しないはず"
    )
    assert_equal "募集中", postings.find { |posting| posting.url.end_with?("/501") }.application_status
    assert_equal "募集終了", postings.find { |posting| posting.url.end_with?("/502") }.application_status
  end

  # --- 一覧(20807)の全フィールド（使用技術・本文は一覧に無いので空、掲載日は createdAt） ---

  def test_parse_api_fixture_maps_fields_for_side_job_posting
    posting = parse_api_fixture.find { |candidate| candidate.url.end_with?("/20807") }

    refute_nil posting
    assert_equal "SOKUDAN", posting.site
    assert_equal "https://sokudan.work/top/projects/20807", posting.url
    assert_equal "【一部リモ可】全国展開化粧品ブランドのECシステムエンジニアを募集！", posting.title
    assert_equal "Ruby", posting.category_hint
    assert_equal "650,000〜950,000円／月", posting.reward
    assert_equal "月額制（業務委託）", posting.work_format
    assert_equal "募集中", posting.application_status
    assert_equal "-", posting.deadline_text
    assert_nil posting.deadline_on
    assert_equal [], posting.skills, "一覧には使用技術が無いので空のはず"
    assert_equal "", posting.client, "未ログインでは企業名がマスクされるので空のはず"
    assert_equal ["業務委託", "長期", "急募", "即日勤務OK"], posting.tags
    assert_equal Date.new(2026, 9, 9), posting.posted_on
  end

  def test_parse_api_fixture_description_uses_list_level_fields_only
    posting = parse_api_fixture.find { |candidate| candidate.url.end_with?("/20807") }

    assert_equal(
      "募集職種: バックエンドエンジニア / 稼働: 週5日（週40h）（週40h〜） / 勤務形態: リモート(一部)可 / " \
      "勤務地: 首都圏 / 契約形態: 業務委託",
      posting.description
    )
  end

  def test_parse_posting_with_outsourcing_to_full_time_contract
    project = build_list_project(id: 601, title: "業務委託→正社員案件", contract_type: "outsourcing_to_full_time")
    posting = FreelanceJobs::Sources::Sokudan.parse(build_list_body([project]), today: TODAY).first

    refute_nil posting
    assert_equal "業務委託→正社員", posting.tags.first
    assert_includes posting.description, "契約形態: 業務委託→正社員"
  end

  # --- 詳細ページ(parse_detail)は据え置き。既存の詳細フィクスチャを使ったテストが引き続き通るはず ---

  def test_parse_detail_full_remote_fixture_has_expected_fields
    posting = parse_detail_fixture(FULL_REMOTE_DETAIL_FIXTURE_NAME)

    assert_equal "SOKUDAN", posting.site
    assert_equal "https://sokudan.work/top/projects/20846", posting.url
    assert_equal "【フルリモ】PKSHAグループ＊AI教育SaaS開発を担うフルスタックエンジニア", posting.title
    assert_equal "Ruby", posting.category_hint
    assert_equal "400,000〜1,000,000円／月", posting.reward
    assert_equal "月額制（業務委託）", posting.work_format
    assert_equal "募集中", posting.application_status
    assert_equal "-", posting.deadline_text
    assert_nil posting.deadline_on
    assert_equal ["Ruby on Railsでの開発経験", "TypeScriptでの開発経験", "AWS", "Azure", "Ruby on Rails", "TypeScript"],
                 posting.skills
    assert_equal "", posting.client
    assert_equal ["業務委託", "フリーランス歓迎", "フルリモート", "高単価"], posting.tags
    assert_equal Date.new(2026, 9, 10), posting.posted_on, "JSON-LD の datePosted を掲載日にするはず"
  end

  # descriptionは「募集職種 / 必須スキル / 稼働 / 勤務形態 / 勤務地 / 契約形態 / 案件詳細」の順に連結する。
  # 本文の "\r\n" は normalize_description で1スペースに畳まれる。
  def test_parse_detail_description_starts_with_structured_parts_and_ends_with_detail_text
    description = parse_detail_fixture(FULL_REMOTE_DETAIL_FIXTURE_NAME).description

    assert description.start_with?(
      "募集職種: バックエンドエンジニア・インフラエンジニア / " \
      "必須スキル: Ruby on Railsでの開発経験 / TypeScriptでの開発経験 / AWS / Azure / Ruby on Rails / TypeScript / " \
      "稼働: 週2日（週16~23h）（週20h〜） / 勤務形態: フルリモート(在宅OK) / 勤務地: 全国 / 契約形態: 業務委託 / " \
      "案件詳細: ※本案件は、SOKUDANと契約いただく案件となります。 ▼案件概要 弊社は、コンタクトセンター向けに"
    ), description[0, 300]
    refute_includes description, "\r\n"
  end

  def test_parse_detail_fixture_with_twelve_skills
    posting = parse_detail_fixture(DETAIL_FIXTURE_NAME, category_hint: "TypeScript")

    assert_equal "https://sokudan.work/top/projects/20807", posting.url
    assert_equal "TypeScript", posting.category_hint
    assert_equal 12, posting.skills.size
    assert_includes posting.skills, "Ruby"
    assert_includes posting.skills, "TypeScript"
    assert_includes posting.skills, "Webアプリケーション・サーバーサイドの開発経験"
    assert_equal ["業務委託", "長期", "急募", "即日勤務OK", "女性歓迎", "フリーランス歓迎"], posting.tags
    assert_equal Date.new(2026, 9, 9), posting.posted_on
    assert_includes posting.description, "案件詳細: 化粧品ブランドにて自社ECプラットフォームの成⾧を支えるため"
  end

  # --- 全件で必須フィールドが埋まる（"-"や""は許すがnilは許さない） ---

  def test_every_posting_fills_display_fields
    parse_api_fixture.each do |posting|
      refute_empty posting.title, "案件名が空の要素は除外されるはず"
      refute_empty posting.reward
      refute_empty posting.work_format
      refute_empty posting.application_status
      assert_instance_of Array, posting.skills
      assert_instance_of Array, posting.tags
      assert_instance_of String, posting.client
      refute_nil posting.posted_on, "createdAt はフィクスチャ全件に存在する"
    end
  end

  # --- URL正規化 ---

  def test_urls_are_normalized_absolute_without_trailing_slash_or_query
    parse_api_fixture.each do |posting|
      assert_match %r{\Ahttps://sokudan\.work/top/projects/\d+\z}, posting.url,
                   "末尾スラッシュなし・クエリなしの正規化された絶対URLのはず"
    end
  end

  # --- category_hint が引数どおり全件に伝わる（nil も許す） ---

  def test_category_hint_is_propagated_to_every_posting
    postings = parse_api_fixture(category_hint: "TypeScript")

    assert(postings.all? { |posting| posting.category_hint == "TypeScript" })
    assert(parse_api_fixture(category_hint: nil).all? { |posting| posting.category_hint.nil? })
  end

  # --- 単価の単位（JSON-LD unitText）で work_format が分岐する ---

  def test_work_format_is_hourly_when_unit_text_is_hour
    project = detail_project(FULL_REMOTE_DETAIL_FIXTURE_NAME)
    project["projectDetailStructuredData"]["jobPosting"]["baseSalary"]["value"]["unitText"] = "HOUR"
    project["minBudget"]["id"] = 5000
    project["maxBudget"]["id"] = 8000
    posting = FreelanceJobs::Sources::Sokudan.parse_detail(build_detail_body(project), today: TODAY)

    assert_equal "5,000〜8,000円／時", posting.reward
    assert_equal "時間単価制", posting.work_format
  end

  def test_reward_is_single_value_when_min_equals_max
    project = detail_project(FULL_REMOTE_DETAIL_FIXTURE_NAME)
    project["minBudget"]["id"] = 800_000
    project["maxBudget"]["id"] = 800_000
    posting = FreelanceJobs::Sources::Sokudan.parse_detail(build_detail_body(project), today: TODAY)

    assert_equal "800,000円／月", posting.reward
  end

  def test_work_format_falls_back_when_budget_is_missing
    project = detail_project(FULL_REMOTE_DETAIL_FIXTURE_NAME)
    project["minBudget"] = nil
    project["maxBudget"] = { "id" => 0 }
    posting = FreelanceJobs::Sources::Sokudan.parse_detail(build_detail_body(project), today: TODAY)

    assert_equal "要確認", posting.reward
    assert_equal "業務委託（フリーランス）", posting.work_format
  end

  # --- 掲載日のフォールバック: datePosted → applicationOpenAt → createdAt ---

  def test_posted_on_falls_back_to_application_open_at_then_created_at
    project = detail_project(FULL_REMOTE_DETAIL_FIXTURE_NAME)
    project["projectDetailStructuredData"] = nil
    assert_equal Date.new(2026, 9, 10),
                 FreelanceJobs::Sources::Sokudan.parse_detail(build_detail_body(project), today: TODAY).posted_on

    project["applicationOpenAt"] = "not a date"
    project["createdAt"] = "2026-09-01T09:00:00.000+09:00"
    assert_equal Date.new(2026, 9, 1),
                 FreelanceJobs::Sources::Sokudan.parse_detail(build_detail_body(project), today: TODAY).posted_on

    project["createdAt"] = nil
    assert_nil FreelanceJobs::Sources::Sokudan.parse_detail(build_detail_body(project), today: TODAY).posted_on
  end

  # --- 企業名はマスクなら ""、公開されていればそのまま ---

  def test_client_name_is_kept_when_not_masked
    project = detail_project(FULL_REMOTE_DETAIL_FIXTURE_NAME)
    project["corporation"]["name"] = "株式会社テスト"
    posting = FreelanceJobs::Sources::Sokudan.parse_detail(build_detail_body(project), today: TODAY)

    assert_equal "株式会社テスト", posting.client
  end

  # --- 詳細で募集終了になっていれば application_status に反映する（除外は fetch 側） ---

  def test_parse_detail_marks_closed_project_as_closed
    project = detail_project(FULL_REMOTE_DETAIL_FIXTURE_NAME)
    project["state"] = "closed"
    posting = FreelanceJobs::Sources::Sokudan.parse_detail(build_detail_body(project), today: TODAY)

    assert_equal "募集終了", posting.application_status
  end

  # --- 必須要素（id・title）が欠けた要素・壊れたJSONは黙って除外する ---

  def test_parse_returns_empty_when_body_is_not_json
    assert_equal [], FreelanceJobs::Sources::Sokudan.parse("<html><body>Not Found</body></html>", today: TODAY)
  end

  def test_parse_returns_empty_when_json_is_broken
    assert_equal [], FreelanceJobs::Sources::Sokudan.parse('{"projectList": ', today: TODAY)
  end

  def test_parse_returns_empty_when_project_list_key_is_missing
    assert_equal [], FreelanceJobs::Sources::Sokudan.parse(JSON.generate("breadCrumbText" => "Ruby"), today: TODAY)
  end

  def test_parse_returns_empty_when_project_list_is_not_an_array
    assert_equal [], FreelanceJobs::Sources::Sokudan.parse(build_list_body("not an array"), today: TODAY)
  end

  def test_parse_skips_project_without_id_or_title
    projects = [
      build_list_project(id: nil, title: "idなし"),
      build_list_project(id: 100, title: ""),
      build_list_project(id: "abc", title: "idが数字でない"),
      build_list_project(id: 101, title: "正常な案件")
    ]
    postings = FreelanceJobs::Sources::Sokudan.parse(build_list_body(projects), today: TODAY)

    assert_equal ["https://sokudan.work/top/projects/101"], postings.map(&:url)
  end

  def test_parse_detail_returns_nil_when_static_project_is_missing
    assert_nil FreelanceJobs::Sources::Sokudan.parse_detail(wrap_html("<div>該当なし</div>"), today: TODAY)
    assert_nil FreelanceJobs::Sources::Sokudan.parse_detail(read_fixture(LIST_FIXTURE_NAME), today: TODAY),
               "一覧ページには staticProject が無いので nil のはず"
  end

  # --- fetch: 一覧APIから候補を集め、新しい順に詳細取得、URL重複を排除する ---

  # URLごとの応答を台本化するFakeフェッチャー（通信しない）。
  # 各URLの配列を先頭から1回ずつ消費し、例外なら送出・文字列ならbodyとして返す。
  # 台本に無いURL・使い切ったURLは fallback_body を返す。
  class ScriptedFetcher
    def initialize(script, fallback_body:)
      @script = script
      @fallback_body = fallback_body
      @requested_urls = []
      @requested_headers = []
    end

    attr_reader :requested_urls, :requested_headers

    def get(url, headers: {})
      @requested_urls << url
      @requested_headers << headers
      response = @script[url]&.shift || @fallback_body
      raise response if response.is_a?(StandardError)

      response
    end
  end

  # ページ送りの打ち切り条件だけを検証するための、呼ばれた順に応答を払い出すFakeフェッチャー
  # （どのURLに対する応答かは問わない。列挙し尽くしたら空の一覧を返す）。
  class QueueFetcher
    def initialize(queue)
      @queue = queue
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      @queue.shift || JSON.generate("projectList" => [])
    end
  end

  def test_fetch_requests_each_target_list_then_fetches_detail_once_for_the_deduplicated_candidate
    fetcher = ScriptedFetcher.new(
      { detail_url(20807) => [read_fixture(DETAIL_FIXTURE_NAME)] },
      fallback_body: closed_single_project_list_body
    )
    postings = build_source(fetcher).fetch

    list_request_count = fetcher.requested_urls.count { |url| url.include?(LIST_API_PATH) }
    assert_equal 5, list_request_count, "スキル4グループ×1ページ（openedが無く打ち切り）＋全体新着1本のはず"
    assert_equal 1, fetcher.requested_urls.count(detail_url(20807)),
                 "全リストが同じ候補を返しても重複排除され詳細取得は1回のはず"
    assert_equal 1, postings.size
    assert_equal 12, postings.first.skills.size, "詳細取得で上書きされ12スキルになるはず"
  end

  def test_fetch_overrides_list_posting_with_detail_and_keeps_list_posting_when_detail_is_unreadable
    search_targets = [
      { skill_ids: [3], category_hint: "Ruby" },
      { skill_ids: [5], category_hint: "TypeScript" }
    ]
    fetcher = ScriptedFetcher.new(
      {
        skill_list_page_url([3], 1) => [closed_single_project_list_body(id: 20807)],
        skill_list_page_url([5], 1) => [closed_single_project_list_body(id: 19994)],
        detail_url(20807) => [read_fixture(DETAIL_FIXTURE_NAME)],
        detail_url(19994) => [closed_single_project_list_body(id: 19994)]
      },
      fallback_body: read_fixture(LIST_API_EMPTY_FIXTURE_NAME)
    )
    postings = build_source(fetcher, search_targets: search_targets, include_latest_list: false).fetch

    detailed = postings.find { |posting| posting.url.end_with?("/20807") }
    assert_equal 12, detailed.skills.size, "詳細が取れた案件は requiredSkills で上書きされるはず"
    assert_includes detailed.description, "案件詳細:"

    list_only = postings.find { |posting| posting.url.end_with?("/19994") }
    assert_equal [], list_only.skills, "詳細JSONが読めない案件（一覧ページが返った）は一覧の内容のままのはず"
  end

  def test_fetch_skips_latest_list_when_option_is_off
    fetcher = ScriptedFetcher.new({}, fallback_body: closed_single_project_list_body)
    postings = build_source(fetcher, include_latest_list: false).fetch

    refute(fetcher.requested_urls.any? { |url| url == latest_list_page_url }, "全体新着一覧は取得しないはず")
    assert_equal 4 + 1, fetcher.requested_urls.size,
                 "スキル4グループ×1ページ（打ち切り）＋重複排除後の詳細1件のはず"
    assert_equal 1, postings.size
  end

  # 全体新着一覧にしか無い案件は createdAt が直近 latest_lookback_days 日のものだけ採る。
  # today=2026-09-10 なら 20807(09-09) は採り、19994(2025-01) は落とす。
  def test_fetch_takes_only_recent_candidates_from_latest_list
    recent_project = build_list_project(id: 20807, title: "新着案件", state: "opened")
    recent_project["createdAt"] = "2026-09-09T10:00:00.000+09:00"
    old_project = build_list_project(id: 19994, title: "古い案件", state: "opened")
    old_project["createdAt"] = "2025-01-01T10:00:00.000+09:00"

    fetcher = ScriptedFetcher.new(
      { detail_url(20807) => [closed_single_project_list_body(id: 20807)] },
      fallback_body: build_list_body([recent_project, old_project])
    )
    source = FreelanceJobs::Sources::Sokudan.new(
      fetcher: fetcher, today: Date.new(2026, 9, 10), search_targets: [], include_latest_list: true
    )
    postings = source.fetch

    assert_equal [latest_list_page_url, detail_url(20807)], fetcher.requested_urls
    assert_equal ["https://sokudan.work/top/projects/20807"], postings.map(&:url)
    assert_nil postings.first.category_hint, "全体新着由来は category_hint なし（本文判定に任せる）のはず"
  end

  # 詳細の取得件数は max_detail_fetches で打ち切り、溢れた古い候補は一覧の内容で採用する。
  def test_fetch_limits_detail_requests_and_keeps_overflow_candidates_from_list
    search_targets = [
      { skill_ids: [3], category_hint: "Ruby" },
      { skill_ids: [5], category_hint: "TypeScript" }
    ]
    newer_project = build_list_project(id: 20807, title: "新しい案件", state: "closed")
    newer_project["createdAt"] = "2026-09-12T10:00:00.000+09:00"
    older_project = build_list_project(id: 19994, title: "古い案件", state: "closed")
    older_project["createdAt"] = "2026-09-01T10:00:00.000+09:00"

    fetcher = ScriptedFetcher.new(
      {
        skill_list_page_url([3], 1) => [build_list_body([newer_project])],
        skill_list_page_url([5], 1) => [build_list_body([older_project])],
        detail_url(20807) => [read_fixture(DETAIL_FIXTURE_NAME)]
      },
      fallback_body: read_fixture(LIST_API_EMPTY_FIXTURE_NAME)
    )
    postings = build_source(fetcher, search_targets: search_targets, include_latest_list: false,
                                      max_detail_fetches: 1).fetch

    assert_equal 1, fetcher.requested_urls.count { |url| url.start_with?("#{LIST_URL}/") },
                 "詳細は最新の1件だけ取るはず"
    detailed = postings.find { |posting| posting.url.end_with?("/20807") }
    assert_equal 12, detailed.skills.size
    older = postings.find { |posting| posting.url.end_with?("/19994") }
    assert_equal [], older.skills, "詳細を取らなかった候補は一覧の内容のまま残るはず"
    assert_equal 2, postings.size
  end

  def test_max_detail_fetches_and_request_budget_account_for_paginated_lists
    assert_equal 27, FreelanceJobs::Sources::Sokudan::MAX_DETAIL_FETCHES
    assert_equal 40, FreelanceJobs::Sources::Sokudan::REQUEST_BUDGET
  end

  # 一覧は最大 4スキルグループ×3ページ＋全体新着1本 = 13本。詳細27件で合計40（予算内）。
  # max_detail_fetches を大きくしても予算は超えない。
  def test_detail_fetch_limit_accounts_for_worst_case_pagination_and_never_exceeds_request_budget
    source = build_source(Object.new, max_detail_fetches: 100)

    assert_equal 27, source.send(:detail_fetch_limit)
  end

  # 一覧取得〜詳細取得の間に締め切られた案件は落とす。
  def test_fetch_drops_project_closed_at_detail
    closed_project = detail_project(DETAIL_FIXTURE_NAME)
    closed_project["state"] = "closed"
    search_targets = [
      { skill_ids: [3], category_hint: "Ruby" },
      { skill_ids: [5], category_hint: "TypeScript" }
    ]
    fetcher = ScriptedFetcher.new(
      {
        skill_list_page_url([3], 1) => [closed_single_project_list_body(id: 20807)],
        skill_list_page_url([5], 1) => [build_list_body([build_list_project(id: 19994, title: "残る案件", state: "opened")])],
        detail_url(20807) => [build_detail_body(closed_project)]
        # 19994の詳細はfallback（一覧の内容のまま採用される）
      },
      fallback_body: read_fixture(LIST_API_EMPTY_FIXTURE_NAME)
    )
    postings = build_source(fetcher, search_targets: search_targets, include_latest_list: false).fetch

    refute_includes postings.map(&:url), "https://sokudan.work/top/projects/20807",
                    "詳細取得時に募集終了になっていた案件は落とすはず"
    assert_includes postings.map(&:url), "https://sokudan.work/top/projects/19994",
                     "詳細を取らなかった別候補は残るはず"
    assert_equal 1, postings.size
  end

  # --- 散発的な取得失敗に耐える（1回リトライ → その単位だけ打ち切り → 全滅時のみ例外） ---

  def test_fetch_retries_once_when_a_list_returns_server_error
    fetcher = ScriptedFetcher.new(
      { skill_list_page_url([3], 1) => [server_error, closed_single_project_list_body] },
      fallback_body: empty_list_body
    )
    search_targets = [{ skill_ids: [3], category_hint: "Ruby" }]
    postings = silencing_fetch_logs do
      build_source(fetcher, search_targets: search_targets, include_latest_list: false).fetch
    end

    assert_equal 1, postings.size, "1回目が失敗でも再取得すれば取得できるはず"
    assert_equal [skill_list_page_url([3], 1), skill_list_page_url([3], 1)], fetcher.requested_urls.first(2)
  end

  # HTTPエラーだけでなく通信層の切断・タイムアウトも同じく1一覧の打ち切りで済ませる。
  def test_fetch_skips_failing_list_and_keeps_other_lists
    connection_error = Errno::ECONNRESET.new("Connection reset by peer")
    fetcher = ScriptedFetcher.new(
      {
        skill_list_page_url([5], 1) => [connection_error, connection_error],
        skill_list_page_url([3], 1) => [closed_single_project_list_body]
      },
      fallback_body: empty_list_body
    )
    search_targets = [
      { skill_ids: [5], category_hint: "TypeScript" },
      { skill_ids: [3], category_hint: "Ruby" }
    ]
    postings = silencing_fetch_logs do
      build_source(fetcher, search_targets: search_targets, include_latest_list: false).fetch
    end

    assert_equal 1, postings.size, "TypeScript一覧が落ちてもRuby一覧の結果は返すはず"
  end

  def test_fetch_keeps_list_posting_when_detail_fails_twice
    detail_error = FreelanceJobs::FetchError.new("HTTP 500 #{detail_url(20807)}")
    fetcher = ScriptedFetcher.new(
      {
        skill_list_page_url([3], 1) => [closed_single_project_list_body],
        detail_url(20807) => [detail_error, detail_error]
      },
      fallback_body: empty_list_body
    )
    search_targets = [{ skill_ids: [3], category_hint: "Ruby" }]
    postings = silencing_fetch_logs do
      build_source(fetcher, search_targets: search_targets, include_latest_list: false).fetch
    end

    assert_equal 1, postings.size, "詳細に2回失敗した案件も一覧の内容で残るはず"
    assert_equal 2, fetcher.requested_urls.count(detail_url(20807)), "詳細も1回だけ再取得するはず"
  end

  # 全滅を黙って0件で返すとサイト構造の崩れに気付けないため、最初の失敗を送出する。
  def test_fetch_raises_when_no_list_succeeds
    error = server_error
    fetcher = ScriptedFetcher.new({ skill_list_page_url([3], 1) => [error, error] }, fallback_body: empty_list_body)
    search_targets = [{ skill_ids: [3], category_hint: "Ruby" }]

    raised = assert_raises(FreelanceJobs::FetchError) do
      silencing_fetch_logs { build_source(fetcher, search_targets: search_targets, include_latest_list: false).fetch }
    end

    assert_equal error.message, raised.message
  end

  # WAFのアクセス制限は取り直しても解消しないので、再取得せずそのまま送出する。
  def test_fetch_does_not_retry_when_access_is_blocked
    fetcher = ScriptedFetcher.new(
      { skill_list_page_url([3], 1) => [FreelanceJobs::AccessBlockedError.new("アクセス制限（WAF captcha）")] },
      fallback_body: empty_list_body
    )
    search_targets = [{ skill_ids: [3], category_hint: "Ruby" }]

    assert_raises(FreelanceJobs::AccessBlockedError) do
      silencing_fetch_logs { build_source(fetcher, search_targets: search_targets).fetch }
    end
    assert_equal 1, fetcher.requested_urls.size, "アクセス制限では再取得しないはず"
  end

  # --- 一覧APIのリクエスト形式: ヘッダ・URLの組み立て ---

  def test_fetch_sends_xhr_header_on_every_list_request
    fetcher = ScriptedFetcher.new({}, fallback_body: read_fixture(LIST_API_EMPTY_FIXTURE_NAME))
    search_targets = [{ skill_ids: [3, 19], category_hint: "Ruby" }]
    build_source(fetcher, search_targets: search_targets, include_latest_list: true, max_detail_fetches: 0).fetch

    assert_equal 2, fetcher.requested_urls.size, "スキル一覧1本＋全体新着一覧1本のはず"
    assert(
      fetcher.requested_headers.all? { |headers| headers == { "x-requested-with" => "XMLHttpRequest" } },
      "一覧リクエストは全て x-requested-with: XMLHttpRequest を付けるはず"
    )
  end

  def test_fetch_builds_skill_list_url_with_repeated_skill_ids_and_page_param
    fetcher = ScriptedFetcher.new({}, fallback_body: read_fixture(LIST_API_EMPTY_FIXTURE_NAME))
    search_targets = [{ skill_ids: [8, 1], category_hint: nil }]
    build_source(fetcher, search_targets: search_targets, include_latest_list: false, max_detail_fetches: 0).fetch

    assert_equal 1, fetcher.requested_urls.size
    uri = URI.parse(fetcher.requested_urls.first)
    assert_equal LIST_API_PATH, uri.path
    query_pairs = URI.decode_www_form(uri.query)
    skill_id_values = query_pairs.select { |key, _| key == "search_project[searchable_language_skill_ids][]" }
                                  .map { |_, value| value }
    assert_equal %w[8 1], skill_id_values, "skill_idsの数だけ、配列の順序どおりに繰り返すはず"
    assert_includes query_pairs, %w[page 1]
  end

  def test_fetch_builds_latest_list_url_without_skill_ids
    fetcher = ScriptedFetcher.new({}, fallback_body: read_fixture(LIST_API_EMPTY_FIXTURE_NAME))
    build_source(fetcher, search_targets: [], include_latest_list: true, max_detail_fetches: 0).fetch

    assert_equal 1, fetcher.requested_urls.size
    uri = URI.parse(fetcher.requested_urls.first)
    assert_equal LIST_API_PATH, uri.path
    query_pairs = URI.decode_www_form(uri.query)
    refute(
      query_pairs.any? { |key, _| key == "search_project[searchable_language_skill_ids][]" },
      "全体新着一覧はskill指定なしのはず"
    )
    assert_equal [%w[page 1]], query_pairs
  end

  # --- ページングの打ち切り条件: state == "opened" が1件も無いページに当たるまで、最大3ページ ---

  def test_fetch_continues_paging_while_opened_projects_exist_and_stops_when_a_page_has_none
    fetcher = QueueFetcher.new(
      [
        read_fixture(LIST_API_FIXTURE_NAME),                    # page1: opened 2件 + closed 2件
        read_fixture(LIST_API_PAGE2_CLOSED_ONLY_FIXTURE_NAME)   # page2: closedのみ
      ]
    )
    search_targets = [{ skill_ids: [3, 19], category_hint: "Ruby" }]
    build_source(fetcher, search_targets: search_targets, include_latest_list: false, max_detail_fetches: 0).fetch

    assert_equal 2, fetcher.requested_urls.size,
                 "page1にopenedがあるのでpage2は取りに行くが、page2はclosedのみなのでpage3は取りに行かないはず"
  end

  def test_fetch_does_not_page_when_first_page_has_no_opened_projects
    fetcher = QueueFetcher.new([read_fixture(LIST_API_PAGE2_CLOSED_ONLY_FIXTURE_NAME)])
    search_targets = [{ skill_ids: [3, 19], category_hint: "Ruby" }]
    postings = build_source(fetcher, search_targets: search_targets, include_latest_list: false,
                                      max_detail_fetches: 0).fetch

    assert_equal 1, fetcher.requested_urls.size
    assert_equal 1, postings.size, "page1の業務委託1件(20644)は募集終了のまま採用されるはず（正しくJSON APIを読めているかの確認）"
  end

  def test_fetch_stops_paging_at_max_three_pages_even_when_every_page_has_opened_projects
    fetcher = QueueFetcher.new(Array.new(3) { read_fixture(LIST_API_FIXTURE_NAME) })
    search_targets = [{ skill_ids: [3, 19], category_hint: "Ruby" }]
    build_source(fetcher, search_targets: search_targets, include_latest_list: false, max_detail_fetches: 0).fetch

    assert_equal 3, fetcher.requested_urls.size, "openedが続いても最大3ページで打ち切るはず"
  end

  def test_fetch_treats_empty_project_list_page_as_no_opened_projects_without_raising
    fetcher = QueueFetcher.new([read_fixture(LIST_API_EMPTY_FIXTURE_NAME)])
    search_targets = [{ skill_ids: [3, 19], category_hint: "Ruby" }]
    postings = build_source(fetcher, search_targets: search_targets, include_latest_list: false,
                                      max_detail_fetches: 0).fetch

    assert_equal 1, fetcher.requested_urls.size
    assert_equal [], postings
  end

  def test_fetch_paginates_each_target_independently
    fetcher = QueueFetcher.new(
      [
        read_fixture(LIST_API_FIXTURE_NAME),                    # 対象A page1: openedあり
        read_fixture(LIST_API_PAGE2_CLOSED_ONLY_FIXTURE_NAME),  # 対象A page2: closedのみ→打ち切り
        read_fixture(LIST_API_PAGE2_CLOSED_ONLY_FIXTURE_NAME)   # 対象B page1: closedのみ→1ページで打ち切り
      ]
    )
    search_targets = [
      { skill_ids: [3], category_hint: "Ruby" },
      { skill_ids: [5], category_hint: "TypeScript" }
    ]
    build_source(fetcher, search_targets: search_targets, include_latest_list: false, max_detail_fetches: 0).fetch

    assert_equal 3, fetcher.requested_urls.size, "対象Aは2ページ、対象Bは1ページで打ち切るはず（合計3リクエスト）"
  end

  # --- DEFAULT_SEARCH_TARGETS は skill_ids ベース（HTML=8 / CSS=1 / Ruby=3 / TypeScript=5 / React=15 / Ruby on Rails=19） ---

  def test_default_search_targets_use_skill_ids
    assert_equal(
      [
        { skill_ids: [3, 19], category_hint: "Ruby" },
        { skill_ids: [5], category_hint: "TypeScript" },
        { skill_ids: [15], category_hint: "React" },
        { skill_ids: [8, 1], category_hint: nil }
      ],
      FreelanceJobs::Sources::Sokudan::DEFAULT_SEARCH_TARGETS
    )
    assert_equal 1.5, FreelanceJobs::Sources::Sokudan::REQUEST_INTERVAL
  end

  # --- Profile::ENGINEER にSokudanが含まれる（BEGINNERには含まれない） ---

  def test_engineer_profile_includes_sokudan_source
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_includes source_classes, FreelanceJobs::Sources::Sokudan
  end

  def test_beginner_profile_does_not_include_sokudan_source
    source_classes = FreelanceJobs::Profile::BEGINNER.source_specs.map(&:first)

    refute_includes source_classes, FreelanceJobs::Sources::Sokudan
  end

  private

  def build_source(fetcher, search_targets: FreelanceJobs::Sources::Sokudan::DEFAULT_SEARCH_TARGETS,
                    include_latest_list: true, max_detail_fetches: FreelanceJobs::Sources::Sokudan::MAX_DETAIL_FETCHES)
    FreelanceJobs::Sources::Sokudan.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets,
      include_latest_list: include_latest_list, max_detail_fetches: max_detail_fetches
    )
  end

  def server_error
    FreelanceJobs::FetchError.new("HTTP 500 #{skill_list_page_url([3], 1)}")
  end

  def empty_list_body
    build_list_body([])
  end

  # 取得失敗の警告ログでテスト出力が汚れるのを防ぐ。FreelanceJobs.loggerはメモ化された
  # インスタンス変数で差し替え口が無いため、テスト中だけ直接入れ替えて必ず戻す。
  def silencing_fetch_logs
    original_logger = FreelanceJobs.logger
    FreelanceJobs.instance_variable_set(:@logger, Logger.new(File::NULL))
    yield
  ensure
    FreelanceJobs.instance_variable_set(:@logger, original_logger)
  end

  # スキル一覧APIのURL（クエリはsearch_project[searchable_language_skill_ids][]をskill_idsの数だけ
  # 繰り返し、末尾にpageを付ける）。fetcher スタブへの台本キーとして使う。
  def skill_list_page_url(skill_ids, page)
    query = skill_ids.map { |skill_id| "search_project[searchable_language_skill_ids][]=#{skill_id}" }.join("&")
    "#{LIST_API_URL}?#{query}&page=#{page}"
  end

  # 全体新着一覧APIのURL（skill指定なし）。
  def latest_list_page_url(page = 1)
    "#{LIST_API_URL}?page=#{page}"
  end

  # 詳細ページ（人が見る/top/projects/<id>）のURL。一覧APIのURLとは別物。
  def detail_url(project_id)
    "#{LIST_URL}/#{project_id}"
  end

  # 詳細フィクスチャの staticProject を取り出す（値を書き換えて境界ケースの本文を組み立てるため）。
  def detail_project(fixture_name)
    next_data = JSON.parse(Nokogiri::HTML(read_fixture(fixture_name)).at_css("script#__NEXT_DATA__").text)
    next_data["props"]["pageProps"]["staticProject"]
  end

  # script#__NEXT_DATA__ に JSON を埋めた本文（parse_detail用。実データと同じ構造）。
  def build_next_data_body(page_props)
    payload = { "props" => { "pageProps" => page_props }, "page" => "/top/projects" }
    wrap_html(%(<script id="__NEXT_DATA__" type="application/json">#{JSON.generate(payload)}</script>))
  end

  def build_detail_body(project)
    build_next_data_body("staticProject" => project)
  end

  # 一覧APIの応答本文（JSON文字列）。parseが読むのはprojectListキーだけなので、テストではこのキーだけで足りる
  # （実際のAPI応答はbreadCrumbTextやmetatag等の他のトップキーも持つが、フィクスチャファイル側で構造を確認する）。
  def build_list_body(project_list)
    JSON.generate("projectList" => project_list)
  end

  # 一覧の複数のテスト（重複排除・リトライ・予算上限など）で共通して使う、1件だけ・募集終了・
  # 1ページ（openedが無い）で打ち切られる一覧本文。
  def closed_single_project_list_body(id: 20807)
    build_list_body([build_list_project(id: id, title: "テスト案件", state: "closed")])
  end

  # 一覧の案件要素1件分（デフォルト値は募集中・業務委託の正常系）。
  def build_list_project(id:, title:, state: "opened", contract_type: "outsourcing")
    {
      "id" => id, "title" => title, "state" => state, "contractType" => contract_type,
      "createdAt" => "2026-09-12T10:00:00.000+09:00",
      "minBudget" => { "id" => 500_000, "label" => "50万" }, "maxBudget" => { "id" => 700_000, "label" => "70万" },
      "remoteType" => { "name" => "full_remote", "label" => "フルリモート(在宅OK)" },
      "projectAvailableTime" => { "name" => "over_sixteen_hours", "label" => "週2日（週16~23h）" },
      "minWorkingHoursLabel" => "週16h", "prefecture" => { "label" => "全国" },
      "tags" => [{ "name" => "", "label" => "フリーランス歓迎" }],
      "professions" => [{ "name" => "backend_engineer", "label" => "バックエンドエンジニア" }],
      "corporation" => { "name" => "＊＊＊＊＊" }
    }
  end
end
