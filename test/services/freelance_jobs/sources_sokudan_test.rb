# frozen_string_literal: true
# test/services/freelance_jobs/sources_sokudan_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"
require "json"
require "logger"

class FreelanceJobsSourcesSokudanTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 13)
  LIST_FIXTURE_NAME = "sokudan_ruby.html"
  DETAIL_FIXTURE_NAME = "sokudan_detail.html"
  FULL_REMOTE_DETAIL_FIXTURE_NAME = "sokudan_detail_full_remote.html"

  LIST_URL = "https://sokudan.work/top/projects"
  SKILL_LIST_URL_PREFIX = "https://sokudan.work/top/projects/required_skills"

  def parse_fixture(category_hint: "Ruby")
    FreelanceJobs::Sources::Sokudan.parse(read_fixture(LIST_FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  def parse_detail_fixture(fixture_name, category_hint: "Ruby")
    FreelanceJobs::Sources::Sokudan.parse_detail(read_fixture(fixture_name), today: TODAY, category_hint: category_hint)
  end

  # 一覧の projectList は40件。うち募集中(opened)は4件で、正社員(full_time)の1件を除いた
  # 業務委託系3件（20807 / 19994 / 13278）だけが案件として採用される。
  def test_parse_fixture_returns_three_side_job_postings
    postings = parse_fixture

    assert_equal 3, postings.size
    assert_equal(
      %w[
        https://sokudan.work/top/projects/20807
        https://sokudan.work/top/projects/19994
        https://sokudan.work/top/projects/13278
      ],
      postings.map(&:url)
    )
  end

  def test_parse_excludes_closed_and_full_time_projects
    urls = parse_fixture.map(&:url)

    refute_includes urls, "https://sokudan.work/top/projects/11866", "正社員(full_time)は副業案件でないので除外するはず"
    refute_includes urls, "https://sokudan.work/top/projects/20720", "募集終了(closed)は除外するはず"
  end

  # --- 一覧1件目の全フィールド（使用技術・本文は一覧に無いので空、掲載日は createdAt） ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "SOKUDAN", first.site
    assert_equal "https://sokudan.work/top/projects/20807", first.url
    assert_equal "【一部リモ可】全国展開化粧品ブランドのECシステムエンジニアを募集！", first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "650,000〜950,000円／月", first.reward
    assert_equal "月額制（業務委託）", first.work_format
    assert_equal "募集中", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_equal [], first.skills
    assert_equal "", first.client, "未ログインでは企業名がマスクされるので空のはず"
    assert_equal ["業務委託", "長期", "急募", "即日勤務OK"], first.tags
    assert_equal Date.new(2026, 9, 9), first.posted_on
  end

  def test_parse_first_posting_description_uses_list_level_fields_only
    description = parse_fixture.first.description

    assert_equal(
      "募集職種: バックエンドエンジニア / 稼働: 週5日（週40h）（週40h〜） / 勤務形態: リモート(一部)可 / " \
      "勤務地: 首都圏 / 契約形態: 業務委託",
      description
    )
  end

  def test_parse_posting_with_outsourcing_to_full_time_contract
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/13278") }

    refute_nil posting
    assert_equal "業務委託→正社員", posting.tags.first
    assert_includes posting.description, "契約形態: 業務委託→正社員"
  end

  # --- 詳細ページ: 使用技術・本文・掲載日まで全フィールドが埋まる ---

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
    parse_fixture.each do |posting|
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
    parse_fixture.each do |posting|
      assert_match %r{\Ahttps://sokudan\.work/top/projects/\d+\z}, posting.url,
                   "末尾スラッシュなし・クエリなしの正規化された絶対URLのはず"
    end
  end

  # --- category_hint が引数どおり全件に伝わる（nil も許す） ---

  def test_category_hint_is_propagated_to_every_posting
    postings = parse_fixture(category_hint: "TypeScript")

    assert(postings.all? { |posting| posting.category_hint == "TypeScript" })
    assert(parse_fixture(category_hint: nil).all? { |posting| posting.category_hint.nil? })
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

  def test_parse_returns_empty_when_next_data_script_is_missing
    assert_equal [], FreelanceJobs::Sources::Sokudan.parse(wrap_html("<div>該当なし</div>"), today: TODAY)
  end

  def test_parse_returns_empty_when_json_is_broken
    body = wrap_html('<script id="__NEXT_DATA__" type="application/json">{"props": </script>')

    assert_equal [], FreelanceJobs::Sources::Sokudan.parse(body, today: TODAY)
  end

  def test_parse_returns_empty_when_project_list_is_not_an_array
    body = build_list_body("not an array")

    assert_equal [], FreelanceJobs::Sources::Sokudan.parse(body, today: TODAY)
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

  # --- fetch: 一覧5本 → 候補を新しい順に詳細取得、URL重複を排除する ---

  # URLごとの応答を台本化するFakeフェッチャー（通信しない）。
  # 各URLの配列を先頭から1回ずつ消費し、例外なら送出・文字列ならbodyとして返す。
  # 台本に無いURL・使い切ったURLは fallback_body を返す。
  class ScriptedFetcher
    def initialize(script, fallback_body:)
      @script = script
      @fallback_body = fallback_body
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      response = @script[url]&.shift || @fallback_body
      raise response if response.is_a?(StandardError)

      response
    end
  end

  def test_fetch_requests_each_list_then_details_and_deduplicates_urls
    fetcher = ScriptedFetcher.new(
      { "#{LIST_URL}/20807" => [read_fixture(DETAIL_FIXTURE_NAME)] },
      fallback_body: read_fixture(LIST_FIXTURE_NAME)
    )
    postings = build_source(fetcher).fetch

    assert_equal(
      [
        "#{SKILL_LIST_URL_PREFIX}/Ruby",
        "#{SKILL_LIST_URL_PREFIX}/ruby_on_rails",
        "#{SKILL_LIST_URL_PREFIX}/TypeScript",
        "#{SKILL_LIST_URL_PREFIX}/React",
        LIST_URL,
        "#{LIST_URL}/20807",
        "#{LIST_URL}/19994",
        "#{LIST_URL}/13278"
      ],
      fetcher.requested_urls,
      "一覧5本のあと、候補3件を createdAt 降順で詳細取得するはず"
    )
    assert_equal 3, postings.size, "5一覧すべてに同じ3件が出ても重複排除されるはず"
    assert_equal %w[Ruby Ruby Ruby], postings.map(&:category_hint), "最初に出た一覧（Ruby）のhintを保持するはず"
  end

  def test_fetch_overrides_list_posting_with_detail_and_keeps_list_posting_when_detail_is_unreadable
    fetcher = ScriptedFetcher.new(
      { "#{LIST_URL}/20807" => [read_fixture(DETAIL_FIXTURE_NAME)] },
      fallback_body: read_fixture(LIST_FIXTURE_NAME)
    )
    postings = build_source(fetcher).fetch

    detailed = postings.find { |posting| posting.url.end_with?("/20807") }
    assert_equal 12, detailed.skills.size, "詳細が取れた案件は requiredSkills で上書きされるはず"
    assert_includes detailed.description, "案件詳細:"

    list_only = postings.find { |posting| posting.url.end_with?("/19994") }
    assert_equal [], list_only.skills, "詳細JSONが読めない案件（一覧ページが返った）は一覧の内容のままのはず"
  end

  def test_fetch_skips_latest_list_when_option_is_off
    fetcher = ScriptedFetcher.new({}, fallback_body: read_fixture(LIST_FIXTURE_NAME))
    build_source(fetcher, include_latest_list: false).fetch

    refute_includes fetcher.requested_urls, LIST_URL
    assert_equal 4 + 3, fetcher.requested_urls.size
  end

  # 全体新着一覧にしか無い案件は createdAt が直近 latest_lookback_days 日のものだけ採る。
  # today=2026-09-10 なら 20807(09-09) は採り、19994(07-31)・13278(2025-01) は落とす。
  def test_fetch_takes_only_recent_candidates_from_latest_list
    fetcher = ScriptedFetcher.new({}, fallback_body: read_fixture(LIST_FIXTURE_NAME))
    source = FreelanceJobs::Sources::Sokudan.new(
      fetcher: fetcher, today: Date.new(2026, 9, 10), search_targets: [], include_latest_list: true
    )
    postings = source.fetch

    assert_equal [LIST_URL, "#{LIST_URL}/20807"], fetcher.requested_urls
    assert_equal ["https://sokudan.work/top/projects/20807"], postings.map(&:url)
    assert_nil postings.first.category_hint, "全体新着由来は category_hint なし（本文判定に任せる）のはず"
  end

  # 詳細の取得件数は max_detail_fetches で打ち切り、溢れた古い候補は一覧の内容で採用する。
  def test_fetch_limits_detail_requests_and_keeps_overflow_candidates_from_list
    fetcher = ScriptedFetcher.new(
      { "#{LIST_URL}/20807" => [read_fixture(DETAIL_FIXTURE_NAME)] },
      fallback_body: read_fixture(LIST_FIXTURE_NAME)
    )
    postings = build_source(fetcher, include_latest_list: false, max_detail_fetches: 1).fetch

    assert_equal 4 + 1, fetcher.requested_urls.size, "詳細は最新の1件だけ取るはず"
    assert_equal "#{LIST_URL}/20807", fetcher.requested_urls.last
    assert_equal 3, postings.size, "詳細を取らなかった候補も一覧の内容で残るはず"
  end

  # 一覧5本＋詳細35件で予算40。max_detail_fetches を大きくしても予算は超えない。
  def test_detail_fetch_limit_never_exceeds_request_budget
    fetcher = ScriptedFetcher.new({}, fallback_body: read_fixture(LIST_FIXTURE_NAME))
    source = build_source(fetcher, max_detail_fetches: 100)

    assert_equal 35, source.send(:detail_fetch_limit)
    assert_equal 40, FreelanceJobs::Sources::Sokudan::REQUEST_BUDGET
    assert_equal 35, FreelanceJobs::Sources::Sokudan::MAX_DETAIL_FETCHES
  end

  # 一覧取得〜詳細取得の間に締め切られた案件は落とす。
  def test_fetch_drops_project_closed_at_detail
    closed_project = detail_project(DETAIL_FIXTURE_NAME)
    closed_project["state"] = "closed"
    fetcher = ScriptedFetcher.new(
      { "#{LIST_URL}/20807" => [build_detail_body(closed_project)] },
      fallback_body: read_fixture(LIST_FIXTURE_NAME)
    )
    postings = build_source(fetcher, include_latest_list: false).fetch

    refute_includes postings.map(&:url), "https://sokudan.work/top/projects/20807"
    assert_equal 2, postings.size
  end

  # --- 散発的な取得失敗に耐える（1回リトライ → その単位だけ打ち切り → 全滅時のみ例外） ---

  def test_fetch_retries_once_when_a_list_returns_server_error
    fetcher = ScriptedFetcher.new(
      { "#{SKILL_LIST_URL_PREFIX}/Ruby" => [server_error, read_fixture(LIST_FIXTURE_NAME)] },
      fallback_body: empty_list_body
    )
    postings = silencing_fetch_logs { build_source(fetcher, skill_slugs: %w[Ruby], include_latest_list: false).fetch }

    assert_equal 3, postings.size, "1回目が失敗でも再取得すれば取得できるはず"
    assert_equal ["#{SKILL_LIST_URL_PREFIX}/Ruby", "#{SKILL_LIST_URL_PREFIX}/Ruby"], fetcher.requested_urls.first(2)
  end

  # HTTPエラーだけでなく通信層の切断・タイムアウトも同じく1一覧の打ち切りで済ませる。
  def test_fetch_skips_failing_list_and_keeps_other_lists
    connection_error = Errno::ECONNRESET.new("Connection reset by peer")
    fetcher = ScriptedFetcher.new(
      {
        "#{SKILL_LIST_URL_PREFIX}/TypeScript" => [connection_error, connection_error],
        "#{SKILL_LIST_URL_PREFIX}/Ruby" => [read_fixture(LIST_FIXTURE_NAME)]
      },
      fallback_body: empty_list_body
    )
    postings = silencing_fetch_logs do
      build_source(fetcher, skill_slugs: %w[TypeScript Ruby], include_latest_list: false).fetch
    end

    assert_equal 3, postings.size, "TypeScript一覧が落ちてもRuby一覧の結果は返すはず"
  end

  def test_fetch_keeps_list_posting_when_detail_fails_twice
    detail_error = FreelanceJobs::FetchError.new("HTTP 500 #{LIST_URL}/20807")
    fetcher = ScriptedFetcher.new(
      {
        "#{SKILL_LIST_URL_PREFIX}/Ruby" => [read_fixture(LIST_FIXTURE_NAME)],
        "#{LIST_URL}/20807" => [detail_error, detail_error]
      },
      fallback_body: empty_list_body
    )
    postings = silencing_fetch_logs { build_source(fetcher, skill_slugs: %w[Ruby], include_latest_list: false).fetch }

    assert_equal 3, postings.size, "詳細に2回失敗した案件も一覧の内容で残るはず"
    assert_equal 2, fetcher.requested_urls.count("#{LIST_URL}/20807"), "詳細も1回だけ再取得するはず"
  end

  # 全滅を黙って0件で返すとサイト構造の崩れに気付けないため、最初の失敗を送出する。
  def test_fetch_raises_when_no_list_succeeds
    error = server_error
    fetcher = ScriptedFetcher.new({ "#{SKILL_LIST_URL_PREFIX}/Ruby" => [error, error] }, fallback_body: empty_list_body)

    raised = assert_raises(FreelanceJobs::FetchError) do
      silencing_fetch_logs { build_source(fetcher, skill_slugs: %w[Ruby], include_latest_list: false).fetch }
    end

    assert_equal error.message, raised.message
  end

  # WAFのアクセス制限は取り直しても解消しないので、再取得せずそのまま送出する。
  def test_fetch_does_not_retry_when_access_is_blocked
    fetcher = ScriptedFetcher.new(
      { "#{SKILL_LIST_URL_PREFIX}/Ruby" => [FreelanceJobs::AccessBlockedError.new("アクセス制限（WAF captcha）")] },
      fallback_body: empty_list_body
    )

    assert_raises(FreelanceJobs::AccessBlockedError) do
      silencing_fetch_logs { build_source(fetcher, skill_slugs: %w[Ruby]).fetch }
    end
    assert_equal 1, fetcher.requested_urls.size, "アクセス制限では再取得しないはず"
  end

  def test_default_search_targets_cover_four_skill_slugs
    slugs = FreelanceJobs::Sources::Sokudan::DEFAULT_SEARCH_TARGETS.map { |target| target[:skill_slug] }

    assert_equal %w[Ruby ruby_on_rails TypeScript React], slugs
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

  def build_source(fetcher, skill_slugs: nil, include_latest_list: true, max_detail_fetches: 35)
    search_targets = if skill_slugs
                       skill_slugs.map { |skill_slug| { skill_slug: skill_slug, category_hint: "Ruby" } }
                     else
                       FreelanceJobs::Sources::Sokudan::DEFAULT_SEARCH_TARGETS
                     end
    FreelanceJobs::Sources::Sokudan.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets,
      include_latest_list: include_latest_list, max_detail_fetches: max_detail_fetches
    )
  end

  def server_error
    FreelanceJobs::FetchError.new("HTTP 500 #{SKILL_LIST_URL_PREFIX}/Ruby")
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

  # 詳細フィクスチャの staticProject を取り出す（値を書き換えて境界ケースの本文を組み立てるため）。
  def detail_project(fixture_name)
    next_data = JSON.parse(Nokogiri::HTML(read_fixture(fixture_name)).at_css("script#__NEXT_DATA__").text)
    next_data["props"]["pageProps"]["staticProject"]
  end

  # script#__NEXT_DATA__ に JSON を埋めた本文（実データと同じ構造）。
  def build_next_data_body(page_props)
    payload = { "props" => { "pageProps" => page_props }, "page" => "/top/projects" }
    wrap_html(%(<script id="__NEXT_DATA__" type="application/json">#{JSON.generate(payload)}</script>))
  end

  def build_list_body(project_list)
    build_next_data_body("staticProjectSearchResult" => { "projectList" => project_list })
  end

  def build_detail_body(project)
    build_next_data_body("staticProject" => project)
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
