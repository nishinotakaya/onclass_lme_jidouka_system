# frozen_string_literal: true
# test/services/freelance_jobs/sources_findy_freelance_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"
require "json"

class FreelanceJobsSourcesFindyFreelanceTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 12)

  # Ruby一覧（/works/languages/ruby）と React一覧（/works/skills/react）の実データ。
  # 保存時点で initialWorkList はどちらも10件。
  RUBY_FIXTURE = "findy_freelance_ruby.html"
  REACT_FIXTURE = "findy_freelance_react.html"

  def parse_ruby_fixture
    FreelanceJobs::Sources::FindyFreelance.parse(read_fixture(RUBY_FIXTURE), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_fixture_returns_ten_postings
    assert_equal 10, parse_ruby_fixture.size
  end

  # Reactは /works/skills/<slug> という別ページ型だが pageProps の形は同一なので、同じparseで通る。
  def test_parse_handles_skills_page_type_with_the_same_json_path
    postings = FreelanceJobs::Sources::FindyFreelance.parse(read_fixture(REACT_FIXTURE), today: TODAY, category_hint: "React")

    assert_equal 10, postings.size
    assert_equal "https://freelance.findy-code.io/works/j_1gN39PdXsjJ", postings.first.url
  end

  # --- 1件目の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    first = parse_ruby_fixture.first

    assert_equal "Findy Freelance", first.site
    assert_equal "https://freelance.findy-code.io/works/_fhNZS9onGdfx", first.url
    assert_equal "【週5日/フルリモート/Ruby,TypeScript】フルスタックエンジニア - " \
                 "急成長中の現場向け動画教育SaaSにおける新機能開発および技術負債解消", first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "〜1,280,000円／月", first.reward
    assert_equal "月額制（業務委託）", first.work_format
    assert_equal ["Ruby", "TypeScript"], first.skills
    assert_equal "", first.client
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_equal ["NEW", "面談1回", "急募案件", "自社サービス開発", "弊社経由参画実績あり", "BtoB"], first.tags
    assert_equal Date.new(2026, 9, 11), first.posted_on
  end

  def test_parse_first_posting_description_contains_classifier_relevant_fields
    description = parse_ruby_fixture.first.description

    assert_includes description, "募集職種: フルスタックエンジニア"
    assert_includes description, "使用技術: Ruby / TypeScript"
    assert_includes description, "稼働: 週5日"
    assert_includes description, "勤務形態: フルリモート"
    assert_includes description, "参画メリット: - 仕様策定からデリバリーまで"
  end

  # 事業内容(company.profile.businessAbstract)を入れると EngineerClassifier::NON_DEV_RE の
  # 「営業」に誤爆して案件が丸ごと落ちるため、意図的に description へ含めていない。
  def test_description_excludes_company_business_abstract
    descriptions = parse_ruby_fixture.map(&:description)

    refute(descriptions.any? { |description| description.include?("物流／製造／清掃／販売") },
           "事業内容は分類器の誤爆源になるためdescriptionに含めないはず")
  end

  # --- 稼働日数の幅表記 ---

  def test_description_uses_range_notation_when_working_days_have_a_range
    posting = parse_ruby_fixture.find { |candidate| candidate.url.end_with?("/yE3S5txc1ZE1o") }

    refute_nil posting
    assert_includes posting.description, "稼働: 週4〜5日"
  end

  # --- skills は開発言語＋開発スキルの連結 ---

  def test_skills_merge_development_languages_and_development_skills
    posting = parse_ruby_fixture.find { |candidate| candidate.url.end_with?("/B9JYRQj3ptdJ0") }

    refute_nil posting
    assert_equal ["Ruby", "TypeScript", "Rails", "React", "Next.js"], posting.skills
  end

  # --- URL正規化 ---

  def test_urls_are_normalized_absolute_without_trailing_slash_or_query
    parse_ruby_fixture.each do |posting|
      assert_match %r{\Ahttps://freelance\.findy-code\.io/works/\w+\z}, posting.url,
                   "末尾スラッシュなし・クエリなしの正規化されたURLのはず"
    end
  end

  # workHash は `_` 始まり・`_` 終わりがあり得るので、URL組み立てで削れていないことを確かめる。
  def test_urls_keep_underscores_at_both_ends_of_work_hash
    urls = parse_ruby_fixture.map(&:url)

    assert_includes urls, "https://freelance.findy-code.io/works/_fhNZS9onGdfx"
    assert_includes urls, "https://freelance.findy-code.io/works/0ezwrVGBbLqs_"
  end

  # --- category_hint が引数どおり全件に伝わる ---

  def test_category_hint_is_propagated_to_every_posting
    postings = parse_ruby_fixture

    assert(postings.all? { |posting| posting.category_hint == "Ruby" },
           "全件のcategory_hintが引数のRubyになるはず")
  end

  # --- 報酬・契約形態の分岐（月額優先） ---

  def test_reward_is_monthly_for_every_fixture_posting
    parse_ruby_fixture.each do |posting|
      assert_match(/\A〜[\d,]+円／月\z/, posting.reward, "Findyは月額表記を採用するはず")
      assert_equal "月額制（業務委託）", posting.work_format
    end
  end

  def test_reward_falls_back_to_hourly_when_monthly_wage_is_missing
    body = build_next_data_body([build_work(work_hash: "hourlyOnly", max_monthly_wage: nil, max_hourly_wage: 6000)])
    posting = FreelanceJobs::Sources::FindyFreelance.parse(body, today: TODAY, category_hint: "Ruby").first

    assert_equal "〜6,000円／時", posting.reward
    assert_equal "時間単価制", posting.work_format
  end

  def test_reward_is_unconfirmed_when_both_wages_are_missing
    body = build_next_data_body([build_work(work_hash: "noWage", max_monthly_wage: nil, max_hourly_wage: nil)])
    posting = FreelanceJobs::Sources::FindyFreelance.parse(body, today: TODAY, category_hint: "Ruby").first

    assert_equal "要確認", posting.reward
    assert_equal "業務委託（フリーランス）", posting.work_format
  end

  # --- 必須要素が欠けた案件は黙って除外する ---

  def test_parse_skips_works_without_work_hash_or_title
    body = build_next_data_body(
      [
        build_work(work_hash: "", title: "workHashなし"),
        build_work(work_hash: "titleMissing", title: ""),
        build_work(work_hash: "valid1234567", title: "正常な案件")
      ]
    )
    postings = FreelanceJobs::Sources::FindyFreelance.parse(body, today: TODAY, category_hint: "Ruby")

    assert_equal ["https://freelance.findy-code.io/works/valid1234567"], postings.map(&:url)
  end

  def test_parse_returns_empty_when_next_data_script_is_missing
    postings = FreelanceJobs::Sources::FindyFreelance.parse(wrap_html("<div id=\"__next\"></div>"),
                                                            today: TODAY, category_hint: "Ruby")

    assert_equal [], postings
  end

  def test_parse_returns_empty_when_json_is_broken
    broken_body = wrap_html(%(<script id="__NEXT_DATA__" type="application/json">{"props":</script>))
    postings = FreelanceJobs::Sources::FindyFreelance.parse(broken_body, today: TODAY, category_hint: "Ruby")

    assert_equal [], postings
  end

  def test_parse_returns_empty_when_initial_work_list_key_is_absent
    body = wrap_html(%(<script id="__NEXT_DATA__" type="application/json">#{JSON.generate({ "props" => { "pageProps" => {} } })}</script>))
    postings = FreelanceJobs::Sources::FindyFreelance.parse(body, today: TODAY, category_hint: "Ruby")

    assert_equal [], postings
  end

  # JSONは正しいがトップレベルがオブジェクトでない場合（Hash#digがTypeErrorで落ちるケース）。
  # サイト構造が変わっても「0件になる」形で表面化させ、バッチを落とさない。
  def test_parse_returns_empty_when_json_root_is_not_an_object
    ["[1,2,3]", %("just a string"), "null"].each do |payload|
      body = wrap_html(%(<script id="__NEXT_DATA__" type="application/json">#{payload}</script>))

      assert_equal [], FreelanceJobs::Sources::FindyFreelance.parse(body, today: TODAY, category_hint: "Ruby"),
                   "トップレベルが#{payload}でも例外にせず空配列を返すはず"
    end
  end

  # 途中の階層（props / pageProps）がHash以外に変わった場合も同様に空配列へ倒す。
  def test_parse_returns_empty_when_a_nested_level_is_not_an_object
    [{ "props" => [] }, { "props" => { "pageProps" => "unexpected" } }].each do |payload|
      body = wrap_html(%(<script id="__NEXT_DATA__" type="application/json">#{JSON.generate(payload)}</script>))

      assert_equal [], FreelanceJobs::Sources::FindyFreelance.parse(body, today: TODAY, category_hint: "Ruby"),
                   "途中の階層が#{payload.inspect}でも例外にせず空配列を返すはず"
    end
  end

  # --- fetch: search_targetsの数だけ取得し、一覧をまたいだURL重複を排除する ---

  # URLごとにbodyを返すFakeフェッチャー（呼び出されたURLを記録する）。
  class RecordingFetcher
    def initialize(bodies_by_url:)
      @bodies_by_url = bodies_by_url
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      @bodies_by_url.fetch(url)
    end
  end

  def test_fetch_requests_each_search_target_and_deduplicates_urls_across_lists
    bodies_by_url = {
      "https://freelance.findy-code.io/works/languages/ruby" => read_fixture(RUBY_FIXTURE),
      "https://freelance.findy-code.io/works/skills/react" => read_fixture(REACT_FIXTURE)
    }
    fetcher = RecordingFetcher.new(bodies_by_url: bodies_by_url)
    search_targets = [
      { path: "/works/languages/ruby", hint: "Ruby" },
      { path: "/works/skills/react", hint: "React" }
    ]
    source = FreelanceJobs::Sources::FindyFreelance.new(fetcher: fetcher, today: TODAY, search_targets: search_targets)

    postings = source.fetch

    assert_equal bodies_by_url.keys, fetcher.requested_urls, "search_targetsの数だけ一覧を取得するはず"
    # 2一覧で計20件だが B9JYRQj3ptdJ0 が両方に載るため19件になる。
    assert_equal 19, postings.size
    assert_equal 19, postings.map(&:url).uniq.size
    duplicated = postings.select { |posting| posting.url.end_with?("/B9JYRQj3ptdJ0") }
    assert_equal 1, duplicated.size
    assert_equal "Ruby", duplicated.first.category_hint, "先に取得した一覧のcategory_hintが残るはず"
  end

  def test_default_search_targets_cover_ruby_typescript_and_react
    paths = FreelanceJobs::Sources::FindyFreelance::DEFAULT_SEARCH_TARGETS.map { |target| target[:path] }

    # ReactはFindy上「言語」ではなく「スキル」なので /works/languages/react では取れない。
    assert_equal ["/works/languages/ruby", "/works/languages/typescript", "/works/skills/react"], paths
  end

  # --- 分類器との相互作用（reward・descriptionの設計意図を固定する回帰テスト） ---

  def test_every_fixture_posting_is_classified_with_a_category
    categories = parse_ruby_fixture.map { |posting| FreelanceJobs::EngineerClassifier.classify(posting, today: TODAY).category }

    assert_equal 10, categories.count("Ruby"), "Ruby一覧の10件すべてがRuby案件として分類されるはず"
  end

  def test_monthly_reward_makes_classifier_mark_the_posting_as_high_reward
    result = FreelanceJobs::EngineerClassifier.classify(parse_ruby_fixture.first, today: TODAY)

    assert_includes result.memo, "高単価"
    assert_includes result.memo, "リモート可"
    assert_includes result.memo, "長期・継続あり"
    assert_equal "🌟🌟", result.recommend
  end

  # --- Profile::ENGINEER にFindy Freelanceが含まれる（BEGINNERには含まれない） ---

  def test_engineer_profile_includes_findy_freelance_source
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_includes source_classes, FreelanceJobs::Sources::FindyFreelance
  end

  def test_beginner_profile_does_not_include_findy_freelance_source
    source_classes = FreelanceJobs::Profile::BEGINNER.source_specs.map(&:first)

    refute_includes source_classes, FreelanceJobs::Sources::FindyFreelance
  end

  private

  # __NEXT_DATA__ を持つ最小のページを組み立てる（initialWorkList だけ差し替える）。
  def build_next_data_body(work_list)
    payload = { "props" => { "pageProps" => { "initialWorkList" => work_list } } }
    wrap_html(%(<script id="__NEXT_DATA__" type="application/json">#{JSON.generate(payload)}</script>))
  end

  # initialWorkList 1件分（実データのキー構成に合わせた正常系がデフォルト）。
  def build_work(work_hash: "sampleHash12", title: "【週5日/フルリモート/Ruby】バックエンドエンジニア",
                 max_monthly_wage: 800_000, max_hourly_wage: 5_000, opened_at: "2026-09-11T16:02:32+09:00",
                 development_languages: ["Ruby"], development_skills: ["Rails"], is_new_opened: false)
    {
      "workHash" => work_hash,
      "title" => title,
      "maxMonthlyWage" => max_monthly_wage,
      "maxHourlyWage" => max_hourly_wage,
      "openedAt" => opened_at,
      "isNewOpened" => is_new_opened,
      "jobType" => { "id" => 7, "name" => "バックエンドエンジニア" },
      "minDaysPerWeek" => 5,
      "maxDaysPerWeek" => 5,
      "developmentLanguages" => development_languages.map { |name| { "id" => 1, "name" => name } },
      "developmentSkills" => development_skills.map { |name| { "id" => 2, "name" => name } },
      "remoteWork" => { "id" => 4, "name" => "フルリモート" },
      "workCharacteristics" => [{ "id" => 1, "name" => "面談1回" }],
      "participationBenefits" => "- テスト用の参画メリットです。"
    }
  end
end
