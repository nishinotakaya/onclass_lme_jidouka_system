# frozen_string_literal: true
# test/services/freelance_jobs/sources_levtech_creator_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesLevtechCreatorTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 12)

  # Ruby検索は実際に3件しかヒットしないため、境界・異常系はカードが20件あるReact検索のfixtureを使う。
  RUBY_FIXTURE = "levtech_creator_ruby.html"
  REACT_FIXTURE = "levtech_creator_react.html"

  def parse_ruby_fixture
    FreelanceJobs::Sources::LevtechCreator.parse(read_fixture(RUBY_FIXTURE), today: TODAY, category_hint: "Ruby")
  end

  def parse_react_fixture
    FreelanceJobs::Sources::LevtechCreator.parse(read_fixture(REACT_FIXTURE), today: TODAY, category_hint: "React")
  end

  # --- 件数（セレクタが外れて静かに0件になっていないことの検知を兼ねる） ---

  def test_parse_ruby_fixture_returns_three_postings
    assert_equal 3, parse_ruby_fixture.size
  end

  def test_parse_react_fixture_returns_twenty_postings
    assert_equal 20, parse_react_fixture.size
  end

  # --- 1件目の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    first = parse_ruby_fixture.first

    assert_equal "レバテッククリエイター", first.site
    assert_equal "https://creator.levtech.jp/project/detail/548083", first.url
    # 親のh3を読むとアンカー外の「の求人・案件」が混ざるので、a.projectNameだけを見る。
    assert_equal "【Java】アプリケーション構築運用", first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "〜5,180円／時", first.reward
    assert_equal "時間単価制", first.work_format
    assert_equal ["GitHub", "Git"], first.skills
    assert_equal ["フロントエンドエンジニア"], first.tags
    assert_equal "", first.client
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_nil first.posted_on
  end

  # --- description に一覧の情報が漏れなく集まる（分類器の判定材料になる） ---

  def test_description_joins_occupation_detail_table_skills_location_and_business_comment
    first = parse_ruby_fixture.first

    assert_equal(
      "募集職種: フロントエンドエンジニア / " \
      "作業内容: ・広告配信商材の開発および運用に携わっていただきます。 ・主に下記作業をご担当いただきます。 " \
      "- 調査 - 実装 - 環境構築 - 運用 - 移行 - 技術文書作成 / " \
      "求めるスキル: ・Javaを用いた開発経験 ・Rubyを用いた開発経験 ・既存システムの調査... / " \
      "ツール・言語: GitHub / Git / " \
      "勤務地: 高円寺（東京都） / " \
      "レバテックでの実績がある企業の案件でございます。 Javaの経験を活かすことができます。 " \
      "新しいアイディアや技術を積極的に導入し、 経験豊富なメンバーと成長が出来る環境でございます。 " \
      "スキルアップされたい方、長期的に参画されたい...",
      first.description
    )
  end

  # 営業コメントは一覧で唯一リモート勤務に触れる箇所なので、欠かすと分類のリモート判定が死ぬ。
  def test_description_includes_business_comment_so_remote_work_is_detectable
    remote_postings = parse_react_fixture.select do |posting|
      posting.description.match?(FreelanceJobs::EngineerClassifier::REMOTE_RE)
    end

    refute_empty remote_postings, "営業コメント由来のリモート表記が description に残っているはず"
  end

  # --- 単価の単位で work_format が分岐する ---

  def test_work_format_is_hourly_when_reward_includes_per_hour_unit
    hourly_posting = parse_ruby_fixture.find { |posting| posting.url.end_with?("/548083") }

    refute_nil hourly_posting
    assert_equal "〜5,180円／時", hourly_posting.reward
    assert_equal "時間単価制", hourly_posting.work_format
  end

  def test_work_format_is_monthly_when_reward_includes_per_month_unit
    monthly_posting = parse_ruby_fixture.find { |posting| posting.url.end_with?("/547887") }

    refute_nil monthly_posting
    assert_equal "〜900,000円／月", monthly_posting.reward
    assert_equal "月額制（業務委託）", monthly_posting.work_format
  end

  # --- URL正規化（href は "/project/detail/548083/" と末尾スラッシュ付きで出る） ---

  def test_urls_are_normalized_absolute_without_trailing_slash_or_query
    parse_react_fixture.each do |posting|
      assert_match %r{\Ahttps://creator\.levtech\.jp/project/detail/\d+\z}, posting.url,
                   "末尾スラッシュなし・クエリなしの正規化されたURLのはず"
    end
  end

  # --- category_hint が引数どおり全件に伝わる ---

  def test_category_hint_is_propagated_to_every_posting
    assert(parse_react_fixture.all? { |posting| posting.category_hint == "React" },
           "全件のcategory_hintが引数のReactになるはず")
  end

  # --- skills は「ツール・言語」のリンク列から取る ---

  def test_skills_are_collected_from_tool_language_links
    first = parse_react_fixture.first

    assert_equal ["HTML", "CSS", "PHP", "JavaScript", "React", "Git"], first.skills
  end

  # 「ツール・言語」のdl自体が無いカードが実在する（このfixtureでは20件中1件）。
  def test_skills_are_empty_when_tool_language_definition_list_is_missing
    posting = parse_react_fixture.find { |candidate| candidate.url.end_with?("/577721") }

    refute_nil posting
    assert_equal [], posting.skills
    refute_includes posting.description, "ツール・言語:", "skillsが空のときは説明にもツール・言語を出さない"
  end

  # --- tags は職種（featureList/statusLabel が存在しないサイトなので代用している） ---

  def test_tags_are_split_when_card_has_multiple_occupations
    first = parse_react_fixture.first

    assert_equal ["フロントエンドエンジニア", "HTMLコーダー"], first.tags
  end

  # 職種のli自体が欠けるカードもある。並び順固定で読むと勤務地を職種として拾ってしまうため、
  # 勤務地が正しく勤務地のまま残ることまで確認する。
  def test_tags_are_empty_and_location_is_still_correct_when_occupation_item_is_missing
    posting = parse_react_fixture.find { |candidate| candidate.url.end_with?("/548186") }

    refute_nil posting
    assert_equal [], posting.tags
    refute_includes posting.description, "募集職種:"
    assert_includes posting.description, "勤務地: 中野坂上（東京都）"
    assert_equal "〜650,000円／月", posting.reward
  end

  # --- 必須要素が欠けたカードは黙って除外する ---

  def test_parse_skips_card_without_project_name_link
    fragment = <<~HTML
      <li class="projectCard">
        <h3 class="projectNameWrapper"><span>リンクなし</span>の求人・案件</h3>
      </li>
    HTML
    postings = FreelanceJobs::Sources::LevtechCreator.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")

    assert_equal [], postings
  end

  # フッタの a.linkButton だけが残ったカード（案件名リンクが無い）も除外されること。
  def test_parse_skips_card_whose_only_link_is_the_footer_button
    fragment = <<~HTML
      <li class="projectCard">
        <div class="projectFooter">
          <a class="linkButton -medium" href="/project/detail/999999/">詳細を見る</a>
        </div>
      </li>
    HTML
    postings = FreelanceJobs::Sources::LevtechCreator.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")

    assert_equal [], postings
  end

  # 案件名リンクはあるが詳細URL形式でない（カテゴリページ等に化けた）場合も除外されること。
  def test_parse_skips_card_whose_link_is_not_a_job_detail_path
    fragment = <<~HTML
      <li class="projectCard">
        <h3 class="projectNameWrapper"><span><a class="projectName" href="/project/occ-10/">職種一覧</a>の求人・案件</span></h3>
      </li>
    HTML
    postings = FreelanceJobs::Sources::LevtechCreator.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")

    assert_equal [], postings
  end

  # 単価が取れないカードでも nil を入れず "要確認" で埋めること。
  def test_reward_falls_back_to_placeholder_when_price_is_missing
    fragment = <<~HTML
      <li class="projectCard">
        <h3 class="projectNameWrapper"><span><a class="projectName" href="/project/detail/123456/">単価非公開案件</a>の求人・案件</span></h3>
        <ul class="summaryArea">
          <li class="summaryList"><p class="summaryText">業務委託 （フリーランス）</p></li>
          <li class="summaryList"><p class="summaryText">渋谷（東京都）</p></li>
        </ul>
      </li>
    HTML
    posting = FreelanceJobs::Sources::LevtechCreator.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    refute_nil posting
    assert_equal "要確認", posting.reward
    assert_equal "業務委託（フリーランス）", posting.work_format
    assert_equal "勤務地: 渋谷（東京都）", posting.description
  end

  # --- fetch: search_targetsの数だけ取得し、URL重複を排除する ---

  # keywordを問わず常に同じbodyを返すFakeフェッチャー（呼び出されたURLを記録する）。
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

  def test_fetch_requests_each_search_target_and_deduplicates_urls_across_keywords
    fetcher = RecordingFetcher.new(body: read_fixture(REACT_FIXTURE))
    search_targets = [
      { keyword: "Ruby", hint: "Ruby" },
      { keyword: "TypeScript", hint: "TypeScript" },
      { keyword: "React", hint: "React" }
    ]
    source = FreelanceJobs::Sources::LevtechCreator.new(fetcher: fetcher, today: TODAY, search_targets: search_targets)

    postings = source.fetch

    assert_equal 3, fetcher.requested_urls.size, "search_targetsの数だけ取得するはず"
    assert_equal(
      %w[
        https://creator.levtech.jp/project/search/?keyword=Ruby
        https://creator.levtech.jp/project/search/?keyword=TypeScript
        https://creator.levtech.jp/project/search/?keyword=React
      ],
      fetcher.requested_urls
    )
    assert_equal 20, postings.size, "同じURLの案件が3キーワード分返っても重複排除され20件のままのはず"
    assert_equal "Ruby", postings.first.category_hint, "重複時は最初に見つけたキーワードのhintを保つはず"
  end

  def test_default_search_targets_cover_three_keywords
    keywords = FreelanceJobs::Sources::LevtechCreator::DEFAULT_SEARCH_TARGETS.map { |target| target[:keyword] }

    assert_equal %w[Ruby TypeScript React], keywords
  end

  # --- Profile::ENGINEER にLevtechCreatorが含まれる（BEGINNERには含まれない） ---

  def test_engineer_profile_includes_levtech_creator_source
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_includes source_classes, FreelanceJobs::Sources::LevtechCreator
  end

  def test_beginner_profile_does_not_include_levtech_creator_source
    source_classes = FreelanceJobs::Profile::BEGINNER.source_specs.map(&:first)

    refute_includes source_classes, FreelanceJobs::Sources::LevtechCreator
  end
end
