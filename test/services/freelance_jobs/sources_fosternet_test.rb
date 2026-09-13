# frozen_string_literal: true
# test/services/freelance_jobs/sources_fosternet_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesFosternetTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 12)
  FIXTURE_NAME = "fosternet_ruby.html"

  # fixture（/projects/list/?search_word=Ruby の実HTML）は1ページ20件。
  def parse_fixture(category_hint: "Ruby")
    FreelanceJobs::Sources::Fosternet.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  # 合成カード1件分のHTML片。fixtureに含まれない例外ケース（勤務地が空・単価/時・リンク欠落）の検証用。
  # 実DOMと同じく .c-jobitem 直下に title / tags / side / body の4つのdivを並べる。
  def build_fosternet_card_html(href: "/projects/detail/J99999", title: "テスト案件",
                                unit_price_label: "単価/月", unit_price_text: "85～95万円",
                                work_location: "東京都,港区", tags: ["NEW", "フルリモート"],
                                body: "本文です。")
    title_link_html = href ? %(<a href="#{href}">#{title}</a>) : title
    tag_links_html = tags.map { |tag| %(<a href="/projects/list/" class="c-search-tag">#{tag}</a>) }.join

    <<~HTML
      <div class="c-jobitem is-8line job_box1">
        <div class="c-jobitem__title"><h2>#{title_link_html}</h2></div>
        <div class="c-jobitem__tags">#{tag_links_html}</div>
        <div class="c-jobitem__side">
          <dl><dt>#{unit_price_label}</dt><dd>#{unit_price_text}</dd></dl>
          <dl><dt>勤務地</dt><dd>#{work_location}</dd></dl>
        </div>
        <div class="c-jobitem__body">#{body}</div>
      </div>
    HTML
  end

  # 一覧コンテナ .l-joblist で包む（パーサーは .l-joblist の中のカードだけを見る）。
  def wrap_joblist(card_html)
    wrap_html(%(<div class="l-joblist">#{card_html}</div>))
  end

  def parse_card(**card_attributes)
    FreelanceJobs::Sources::Fosternet.parse(
      wrap_joblist(build_fosternet_card_html(**card_attributes)), today: TODAY, category_hint: "Ruby"
    )
  end

  # --- fixtureの件数 ---

  def test_parse_fixture_returns_twenty_postings
    assert_equal 20, parse_fixture.size
  end

  # --- 1件目の全フィールド（J41141） ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "フォスターフリーランス", first.site
    assert_equal "https://freelance.fosternet.jp/projects/detail/J41141", first.url
    assert_equal "【フルリモート・フレックス/フルスタック】駐車場契約・管理サービス設計～テスト/プロダクトエンジニア", first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "650,000〜750,000円／月", first.reward
    assert_equal "月額制（業務委託）", first.work_format
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_equal ["NEW", "フルリモート"], first.tags
    assert_equal "", first.client
    assert_nil first.posted_on
    # 【技術環境】見出しの配下は、ラベルがSKILL_LABEL_REの語彙に無い行
    # （"・ダッシュボード：Redash" "・チャット：Slack"）も技術名として拾う。
    assert_equal ["Python", "Django", "TypeScript", "React", "Next.js", "Angular", "AWS", "GCP",
                  "PostgreSQL", "Aurora", "Redis", "Nginx", "CloudWatch", "Datadog", "Redash",
                  "GitHub Actions", "AWS CodeDeploy", "Jira", "Jira Product Discovery",
                  "Playwright", "Slack", "Confluence", "Copilot", "Cursor", "Claude Code"], first.skills
    assert first.description.start_with?("勤務地: 東京都,中央区 / 働き方: NEW・フルリモート / 【案件概要】"),
           "descriptionの先頭に勤務地と働き方（タグ）が付くはず: #{first.description[0, 80]}"
    assert_includes first.description, "・バックエンド：Python（Django）"
  end

  # --- 単価は「万円」表記を円に展開する（高単価判定に数値を拾わせるため） ---

  def test_reward_is_expanded_from_man_unit_so_that_classifier_reads_the_amount
    ruby_posting = parse_fixture.find { |posting| posting.url.end_with?("J41086") }

    refute_nil ruby_posting
    assert_equal "850,000〜950,000円／月", ruby_posting.reward
    assert_equal 850_000, FreelanceJobs::Classifier.first_reward_amount(ruby_posting.reward),
                 "生の「85～95万円」のままだと85と読まれ、高単価判定が効かなくなる"
  end

  def test_reward_keeps_thousands_separator_for_seven_digit_amounts
    posting = parse_card(unit_price_text: "100～110万円").first

    assert_equal "1,000,000〜1,100,000円／月", posting.reward
  end

  def test_reward_is_unconfirmed_when_unit_price_has_no_number
    posting = parse_card(unit_price_text: "応相談").first

    assert_equal "要確認", posting.reward
  end

  # --- 単価dtのラベルで work_format が分岐する ---

  def test_work_format_is_monthly_for_every_fixture_posting
    postings = parse_fixture

    assert(postings.all? { |posting| posting.work_format == "月額制（業務委託）" },
           "fixtureの20件はすべて dt が「単価/月」のため月額制になるはず")
  end

  def test_work_format_is_hourly_when_unit_price_label_is_per_hour
    posting = parse_card(unit_price_label: "単価/時", unit_price_text: "6000円").first

    assert_equal "時間単価制", posting.work_format
    assert_equal "6,000円／時", posting.reward
  end

  def test_work_format_falls_back_when_side_definitions_are_missing
    fragment = <<~HTML
      <div class="c-jobitem">
        <div class="c-jobitem__title"><h2><a href="/projects/detail/J10001">単価欄なし案件</a></h2></div>
        <div class="c-jobitem__body">本文です。</div>
      </div>
    HTML
    posting = FreelanceJobs::Sources::Fosternet.parse(wrap_joblist(fragment), today: TODAY, category_hint: "Ruby").first

    assert_equal "業務委託（フリーランス）", posting.work_format
    assert_equal "要確認", posting.reward
  end

  # --- URL正規化 ---

  def test_urls_are_normalized_absolute_without_trailing_slash_or_query
    parse_fixture.each do |posting|
      assert_match %r{\Ahttps://freelance\.fosternet\.jp/projects/detail/[A-Za-z0-9]+\z}, posting.url,
                   "末尾スラッシュなし・クエリなしの正規化された絶対URLのはず"
    end
  end

  # --- category_hint が引数どおり全件に伝わる ---

  def test_category_hint_is_propagated_to_every_posting
    postings = parse_fixture(category_hint: "TypeScript")

    assert_equal 20, postings.size
    assert(postings.all? { |posting| posting.category_hint == "TypeScript" },
           "全件のcategory_hintが引数のTypeScriptになるはず")
  end

  # --- description: 勤務地が空でも壊れない（実在するケース） ---

  def test_description_omits_work_location_when_it_is_blank
    posting = parse_card(work_location: "", body: "本文です。").first

    assert posting.description.start_with?("働き方: NEW・フルリモート / 本文です。"),
           "勤務地ddが空なら勤務地は入れないはず: #{posting.description}"
  end

  def test_description_includes_remote_tag_so_that_classifier_can_score_it
    posting = parse_card(tags: ["フルリモート"], body: "Ruby on Railsの開発案件です。").first

    assert_includes posting.description, "フルリモート"
    assert_match FreelanceJobs::EngineerClassifier::REMOTE_RE, posting.description,
                 "tagsは分類器が見ないため、descriptionに混ぜてリモート加点を効かせる"
  end

  # --- skills: 本文の「ラベル：値」行から技術名を取り出す ---

  def test_skills_are_extracted_from_labelled_lines_in_body
    posting = parse_card(body: "【技術環境】\n・バックエンド：Python（Django）\n・インフラ：AWS (ECS, EC2, RDS/Aurora)\n・DB：PostgreSQLなど\n").first

    assert_equal ["Python", "Django", "AWS", "ECS", "EC2", "RDS", "Aurora", "PostgreSQL"], posting.skills
  end

  def test_skills_are_empty_when_body_has_no_labelled_technology_lines
    posting = parse_card(body: "自社サービスの開発をご支援いただきます。").first

    assert_equal [], posting.skills
  end

  # --- skills: 技術欄の見出しにぶら下がる行も拾う（実データで最も多い形式） ---

  def test_skills_are_extracted_from_lines_under_a_technology_heading
    posting = parse_card(body: <<~BODY).first
      <環境>
      言語：JavaScript、Ruby
      Ruby on Rails、React.js
      Github、GitHub Actions/Jenkins

      <勤務時間>
      9:30～18:30
    BODY

    assert_equal ["JavaScript", "Ruby", "Ruby on Rails", "React.js", "Github", "GitHub Actions", "Jenkins"],
                 posting.skills
  end

  def test_skills_are_extracted_from_a_heading_line_that_carries_its_value_inline
    posting = parse_card(body: "【 環境 】Ruby(RoR)、RDB、NoSQL、AWS/GCP、SQL").first

    assert_equal ["Ruby", "RoR", "RDB", "NoSQL", "AWS", "GCP", "SQL"], posting.skills
  end

  # 技術欄の中の "役割: 技術名" 行は右側だけを技術名として採る。
  def test_skills_drop_role_labels_on_the_left_of_a_colon
    posting = parse_card(body: <<~BODY).first
      【技術環境】
      　　‐Backend: Kotlin
      　　‐Frontend: TypeScript
      　　‐DB: PostgreSQL
    BODY

    assert_equal ["Kotlin", "TypeScript", "PostgreSQL"], posting.skills
  end

  # 技術欄以外の見出し配下は拾わない（散文から日付や稼働日数を技術名として誤収集しない）。
  def test_skills_ignore_lines_under_a_non_technology_heading
    posting = parse_card(body: <<~BODY).first
      【稼働日数】
      週4日～週5日※平日の日中稼働必須
      【勤務時間】
      10:00～19:00
    BODY

    assert_equal [], posting.skills
  end

  # 技術欄は空行で閉じる。閉じないと後続の散文から誤ったトークンを拾ってしまう。
  def test_technology_section_is_closed_by_a_blank_line
    posting = parse_card(body: <<~BODY).first
      【開発環境】
      Ruby on Rails、MySQL

      MacBook Pro支給、Windows端末の持ち込みも可
    BODY

    assert_equal ["Ruby on Rails", "MySQL"], posting.skills
  end

  # --- 必須要素が欠けたカードは黙って除外する ---

  def test_parse_skips_cards_without_a_job_link
    postings = parse_card(href: nil)

    assert_equal [], postings
  end

  def test_parse_skips_cards_whose_link_is_not_a_job_detail_path
    postings = parse_card(href: "/projects/list/")

    assert_equal [], postings
  end

  def test_parse_skips_cards_with_an_empty_title
    postings = parse_card(title: "")

    assert_equal [], postings
  end

  # .l-joblist の外にあるカード（将来のおすすめ枠など）は案件として扱わない。
  def test_parse_ignores_cards_outside_the_job_list_container
    postings = FreelanceJobs::Sources::Fosternet.parse(
      wrap_html(build_fosternet_card_html), today: TODAY, category_hint: "Ruby"
    )

    assert_equal [], postings
  end

  # --- application_status: 本文末尾の募集終了告知を反映する ---

  def test_application_status_is_unknown_when_the_body_has_no_closing_notice
    posting = parse_card(body: "自社サービスの開発をご支援いただきます。").first

    assert_equal "-", posting.application_status, "「募集中」とはどこにも書かれていないため断定しない"
  end

  def test_application_status_is_closed_when_the_body_carries_the_closing_notice
    posting = parse_card(body: "自社サービスの開発をご支援いただきます。\n※※こちらの案件は現在募集を終了しております※※").first

    assert_equal "募集終了", posting.application_status
  end

  # parse はページの写しに徹する（募集終了も返す）。採否は fetch の include_closed が決める。
  def test_parse_keeps_closed_postings
    postings = parse_card(body: "※※こちらの案件は現在募集を終了しております※※")

    assert_equal 1, postings.size
  end

  # --- fetch: キーワード×ページ数だけ取得し、URL重複を排除する ---

  # どのURLでも同じbodyを返すFakeフェッチャー（呼び出されたURLを記録する）。
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

  def build_source(search_targets:, page_count:)
    FreelanceJobs::Sources::Fosternet.new(
      fetcher: RecordingFetcher.new(body: read_fixture(FIXTURE_NAME)),
      today: TODAY,
      search_targets: search_targets,
      page_count: page_count
    )
  end

  def test_fetch_requests_every_keyword_and_page_and_deduplicates_urls
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    search_targets = [
      { keyword: "Ruby", hint: "Ruby" },
      { keyword: "Ruby on Rails", hint: "Ruby" }
    ]
    source = FreelanceJobs::Sources::Fosternet.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, page_count: 2, include_closed: true
    )

    postings = source.fetch

    assert_equal [
      "https://freelance.fosternet.jp/projects/list/?search_word=Ruby",
      "https://freelance.fosternet.jp/projects/list/page:2?search_word=Ruby",
      "https://freelance.fosternet.jp/projects/list/?search_word=Ruby+on+Rails",
      "https://freelance.fosternet.jp/projects/list/page:2?search_word=Ruby+on+Rails"
    ], fetcher.requested_urls, "1頁目はクエリのみ・2頁目は page:2 の直後に「?」を続ける形"
    assert_equal 20, postings.size, "4リクエストとも同じ20件が返るがURLキーで重複排除されるはず"
  end

  # fixture（実HTML1ページ）の20件のうち6件は本文に募集終了告知を持つ。
  # 既定のfetchはその6件を落とすので、シートに載るのは14件になる。
  def test_fetch_drops_the_closed_postings_contained_in_the_fixture
    source = build_source(search_targets: [{ keyword: "Ruby", hint: "Ruby" }], page_count: 1)

    postings = source.fetch

    assert_equal 14, postings.size
    assert_equal 6, parse_fixture.count { |posting| posting.application_status == "募集終了" }
    assert(postings.none? { |posting| posting.application_status == "募集終了" })
  end

  def test_fetch_requests_only_the_first_page_when_page_count_is_one
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    source = FreelanceJobs::Sources::Fosternet.new(
      fetcher: fetcher, today: TODAY,
      search_targets: [{ keyword: "React", hint: "React" }], page_count: 1
    )

    source.fetch

    assert_equal ["https://freelance.fosternet.jp/projects/list/?search_word=React"], fetcher.requested_urls
  end

  # 募集終了の案件は既定ではシートに載せない（応募できない案件で埋まるのを避ける）。
  def build_open_and_closed_list_html
    wrap_joblist(
      build_fosternet_card_html(href: "/projects/detail/J10001", title: "募集中の案件", body: "Ruby on Railsの開発案件です。") +
      build_fosternet_card_html(href: "/projects/detail/J10002", title: "終了した案件",
                                body: "Ruby on Railsの開発案件です。\n※※こちらの案件は現在募集を終了しております※※")
    )
  end

  def fetch_with_closed_notice(include_closed:)
    FreelanceJobs::Sources::Fosternet.new(
      fetcher: RecordingFetcher.new(body: build_open_and_closed_list_html), today: TODAY,
      search_targets: [{ keyword: "Ruby", hint: "Ruby" }], page_count: 1, include_closed: include_closed
    ).fetch
  end

  def test_fetch_drops_closed_postings_by_default
    postings = fetch_with_closed_notice(include_closed: false)

    assert_equal ["募集中の案件"], postings.map(&:title)
  end

  def test_fetch_keeps_closed_postings_when_include_closed_is_true
    postings = fetch_with_closed_notice(include_closed: true)

    assert_equal ["募集中の案件", "終了した案件"], postings.map(&:title)
    assert_equal ["-", "募集終了"], postings.map(&:application_status)
  end

  def test_default_search_targets_cover_three_keywords_and_two_pages
    assert_equal ["Ruby", "TypeScript", "React"],
                 FreelanceJobs::Sources::Fosternet::DEFAULT_SEARCH_TARGETS.map { |target| target[:keyword] }
    assert_equal 2, FreelanceJobs::Sources::Fosternet::DEFAULT_PAGE_COUNT,
                 "3キーワード×2頁=6リクエストの想定"
  end

  # --- 実データが分類器を通ること（一覧のみの設計で分類不能が出ない） ---

  def test_every_fixture_posting_is_classified_by_engineer_classifier
    unclassified = parse_fixture.reject do |posting|
      FreelanceJobs::EngineerClassifier.classify(posting, today: TODAY).category
    end

    assert_equal [], unclassified.map(&:title), "一覧の情報だけで全件が分類できるはず"
  end

  # --- Profile::ENGINEER にFosternetが含まれる（BEGINNERには含まれない） ---

  def test_engineer_profile_includes_fosternet_source
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_includes source_classes, FreelanceJobs::Sources::Fosternet
  end

  def test_beginner_profile_does_not_include_fosternet_source
    source_classes = FreelanceJobs::Profile::BEGINNER.source_specs.map(&:first)

    refute_includes source_classes, FreelanceJobs::Sources::Fosternet
  end
end
