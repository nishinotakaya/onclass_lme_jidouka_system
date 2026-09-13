# frozen_string_literal: true
# test/services/freelance_jobs/sources_shuuumatu_worker_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesShuuumatuWorkerTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 13)
  DETAIL_FIXTURE = "shuuumatu_worker_detail.html"               # /projects/18881 募集中
  DETAIL_CLOSED_FIXTURE = "shuuumatu_worker_detail_closed.html" # /projects/17600 募集終了
  LIST_TYPESCRIPT_FIXTURE = "shuuumatu_worker_typescript.html"  # 12カード（募集中4・募集終了8）・次頁あり
  LIST_RUBY_FIXTURE = "shuuumatu_worker_ruby.html"              # 8カード（全て募集終了）・ページャ無し
  LIST_EMPTY_FIXTURE = "shuuumatu_worker_empty.html"            # 範囲外ページ（カード0件）

  BASE = "https://shuuumatu-worker.jp"
  SOURCE_CLASS = FreelanceJobs::Sources::ShuuumatuWorker

  def parse_detail(fixture_name = DETAIL_FIXTURE, category_hint: "TypeScript")
    SOURCE_CLASS.parse(read_fixture(fixture_name), today: TODAY, category_hint: category_hint)
  end

  # --- 詳細ページのパース: 1ページ=1件 ---

  def test_parse_detail_returns_exactly_one_posting
    assert_equal 1, parse_detail.size
  end

  # 詳細ページ下部の関連案件カード（div.project-list-item ×3）が案件として混入しないこと。
  def test_parse_detail_ignores_related_project_cards
    postings = parse_detail

    assert_equal ["#{BASE}/projects/18881"], postings.map(&:url)
  end

  def test_parse_detail_has_expected_fields
    posting = parse_detail.first

    assert_equal "シューマツワーカー", posting.site
    assert_equal "#{BASE}/projects/18881", posting.url
    assert_equal "Webアプリのフルスタック開発（Next.js×TypeScript）｜Google Cloudデータ基盤 × LLM/生成AI API組み込み",
                 posting.title
    assert_equal "TypeScript", posting.category_hint
    assert_equal "420,000円／月", posting.reward
    assert_equal "月額制（業務委託）", posting.work_format
    assert_equal "募集中", posting.application_status
    assert_equal "-", posting.deadline_text
    assert_nil posting.deadline_on
    assert_equal ["Next.js", "Github", "BigQuery", "TypeScript", "Firebase", "React"], posting.skills
    assert_equal "Wello株式会社", posting.client
    assert_equal ["募集中", "フルリモート", "バックエンドエンジニア/フロントエンドエンジニア"], posting.tags
    assert_equal Date.new(2026, 9, 1), posting.posted_on
  end

  # descriptionは「案件内容 / 必要条件 / 歓迎条件 / 募集職種 / 使用技術 / 稼働時間 / 働き方」の順に連結する。
  # 技術名・リモート可否が本文に出ることでEngineerClassifierの判定が効く。
  def test_parse_detail_description_joins_sections_in_expected_order
    description = parse_detail.first.description

    assert_start_with "案件内容: DX・AIソリューション支援および自社SaaS開発を展開されている企業様からの募集です。 " \
                      "受託案件の増加および自社プロダクトの機能拡充に伴い、", description
    assert_includes description, " / 必要条件: -TypeScript / Reactを用いたWebアプリケーション開発の実務経験（目安3年以上） "
    assert_includes description, " / 歓迎条件: -Google Cloud（Cloud Run・BigQuery・Firebase）の利用・構築経験 "
    assert_end_with " / 募集職種: バックエンドエンジニア/フロントエンドエンジニア" \
                    " / 使用技術: Next.js / Github / BigQuery / TypeScript / Firebase / React" \
                    " / 稼働時間: 120h/月（※30h/週程度） ※MTG以外の稼働時間は平日夜間・土日中心など柔軟に対応可能" \
                    " / 働き方: フルリモート", description
  end

  # 本文は "\r<br>" 区切り。<br>を空白へ置換せずに.textすると行が連結して読めなくなる。
  def test_detail_section_line_breaks_become_spaces
    description = parse_detail.first.description

    assert_includes description, "-Next.js（App Router）での開発経験 -GitHubを用いた"
    refute_includes description, "開発経験-GitHub"
  end

  # 職種は sp 版（"バックエンドエンジニア/フロントエ..."）ではなく pc 版のフル文字列を読む。
  def test_parse_detail_reads_full_job_type_not_truncated_sp_version
    posting = parse_detail.first

    refute_includes posting.description, "フロントエ..."
    assert_includes posting.description, "募集職種: バックエンドエンジニア/フロントエンドエンジニア"
  end

  # --- 募集終了ページ: application_status "公開終了" としてそのまま返す（採否は fetch 側） ---

  def test_parse_closed_detail_returns_expired_posting
    posting = parse_detail(DETAIL_CLOSED_FIXTURE, category_hint: "Ruby").first

    assert_equal "#{BASE}/projects/17600", posting.url
    assert_equal "バックエンドエンジニア募集！Ruby on Railsを活用した受託開発でスキルアップ！", posting.title
    assert_equal "公開終了", posting.application_status
    assert_equal "112,500〜150,000円／月", posting.reward
    assert_equal "月額制（業務委託）", posting.work_format
    assert_equal ["GCP", "Ruby on Rails", "AWS", "Ruby"], posting.skills
    assert_equal "個人事業主", posting.client
    assert_equal ["公開終了", "フルリモート", "バックエンドエンジニア"], posting.tags
    assert_equal Date.new(2025, 12, 16), posting.posted_on
  end

  # --- URL正規化 ---

  def test_urls_are_normalized_absolute_without_trailing_slash_or_query
    [DETAIL_FIXTURE, DETAIL_CLOSED_FIXTURE].each do |fixture_name|
      posting = parse_detail(fixture_name).first

      assert_match %r{\Ahttps://shuuumatu-worker\.jp/projects/\d+\z}, posting.url,
                   "末尾スラッシュなし・クエリなしの正規化された絶対URLのはず"
    end
  end

  # og:url が無いページは fetch が渡す取得URLを使い、クエリ付きでも正規化する。
  def test_parse_falls_back_to_detail_url_when_og_url_is_missing
    posting = SOURCE_CLASS.parse(
      wrap_html(build_detail_html(title: "og:urlなし")),
      today: TODAY, category_hint: "Ruby", detail_url: "#{BASE}/projects/99?ref=list"
    ).first

    assert_equal "#{BASE}/projects/99", posting.url
  end

  # --- category_hint が引数どおり伝わる ---

  def test_category_hint_is_propagated
    assert_equal "React", parse_detail(category_hint: "React").first.category_hint
    assert_nil parse_detail(category_hint: nil).first.category_hint
  end

  # --- 報酬の自由記述からの単価組み立て ---

  def test_build_reward_takes_range_before_single_amount
    assert_equal "112,500〜150,000円／月",
                 SOURCE_CLASS.build_reward("112,500〜150,000円/月 ※目安となり、経験/スキルにより応相談(時間単価2,500円前後)")
    assert_equal "320,000〜400,000円／月",
                 SOURCE_CLASS.build_reward("-月額単価320,000〜400,000円 (ご経験による) -時間単価4,000〜5,000円/時程度")
  end

  # 先頭の "-" は箇条書き記号であり範囲の区切りではない。時給は補足として後ろに出るので月額を採る。
  def test_build_reward_ignores_leading_bullet_and_hourly_note
    assert_equal "420,000円／月",
                 SOURCE_CLASS.build_reward("-420,000円/月 (ご経験やスキルにより要相談) ※時間単価：4,500円/時程度")
  end

  def test_build_reward_marks_hourly_when_first_amount_is_per_hour
    assert_equal "4,500円／時", SOURCE_CLASS.build_reward("4,500円/時程度 ※税込")
    assert_equal "4,000〜5,000円／時", SOURCE_CLASS.build_reward("4,000〜5,000円／時")
  end

  def test_build_reward_falls_back_when_no_amount
    assert_equal "要確認", SOURCE_CLASS.build_reward("応相談")
    assert_equal "要確認", SOURCE_CLASS.build_reward(nil)
  end

  def test_work_format_follows_reward_unit
    hourly = SOURCE_CLASS.parse(
      wrap_html(build_detail_html(reward: "4,500円/時程度")), today: TODAY, detail_url: "#{BASE}/projects/1"
    ).first
    unknown = SOURCE_CLASS.parse(
      wrap_html(build_detail_html(reward: "応相談")), today: TODAY, detail_url: "#{BASE}/projects/2"
    ).first

    assert_equal "時間単価制", hourly.work_format
    assert_equal "4,500円／時", hourly.reward
    assert_equal "業務委託（フリーランス）", unknown.work_format
    assert_equal "要確認", unknown.reward
  end

  # --- 想定外の掲載日表記・CTA無しは "取れない" 扱いにする ---

  def test_posted_on_is_nil_when_date_is_unexpected
    posting = SOURCE_CLASS.parse(
      wrap_html(build_detail_html(published: "公開日: 2026.09.01")), today: TODAY, detail_url: "#{BASE}/projects/3"
    ).first

    assert_nil posting.posted_on
  end

  def test_application_status_is_dash_when_cta_is_missing
    posting = SOURCE_CLASS.parse(
      wrap_html(build_detail_html(cta: nil)), today: TODAY, detail_url: "#{BASE}/projects/4"
    ).first

    assert_equal "-", posting.application_status
    refute_includes posting.tags, "-"
  end

  def test_client_is_empty_when_json_ld_is_missing
    posting = SOURCE_CLASS.parse(
      wrap_html(build_detail_html(json_ld: nil)), today: TODAY, detail_url: "#{BASE}/projects/5"
    ).first

    assert_equal "", posting.client
  end

  # --- 必須要素（案件名・URL・詳細ルート）が欠けたページは黙って除外する ---

  def test_parse_returns_empty_when_detail_root_is_missing
    assert_equal [], SOURCE_CLASS.parse(wrap_html("<div>案件ページではない</div>"), today: TODAY)
  end

  def test_parse_skips_detail_without_title
    fragment = build_detail_html(title: nil)

    assert_equal [], SOURCE_CLASS.parse(wrap_html(fragment), today: TODAY, detail_url: "#{BASE}/projects/6")
  end

  def test_parse_skips_detail_without_any_url
    fragment = build_detail_html(title: "URLが取れない")

    assert_equal [], SOURCE_CLASS.parse(wrap_html(fragment), today: TODAY)
  end

  # --- 一覧ページのパース ---

  def test_parse_list_returns_cards_in_order_with_closed_flag
    cards = SOURCE_CLASS.parse_list(read_fixture(LIST_TYPESCRIPT_FIXTURE))

    assert_equal 12, cards.size
    assert_equal %w[/projects/18881 /projects/18856 /projects/18725 /projects/18528],
                 cards.reject(&:closed).map(&:path)
    assert_equal 8, cards.count(&:closed)
    assert_equal "/projects/18103", cards.find(&:closed).path
  end

  def test_parse_list_marks_all_ruby_cards_closed
    cards = SOURCE_CLASS.parse_list(read_fixture(LIST_RUBY_FIXTURE))

    assert_equal 8, cards.size
    assert cards.all?(&:closed)
  end

  def test_parse_list_returns_empty_for_out_of_range_page
    assert_equal [], SOURCE_CLASS.parse_list(read_fixture(LIST_EMPTY_FIXTURE))
  end

  # 一覧パースは .projects-main-body 配下に限定する（詳細ページの関連案件カードは拾わない）。
  def test_parse_list_ignores_related_cards_on_detail_page
    assert_equal [], SOURCE_CLASS.parse_list(read_fixture(DETAIL_FIXTURE))
  end

  def test_parse_list_skips_card_whose_href_is_not_a_project_path
    fragment = <<~HTML
      <div class="projects-main-body">
        <div class="project-list-item"><a class="project-list-item-link" href="/company/about">案件ではない</a></div>
        <div class="project-list-item"><p class="project-list-item-head__title">hrefなし</p></div>
        <div class="project-list-item"><a class="project-list-item-link" href="/projects/7">案件</a></div>
      </div>
    HTML

    assert_equal ["/projects/7"], SOURCE_CLASS.parse_list(wrap_html(fragment)).map(&:path)
  end

  def test_next_page_path_reads_pagination_link
    assert_equal "/projects?page=2&skills=typescript", SOURCE_CLASS.next_page_path(read_fixture(LIST_TYPESCRIPT_FIXTURE))
    assert_nil SOURCE_CLASS.next_page_path(read_fixture(LIST_RUBY_FIXTURE))
  end

  # --- fetch: 一覧→募集中の詳細だけ取得し、URL重複を排除する（Fakeフェッチャーで通信しない） ---

  # URLごとの応答を台本化するFakeフェッチャー（通信しない）。
  # 各URLの配列を先頭から1回ずつ消費し、例外なら送出・文字列ならbodyとして返す。
  # 台本に無いURL・使い切ったURLは fallback_body（既定: カード0件の一覧）を返す。
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

  TYPESCRIPT_LIST_URL = "#{BASE}/projects?skills=typescript"
  REACT_LIST_URL = "#{BASE}/projects?skills=react"
  RUBY_LIST_URL = "#{BASE}/projects?skills=ruby"
  TYPESCRIPT_OPEN_DETAIL_URLS = %w[18881 18856 18725 18528].map { |id| "#{BASE}/projects/#{id}" }.freeze

  # 一覧の募集中4件ぶんだけ詳細を取り、募集終了カード8件の詳細は取りに行かない。
  # 募集終了カードがあるページは次頁（/projects?page=2&skills=typescript）も取らない。
  def test_fetch_requests_only_open_cards_and_stops_paging_at_closed_card
    fetcher = ScriptedFetcher.new(
      { TYPESCRIPT_LIST_URL => [read_fixture(LIST_TYPESCRIPT_FIXTURE)] },
      fallback_body: read_fixture(DETAIL_FIXTURE)
    )
    postings = build_source(fetcher, [{ skill_slug: "typescript", category_hint: "TypeScript" }]).fetch

    assert_equal [TYPESCRIPT_LIST_URL, *TYPESCRIPT_OPEN_DETAIL_URLS], fetcher.requested_urls
    # 全詳細が同じフィクスチャ（/projects/18881）を返すので、URLキーで1件に畳まれる。
    assert_equal 1, postings.size
    assert_equal "#{BASE}/projects/18881", postings.first.url
    assert_equal "TypeScript", postings.first.category_hint
  end

  # 同じ案件が typescript と react の両方の一覧に出ても詳細は1回しか取らず、
  # category_hint は最初に見つけたスキルのものになる。
  def test_fetch_deduplicates_detail_paths_across_skills_and_keeps_first_hint
    fetcher = ScriptedFetcher.new(
      {
        TYPESCRIPT_LIST_URL => [read_fixture(LIST_TYPESCRIPT_FIXTURE)],
        REACT_LIST_URL => [read_fixture(LIST_TYPESCRIPT_FIXTURE)]
      },
      fallback_body: read_fixture(DETAIL_FIXTURE)
    )
    search_targets = [
      { skill_slug: "typescript", category_hint: "TypeScript" },
      { skill_slug: "react", category_hint: "React" }
    ]
    postings = build_source(fetcher, search_targets).fetch

    assert_equal [TYPESCRIPT_LIST_URL, REACT_LIST_URL, *TYPESCRIPT_OPEN_DETAIL_URLS], fetcher.requested_urls,
                 "一覧2回＋ユニークな募集中4件の詳細だけ取得するはず"
    assert_equal "TypeScript", postings.first.category_hint
  end

  # 全カード募集終了の一覧（ruby）は詳細を1件も取らず、ページャも無いので一覧1回で終わる。
  def test_fetch_requests_nothing_more_when_every_card_is_closed
    fetcher = ScriptedFetcher.new(
      { RUBY_LIST_URL => [read_fixture(LIST_RUBY_FIXTURE)] }, fallback_body: read_fixture(LIST_EMPTY_FIXTURE)
    )
    postings = build_source(fetcher, [{ skill_slug: "ruby", category_hint: "Ruby" }]).fetch

    assert_equal [RUBY_LIST_URL], fetcher.requested_urls
    assert_equal [], postings
  end

  # 募集終了カードが無いページは次頁リンクを辿り、範囲外ページ（カード0件）で打ち切る。
  def test_fetch_follows_next_page_until_empty_page_or_max_pages
    open_only_list = read_fixture(LIST_TYPESCRIPT_FIXTURE).gsub(" project-list-item--closed", "")
    fetcher = ScriptedFetcher.new(
      {
        TYPESCRIPT_LIST_URL => [open_only_list],
        "#{BASE}/projects?page=2&skills=typescript" => [read_fixture(LIST_EMPTY_FIXTURE)]
      },
      fallback_body: read_fixture(DETAIL_FIXTURE)
    )
    source = build_source(fetcher, [{ skill_slug: "typescript", category_hint: "TypeScript" }], max_pages: 3)
    source.fetch

    list_urls = fetcher.requested_urls.grep(/skills=/)
    assert_equal [TYPESCRIPT_LIST_URL, "#{BASE}/projects?page=2&skills=typescript"], list_urls,
                 "2頁目が0件なら3頁目は取得しないはず"
    assert_equal 12, fetcher.requested_urls.grep(%r{/projects/\d+\z}).size, "1頁目の12件は全て募集中扱いで詳細を取るはず"
  end

  def test_fetch_respects_max_pages_even_when_next_link_exists
    open_only_list = read_fixture(LIST_TYPESCRIPT_FIXTURE).gsub(" project-list-item--closed", "")
    fetcher = ScriptedFetcher.new(
      {
        TYPESCRIPT_LIST_URL => [open_only_list],
        "#{BASE}/projects?page=2&skills=typescript" => [open_only_list]
      },
      fallback_body: read_fixture(DETAIL_FIXTURE)
    )
    build_source(fetcher, [{ skill_slug: "typescript", category_hint: "TypeScript" }], max_pages: 2).fetch

    assert_equal 2, fetcher.requested_urls.grep(/skills=/).size, "max_pages=2 なら3頁目は取らないはず"
  end

  # 募集終了の詳細は既定では落とし、include_expired: true なら残す。
  def test_fetch_drops_expired_postings_unless_include_expired
    script = { TYPESCRIPT_LIST_URL => [read_fixture(LIST_TYPESCRIPT_FIXTURE)] }
    fetcher = ScriptedFetcher.new(script, fallback_body: read_fixture(DETAIL_CLOSED_FIXTURE))
    assert_equal [], build_source(fetcher, [{ skill_slug: "typescript", category_hint: "TypeScript" }]).fetch

    fetcher = ScriptedFetcher.new(
      { TYPESCRIPT_LIST_URL => [read_fixture(LIST_TYPESCRIPT_FIXTURE)] }, fallback_body: read_fixture(DETAIL_CLOSED_FIXTURE)
    )
    postings = build_source(fetcher, [{ skill_slug: "typescript", category_hint: "TypeScript" }], include_expired: true).fetch

    assert_equal ["公開終了"], postings.map(&:application_status)
  end

  # --- 詳細取得件数とリクエスト予算の上限 ---

  def test_fetch_caps_detail_requests_at_max_detail_requests
    fetcher = ScriptedFetcher.new(
      { TYPESCRIPT_LIST_URL => [read_fixture(LIST_TYPESCRIPT_FIXTURE)] }, fallback_body: read_fixture(DETAIL_FIXTURE)
    )
    build_source(fetcher, [{ skill_slug: "typescript", category_hint: "TypeScript" }], max_detail_requests: 2).fetch

    assert_equal [TYPESCRIPT_LIST_URL, *TYPESCRIPT_OPEN_DETAIL_URLS.first(2)], fetcher.requested_urls
  end

  # 予算は再取得ぶんも含めた物理的な上限。尽きたら取得を止め、取れたぶんだけ返す。
  def test_fetch_stops_at_request_budget_including_retries
    fetcher = ScriptedFetcher.new(
      {
        TYPESCRIPT_LIST_URL => [read_fixture(LIST_TYPESCRIPT_FIXTURE)],
        TYPESCRIPT_OPEN_DETAIL_URLS[0] => [server_error, read_fixture(DETAIL_FIXTURE)]
      },
      fallback_body: read_fixture(DETAIL_FIXTURE)
    )
    postings = silencing_fetch_logs do
      build_source(fetcher, [{ skill_slug: "typescript", category_hint: "TypeScript" }], request_budget: 4).fetch
    end

    assert_equal 4, fetcher.requested_urls.size, "一覧1＋失敗1＋再取得1＋次の詳細1で予算4を使い切るはず"
    assert_equal 1, postings.size
  end

  def test_default_targets_and_limits_fit_request_budget
    slugs = SOURCE_CLASS::DEFAULT_SEARCH_TARGETS.map { |target| target[:skill_slug] }

    assert_equal %w[ruby rubyonrails typescript react nextjs], slugs
    assert_operator slugs.size * SOURCE_CLASS::MAX_PAGES + SOURCE_CLASS::MAX_DETAIL_REQUESTS, :<=, 40
    assert_equal 40, SOURCE_CLASS::REQUEST_BUDGET
  end

  # --- 取得失敗への耐性（1回リトライ→その単位だけ打ち切り→全滅時のみ例外） ---

  def test_fetch_retries_once_when_list_returns_server_error
    fetcher = ScriptedFetcher.new(
      { TYPESCRIPT_LIST_URL => [server_error, read_fixture(LIST_TYPESCRIPT_FIXTURE)] },
      fallback_body: read_fixture(DETAIL_FIXTURE)
    )
    postings = silencing_fetch_logs do
      build_source(fetcher, [{ skill_slug: "typescript", category_hint: "TypeScript" }]).fetch
    end

    assert_equal 1, postings.size, "1回目が500でも再取得すれば取得できるはず"
    assert_equal [TYPESCRIPT_LIST_URL, TYPESCRIPT_LIST_URL, *TYPESCRIPT_OPEN_DETAIL_URLS], fetcher.requested_urls
  end

  # HTTPエラーだけでなく通信層の切断も同じく「その詳細だけ打ち切り」で済ませ、他の詳細は取る。
  def test_fetch_skips_failing_detail_and_keeps_others
    connection_error = Errno::ECONNRESET.new("Connection reset by peer")
    fetcher = ScriptedFetcher.new(
      {
        TYPESCRIPT_LIST_URL => [read_fixture(LIST_TYPESCRIPT_FIXTURE)],
        TYPESCRIPT_OPEN_DETAIL_URLS[0] => [connection_error, connection_error]
      },
      fallback_body: read_fixture(DETAIL_FIXTURE)
    )
    postings = silencing_fetch_logs do
      build_source(fetcher, [{ skill_slug: "typescript", category_hint: "TypeScript" }]).fetch
    end

    assert_equal 1, postings.size, "1件目の詳細が落ちても残りの詳細は返すはず"
    assert_equal 2, fetcher.requested_urls.count(TYPESCRIPT_OPEN_DETAIL_URLS[0]), "落ちた詳細は1回だけ取り直すはず"
  end

  def test_fetch_skips_failing_skill_and_keeps_other_skills
    fetcher = ScriptedFetcher.new(
      {
        RUBY_LIST_URL => [server_error, server_error],
        TYPESCRIPT_LIST_URL => [read_fixture(LIST_TYPESCRIPT_FIXTURE)]
      },
      fallback_body: read_fixture(DETAIL_FIXTURE)
    )
    search_targets = [
      { skill_slug: "ruby", category_hint: "Ruby" },
      { skill_slug: "typescript", category_hint: "TypeScript" }
    ]
    postings = silencing_fetch_logs { build_source(fetcher, search_targets).fetch }

    assert_equal 1, postings.size, "rubyが落ちてもtypescriptの結果は返すはず"
  end

  # 全滅を黙って0件で返すとサイト構造の崩れに気付けないため、最初の失敗を送出する。
  def test_fetch_raises_when_nothing_succeeds
    error = server_error
    fetcher = ScriptedFetcher.new({ TYPESCRIPT_LIST_URL => [error, error] }, fallback_body: read_fixture(LIST_EMPTY_FIXTURE))

    raised = assert_raises(FreelanceJobs::FetchError) do
      silencing_fetch_logs { build_source(fetcher, [{ skill_slug: "typescript", category_hint: "TypeScript" }]).fetch }
    end

    assert_equal error.message, raised.message
  end

  # WAFのアクセス制限は取り直しても解消しないので、再取得せずそのまま送出する。
  def test_fetch_does_not_retry_when_access_is_blocked
    fetcher = ScriptedFetcher.new(
      { TYPESCRIPT_LIST_URL => [FreelanceJobs::AccessBlockedError.new("アクセス制限（WAF captcha）")] },
      fallback_body: read_fixture(LIST_EMPTY_FIXTURE)
    )

    assert_raises(FreelanceJobs::AccessBlockedError) do
      silencing_fetch_logs { build_source(fetcher, [{ skill_slug: "typescript", category_hint: "TypeScript" }]).fetch }
    end
    assert_equal 1, fetcher.requested_urls.size, "アクセス制限では再取得しないはず"
  end

  # --- Profile::ENGINEER に本クラスが含まれる（BEGINNERには含まれない） ---

  def test_engineer_profile_includes_shuuumatu_worker_source
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_includes source_classes, SOURCE_CLASS
  end

  def test_beginner_profile_does_not_include_shuuumatu_worker_source
    source_classes = FreelanceJobs::Profile::BEGINNER.source_specs.map(&:first)

    refute_includes source_classes, SOURCE_CLASS
  end

  private

  def build_source(fetcher, search_targets, **options)
    SOURCE_CLASS.new(fetcher: fetcher, today: TODAY, search_targets: search_targets, **options)
  end

  def server_error
    FreelanceJobs::FetchError.new("HTTP 500 #{TYPESCRIPT_LIST_URL}")
  end

  def assert_start_with(expected_prefix, actual)
    assert actual.start_with?(expected_prefix), "先頭が #{expected_prefix.inspect} のはず:\n#{actual[0, 200]}"
  end

  def assert_end_with(expected_suffix, actual)
    assert actual.end_with?(expected_suffix), "末尾が #{expected_suffix.inspect} のはず:\n#{actual[-400..]}"
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

  # シューマツワーカーの詳細ページ1件分のHTML片（実データのDOM構造を模したもの）。
  # og:url は付けない（url は parse の detail_url から取る）。nil を渡した要素は出力しない。
  def build_detail_html(title: "テスト案件", reward: "-300,000円/月 ※税込", published: "公開日: 2026年09月10日",
                        cta: "エントリーしてみる", json_ld: '{"@type":"JobPosting","hiringOrganization":{"@type":"Organization","name":"テスト株式会社"}}')
    title_html = title ? %(<h1 class="project-main-head__title">#{title}</h1>) : ""
    cta_html = cta ? %(<div class="project-main-body-cta-top"><a class="entry-btn" href="#">#{cta}</a></div>) : ""
    json_ld_html = json_ld ? %(<script type="application/ld+json">#{json_ld}</script>) : ""

    <<~HTML
      #{json_ld_html}
      <div class="project-main">
        <div class="project-main-head">
          #{title_html}
          <div class="project-main-head__jobtype pc">バックエンドエンジニア</div>
          <div class="project-main-head__jobtype sp">バックエンド...</div>
          <p class="project-main-head-published">#{published}</p>
        </div>
        #{cta_html}
        <div class="project-main-body-condition-item">
          <p class="project-main-body-condition-item__label">報酬</p>
          <div class="project-main-body-condition-item__text">#{reward}</div>
        </div>
        <div class="project-main-body-condition-item">
          <p class="project-main-body-condition-item__label">働き方</p>
          <div class="project-main-body-condition-item__text">-フルリモート</div>
        </div>
        <div class="project-main-body-condition-item">
          <p class="project-main-body-condition-item__label">関連スキル</p>
          <div class="project-main-body-condition-item__text">
            <div class="project-main-body-condition-item-skill-list">
              <a class="project-main-body-condition-item-skill-list__item" href="/projects?skills=ruby">Ruby</a>
            </div>
          </div>
        </div>
        <div class="project-main-body-detail">
          <p class="project-main-body-detail__label">案件内容</p>
          <p class="project-main-body-detail__text">Railsアプリの開発<br>API実装</p>
        </div>
      </div>
    HTML
  end
end
