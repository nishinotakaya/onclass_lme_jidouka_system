# frozen_string_literal: true
# test/services/freelance_jobs/sources_relance_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesRelanceTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 13)
  RUBY_FIXTURE = "relance_ruby.html"
  TYPESCRIPT_FIXTURE = "relance_typescript.html"
  TYPESCRIPT_PAGE2_FIXTURE = "relance_typescript_page2.html"
  REACT_FIXTURE = "relance_react.html"

  LIST_URL_PREFIX = "https://relance.jp/project"

  def parse_fixture(name = RUBY_FIXTURE, category_hint: "Ruby")
    FreelanceJobs::Sources::Relance.parse(read_fixture(name), today: TODAY, category_hint: category_hint)
  end

  # ruby 一覧は8件（1ページ・ページングなし）。
  def test_parse_ruby_fixture_returns_eight_postings
    assert_equal 8, parse_fixture.size
  end

  def test_parse_typescript_fixtures_return_ten_and_three_postings
    assert_equal 10, parse_fixture(TYPESCRIPT_FIXTURE, category_hint: "TypeScript").size
    assert_equal 3, parse_fixture(TYPESCRIPT_PAGE2_FIXTURE, category_hint: "TypeScript").size
    assert_equal 7, parse_fixture(REACT_FIXTURE, category_hint: "React").size
  end

  # --- 1件目の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "Relance", first.site
    assert_equal "https://relance.jp/project/ruby/jd02664", first.url
    assert_equal "受託開発・Webシステム構築を手がける成長企業にて、要件整理から実装まで幅広く担うエンジニアを募集！", first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "630,000〜780,000円／月", first.reward
    assert_equal "月額制（業務委託）", first.work_format
    assert_equal ["Ruby", "Ruby on Rails", "Vue.js", "Next.js"], first.skills
    assert_equal ["NEW", "ハイブリッド"], first.tags
    assert_equal "", first.client
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_nil first.posted_on
  end

  # descriptionは「募集職種 / スキル / 稼働日数 / 働き方」の順に連結する。
  # スキルと働き方を含めることでEngineerClassifierの技術判定・リモート判定が効く。
  def test_parse_first_posting_description_joins_spec_table
    assert_equal(
      "募集職種: フルスタックエンジニア / スキル: Ruby, Ruby on Rails, Vue.js, Next.js / 稼働日数: 週5日 / 働き方: ハイブリッド",
      parse_fixture.first.description
    )
  end

  # --- 単価非公開・働き方なしの案件（aws/jd02415）も落とさない ---

  def test_parse_posting_without_price_and_work_style
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/aws/jd02415") }

    refute_nil posting
    assert_equal "要確認", posting.reward
    assert_equal "月額制（業務委託）", posting.work_format, "金額が無くても単位（/ 月）は表示されるので月額制と判定する"
    assert_equal ["Python", "Go", "Ruby", "AWS", "Google Cloud", "Azure", "Kubernetes"], posting.skills
    assert_equal [], posting.tags
    assert_equal "募集職種: SRE / スキル: Python, Go, Ruby, AWS, Google Cloud, Azure, Kubernetes / 稼働日数: 週5日",
                 posting.description
  end

  # 3桁の万円表記（135万円）も円に換算して桁区切りする。
  def test_parse_posting_with_three_digit_price
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/python/jd02101") }

    refute_nil posting
    assert_equal "1,350,000〜1,500,000円／月", posting.reward
    assert_equal ["高単価", "ハイブリッド"], posting.tags
  end

  # 募集職種が複数ある案件（typescript/jd01705-2）は "," 区切りで全てdescriptionに入れる。
  def test_parse_posting_with_multiple_occupations
    posting = parse_fixture(TYPESCRIPT_FIXTURE, category_hint: "TypeScript")
              .find { |candidate| candidate.url.end_with?("/typescript/jd01705-2") }

    refute_nil posting
    assert_includes posting.description, "募集職種: フロントエンドエンジニア, フルスタックエンジニア, AIエンジニア"
  end

  # --- 全件で必須フィールドが埋まる（"-"や""は許すがnilは許さない） ---

  def test_every_posting_fills_display_fields
    parse_fixture.each do |posting|
      refute_empty posting.title, "案件名が空のカードは除外されるはず"
      refute_empty posting.reward
      refute_empty posting.work_format
      refute_empty posting.description
      assert_instance_of Array, posting.skills
      assert_instance_of Array, posting.tags
      refute_empty posting.skills, "フィクスチャ全件にスキルタグがある"
    end
  end

  # --- URL正規化 ---

  def test_urls_are_normalized_absolute_without_trailing_slash_or_query
    parse_fixture.each do |posting|
      assert_match %r{\Ahttps://relance\.jp/project/[a-z]+/jd\d+(?:-\d+)?\z}, posting.url,
                   "末尾スラッシュなし・クエリなしの正規化された絶対URLのはず"
    end
  end

  # --- category_hint が引数どおり全件に伝わる ---

  def test_category_hint_is_propagated_to_every_posting
    postings = parse_fixture(category_hint: "TypeScript")

    assert(postings.all? { |posting| posting.category_hint == "TypeScript" },
           "全件のcategory_hintが引数のTypeScriptになるはず")
  end

  # --- 単価の単位で work_format が分岐する ---

  def test_work_format_is_hourly_when_unit_is_per_hour
    fragment = build_card_html(href: "https://relance.jp/project/ruby/jd00001/", title: "時間単価の案件",
                               amounts: %w[5000 6000], unit: "時")
    posting = FreelanceJobs::Sources::Relance.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    assert_equal "50,000,000〜60,000,000円／時", posting.reward, "数値は万円単位として読む"
    assert_equal "時間単価制", posting.work_format
  end

  def test_work_format_falls_back_when_unit_is_missing
    fragment = build_card_html(href: "https://relance.jp/project/ruby/jd00002/", title: "単位なしの案件", amounts: [], unit: "")
    posting = FreelanceJobs::Sources::Relance.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    assert_equal "要確認", posting.reward
    assert_equal "業務委託（フリーランス）", posting.work_format
  end

  def test_reward_is_single_amount_when_only_one_price_is_shown
    fragment = build_card_html(href: "https://relance.jp/project/ruby/jd00003/", title: "上限のみの案件", amounts: %w[80])
    posting = FreelanceJobs::Sources::Relance.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    assert_equal "800,000円／月", posting.reward
  end

  # --- 相対パスのhrefでも絶対URLに組み立てる ---

  def test_relative_href_is_expanded_to_absolute_url
    fragment = build_card_html(href: "/project/ruby/jd00004/", title: "相対パスの案件")
    posting = FreelanceJobs::Sources::Relance.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    assert_equal "https://relance.jp/project/ruby/jd00004", posting.url
  end

  # --- 必須要素（案件URL・案件名）が欠けたカードは黙って除外する ---

  def test_parse_skips_card_whose_link_is_not_a_job
    fragment = build_card_html(href: "https://relance.jp/blog/how-to-freelance/", title: "案件ではないリンク")

    assert_equal [], FreelanceJobs::Sources::Relance.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_skips_card_whose_link_points_to_another_site
    fragment = build_card_html(href: "https://example.com/project/ruby/jd00005/", title: "別サイトへのリンク")

    assert_equal [], FreelanceJobs::Sources::Relance.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_skips_card_without_href
    fragment = <<~HTML
      <article class="p-project_item">
        <a>
          <h2 class="p-project_item__title">hrefなし</h2>
        </a>
      </article>
    HTML

    assert_equal [], FreelanceJobs::Sources::Relance.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_skips_card_without_title
    fragment = <<~HTML
      <article class="p-project_item">
        <a href="https://relance.jp/project/ruby/jd00006/">
          <div class="p-project_item__price_range"><span class="p-project_item__price_lg">50</span>万円 / 月</div>
        </a>
      </article>
    HTML

    assert_equal [], FreelanceJobs::Sources::Relance.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  # --- 次ページ判定は a.next の有無で行う ---

  # 最終ページにも a.prev と page/1/ への a.p-pagination__num が残るため、「/page/ を含むリンク」では判定できない。
  def test_next_page_is_detected_only_by_next_link
    first_page = Nokogiri::HTML(read_fixture(TYPESCRIPT_FIXTURE))
    last_page = Nokogiri::HTML(read_fixture(TYPESCRIPT_PAGE2_FIXTURE))

    assert FreelanceJobs::Sources::Relance.next_page?(first_page), "1ページ目には a.next がある"
    refute FreelanceJobs::Sources::Relance.next_page?(last_page), "最終ページには a.next が無い（a.prev はある）"
    refute_empty last_page.css("section.p-pagination a[href*='/page/']"), "最終ページにも /page/ リンク自体は残っている"
  end

  # --- fetch: 次ページリンクがある間だけページ送りし、URL重複を排除する ---

  # URLごとの応答を台本化するFakeフェッチャー（通信しない）。
  # 各URLの配列を先頭から1回ずつ消費し、例外なら送出・文字列ならbodyとして返す。
  # 台本に無いURL・使い切ったURLはカード0件のページを返す（＝ページ送りの終端）。
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

  def test_fetch_follows_next_page_and_stops_at_last_page
    fetcher = ScriptedFetcher.new(
      {
        "#{LIST_URL_PREFIX}/typescript/" => [read_fixture(TYPESCRIPT_FIXTURE)],
        "#{LIST_URL_PREFIX}/typescript/page/2/" => [read_fixture(TYPESCRIPT_PAGE2_FIXTURE)]
      },
      fallback_body: empty_list_body
    )
    postings = build_source(fetcher, [{ skill_slug: "typescript", category_hint: "TypeScript" }]).fetch

    assert_equal ["#{LIST_URL_PREFIX}/typescript/", "#{LIST_URL_PREFIX}/typescript/page/2/"], fetcher.requested_urls,
                 "2ページ目に a.next が無いので3ページ目は取得しないはず"
    assert_equal 13, postings.size
  end

  def test_fetch_deduplicates_postings_across_skills
    fetcher = ScriptedFetcher.new(
      {
        "#{LIST_URL_PREFIX}/ruby/" => [read_fixture(RUBY_FIXTURE)],
        "#{LIST_URL_PREFIX}/typescript/" => [read_fixture(TYPESCRIPT_FIXTURE)],
        "#{LIST_URL_PREFIX}/typescript/page/2/" => [read_fixture(TYPESCRIPT_PAGE2_FIXTURE)]
      },
      fallback_body: empty_list_body
    )
    search_targets = [
      { skill_slug: "ruby", category_hint: "Ruby" },
      { skill_slug: "typescript", category_hint: "TypeScript" }
    ]
    postings = build_source(fetcher, search_targets).fetch

    assert_equal 3, fetcher.requested_urls.size, "ruby 1頁 + typescript 2頁"
    # ruby 8件 + typescript 13件のうち other/jd01470-2 と rubyonrails/jd01223 の2件が両方に現れる。
    assert_equal 19, postings.size
    assert_equal postings.map(&:url).uniq.size, postings.size
    overlapping = postings.find { |posting| posting.url.end_with?("/rubyonrails/jd01223") }
    assert_equal "Ruby", overlapping.category_hint, "先に取得した ruby 一覧の category_hint を保持するはず"
  end

  def test_fetch_stops_paging_at_max_pages_even_if_next_link_remains
    fetcher = ScriptedFetcher.new(
      {
        "#{LIST_URL_PREFIX}/typescript/" => [read_fixture(TYPESCRIPT_FIXTURE)],
        "#{LIST_URL_PREFIX}/typescript/page/2/" => [read_fixture(TYPESCRIPT_FIXTURE)],
        "#{LIST_URL_PREFIX}/typescript/page/3/" => [read_fixture(TYPESCRIPT_FIXTURE)]
      },
      fallback_body: empty_list_body
    )
    source = build_source(fetcher, [{ skill_slug: "typescript", category_hint: "TypeScript" }], max_pages: 2)

    postings = source.fetch

    assert_equal 2, fetcher.requested_urls.size, "a.next が残っていても max_pages で打ち切るはず"
    assert_equal 10, postings.size, "同じページを2回読んでもURLで重複排除されるはず"
  end

  def test_fetch_stops_paging_when_a_page_has_no_card
    fetcher = ScriptedFetcher.new({ "#{LIST_URL_PREFIX}/ruby/" => [empty_list_body] }, fallback_body: empty_list_body)
    postings = build_source(fetcher, [{ skill_slug: "ruby", category_hint: "Ruby" }]).fetch

    assert_equal 1, fetcher.requested_urls.size
    assert_equal [], postings
  end

  # --- ページ単位の取得失敗に耐える ---

  def test_fetch_retries_once_when_a_page_returns_server_error
    fetcher = ScriptedFetcher.new(
      { "#{LIST_URL_PREFIX}/ruby/" => [server_error, read_fixture(RUBY_FIXTURE)] },
      fallback_body: empty_list_body
    )
    postings = silencing_fetch_logs { build_source(fetcher, [{ skill_slug: "ruby", category_hint: "Ruby" }]).fetch }

    assert_equal 8, postings.size, "1回目が500でも再取得すれば取得できるはず"
    assert_equal ["#{LIST_URL_PREFIX}/ruby/", "#{LIST_URL_PREFIX}/ruby/"], fetcher.requested_urls
  end

  # HTTPエラーだけでなく通信層の切断・タイムアウトも同じく1スキルの打ち切りで済ませる。
  def test_fetch_skips_failing_skill_and_keeps_other_skills
    connection_error = Errno::ECONNRESET.new("Connection reset by peer")
    fetcher = ScriptedFetcher.new(
      {
        "#{LIST_URL_PREFIX}/typescript/" => [connection_error, connection_error],
        "#{LIST_URL_PREFIX}/ruby/" => [read_fixture(RUBY_FIXTURE)]
      },
      fallback_body: empty_list_body
    )
    search_targets = [
      { skill_slug: "typescript", category_hint: "TypeScript" },
      { skill_slug: "ruby", category_hint: "Ruby" }
    ]
    postings = silencing_fetch_logs { build_source(fetcher, search_targets).fetch }

    assert_equal 8, postings.size, "typescriptが落ちてもrubyの結果は返すはず"
    refute_includes fetcher.requested_urls, "#{LIST_URL_PREFIX}/typescript/page/2/",
                    "2回とも失敗したスキルはページ送りを打ち切るはず"
  end

  # 全滅を黙って0件で返すとサイト構造の崩れに気付けないため、最初の失敗を送出する。
  def test_fetch_raises_when_no_page_succeeds
    error = server_error
    fetcher = ScriptedFetcher.new({ "#{LIST_URL_PREFIX}/ruby/" => [error, error] }, fallback_body: empty_list_body)

    raised = assert_raises(FreelanceJobs::FetchError) do
      silencing_fetch_logs { build_source(fetcher, [{ skill_slug: "ruby", category_hint: "Ruby" }]).fetch }
    end

    assert_equal error.message, raised.message
  end

  # WAFのアクセス制限は取り直しても解消しないので、再取得せずそのまま送出する。
  def test_fetch_does_not_retry_when_access_is_blocked
    fetcher = ScriptedFetcher.new(
      { "#{LIST_URL_PREFIX}/ruby/" => [FreelanceJobs::AccessBlockedError.new("アクセス制限（WAF captcha）")] },
      fallback_body: empty_list_body
    )

    assert_raises(FreelanceJobs::AccessBlockedError) do
      silencing_fetch_logs { build_source(fetcher, [{ skill_slug: "ruby", category_hint: "Ruby" }]).fetch }
    end
    assert_equal 1, fetcher.requested_urls.size, "アクセス制限では再取得しないはず"
  end

  # --- 既定の取得対象とリクエスト予算 ---

  def test_default_search_targets_cover_four_skill_slugs
    slugs = FreelanceJobs::Sources::Relance::DEFAULT_SEARCH_TARGETS.map { |target| target[:skill_slug] }

    assert_equal %w[ruby rubyonrails typescript react], slugs
    assert_equal 3, FreelanceJobs::Sources::Relance::MAX_PAGES_PER_SKILL
    assert_operator slugs.size * FreelanceJobs::Sources::Relance::MAX_PAGES_PER_SKILL, :<=, 40,
                    "1バッチのリクエスト上限40回に収まるはず"
    assert_equal 1.5, FreelanceJobs::Sources::Relance::REQUEST_INTERVAL
  end

  # --- Profile::ENGINEER にRelanceが含まれる（BEGINNERには含まれない） ---

  def test_engineer_profile_includes_relance_source
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_includes source_classes, FreelanceJobs::Sources::Relance
  end

  def test_beginner_profile_does_not_include_relance_source
    source_classes = FreelanceJobs::Profile::BEGINNER.source_specs.map(&:first)

    refute_includes source_classes, FreelanceJobs::Sources::Relance
  end

  private

  def build_source(fetcher, search_targets, max_pages: FreelanceJobs::Sources::Relance::MAX_PAGES_PER_SKILL)
    FreelanceJobs::Sources::Relance.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, max_pages: max_pages
    )
  end

  def server_error
    FreelanceJobs::FetchError.new("HTTP 500 #{LIST_URL_PREFIX}/ruby/")
  end

  def empty_list_body
    wrap_html("<div>該当なし</div>")
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

  # Relanceのカード1件分のHTML片（実データのDOM構造を模したもの）。
  # 単価は万円単位の数値を span.p-project_item__price_lg に入れ、末尾に " / 月" のような単位を付ける。
  def build_card_html(href:, title:, amounts: %w[63 78], unit: "月", occupations: ["バックエンドエンジニア"],
                      skills: ["Ruby"], working_days: "週5日", work_styles: ["フルリモート"], badges: ["NEW"])
    href_attribute = href.nil? ? "" : %( href="#{href}")
    price_html = amounts.map { |amount| %(<span class="p-project_item__price"><span class="p-project_item__price_lg">#{amount}</span>万円</span>) }.join(" ～ ")
    unit_html = unit.empty? ? "" : " / #{unit}"
    badge_html = badges.map { |badge| %(<div class="c-badge">#{badge}</div>) }.join

    <<~HTML
      <article class="p-project_item">
        <a#{href_attribute}>
          <div class="p-project_item__header">
            <div class="p-badges p-project_item__meta">#{badge_html}</div>
            <h2 class="p-project_item__title">#{title}</h2>
            <div class="p-project_item__price_range">#{price_html}#{unit_html}</div>
          </div>
          <div class="p-project_item__body">
            <dl class="p-project_spec p-project_spec--archive">
              #{build_spec_item_html("募集職種", occupations)}
              #{build_spec_item_html("スキル", skills)}
              <div class="p-project_spec__item">
                <dt><span class="u-sp-sr-only">稼働日数</span></dt>
                <dd>#{working_days}</dd>
              </div>
              #{build_spec_item_html("働き方", work_styles)}
            </dl>
          </div>
        </a>
      </article>
    HTML
  end

  def build_spec_item_html(label, terms)
    term_html = terms.map { |term| %(<span class="c-term c-term--round_border">#{term}</span>) }.join
    <<~HTML
      <div class="p-project_spec__item">
        <dt><span class="u-sp-sr-only">#{label}</span></dt>
        <dd class="c-terms">#{term_html}</dd>
      </div>
    HTML
  end
end
