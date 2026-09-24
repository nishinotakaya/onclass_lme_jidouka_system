# frozen_string_literal: true
# test/services/freelance_jobs/sources_pe_bank_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"
require "logger"

class FreelanceJobsSourcesPeBankTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 13)
  FIXTURE_NAME = "pe_bank_ruby.html"
  PAGE2_FIXTURE_NAME = "pe_bank_ruby_page2.html"

  # 言語別LP（/project/<slug>/）を1ページ目、?p=N を2ページ目以降に使う。
  # スキルID検索（?skill[]=8&p=2）は Faraday がクエリを並べ替えてサイトが 301 を返すため使わない。
  RUBY_PAGE1_URL = "https://pe-bank.jp/project/ruby/"
  RUBY_PAGE2_URL = "https://pe-bank.jp/project/ruby/?p=2"
  TYPESCRIPT_PAGE1_URL = "https://pe-bank.jp/project/typescript/"
  TYPESCRIPT_PAGE2_URL = "https://pe-bank.jp/project/typescript/?p=2"

  def parse_fixture(name = FIXTURE_NAME, category_hint: "Ruby")
    FreelanceJobs::Sources::PeBank.parse(read_fixture(name), today: TODAY, category_hint: category_hint)
  end

  # 一覧は50件/頁（Ruby LP 77件の1頁目）。
  def test_parse_fixture_returns_fifty_postings
    assert_equal 50, parse_fixture.size
  end

  # 2頁目は残り27件。1頁目とURLが1件も重ならない（新着順のページングが安定している）。
  def test_parse_page2_fixture_returns_remaining_postings_without_overlap
    page1_urls = parse_fixture.map(&:url)
    page2_urls = parse_fixture(PAGE2_FIXTURE_NAME).map(&:url)

    assert_equal 27, page2_urls.size
    assert_empty page1_urls & page2_urls
  end

  # --- 1件目の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "PE-BANK", first.site
    assert_equal "https://pe-bank.jp/project/ruby/54618-65/", first.url
    assert_equal "【Ruby/リモート】口コミ機能開発AI活用Rails案件", first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "80万円～85万円／月", first.reward
    assert_equal "月額制（業務委託）", first.work_format
    assert_equal ["Ruby"], first.skills
    assert_equal ["担当者オススメ案件", "リモート可"], first.tags
    assert_equal "", first.client
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_nil first.posted_on, "一覧に掲載日は無い"
  end

  # descriptionは「内容 / 使用技術 / 勤務地 / こだわり」の順に連結する。
  # 内容の改行と勤務地の全角スペースは normalize_description で半角スペース1つに畳まれる。
  def test_parse_first_posting_description_joins_content_skills_location_and_tags
    description = parse_fixture.first.description

    assert_equal(
      "内容: ・口コミ機能の新規開発／改善（Ruby on Rails） ・AIツール（Claude Code／Codex／Dev… / " \
      "使用技術: Ruby / 勤務地: 23区 その他（非公開含む） / 担当者オススメ案件・リモート可",
      description
    )
  end

  # --- スキルは「Ruby , Typescript , Vue.js , React」のカンマ区切り1本のテキストを分割する ---

  def test_parse_posting_with_comma_separated_skills
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/54808-32/") }

    refute_nil posting
    assert_equal "【Ruby on Rails/リモート可/ビジネス英語】SaaS開発支援", posting.title
    assert_equal "75万円～87万円／月", posting.reward
    assert_equal ["Ruby", "Typescript", "Vue.js", "React"], posting.skills
    assert_includes posting.description, "使用技術: Ruby / Typescript / Vue.js / React"
    assert_includes posting.description, "勤務地: フルリモート"
  end

  # 全55スキルを列挙した「全部盛り」カードもそのまま格納する（分類器はtitle+descriptionも見るため）。
  def test_parse_posting_with_every_skill_listed_keeps_all_skills
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/java/53135-H03/") }

    refute_nil posting
    assert_operator posting.skills.size, :>, 50
    assert_includes posting.skills, "Ruby"
    assert_includes posting.skills, "Go言語(golang)"
  end

  # 案件コードが数字始まりでない（H5018-H01）URLも案件として扱う。
  def test_parse_accepts_alphanumeric_job_code
    posting = parse_fixture(PAGE2_FIXTURE_NAME).find { |candidate| candidate.url.end_with?("/php/H5018-H01/") }

    refute_nil posting
    assert_equal ["PHP", "Ruby", "SQL"], posting.skills
  end

  # --- 全件で必須フィールドが埋まる（"-"や""は許すがnilは許さない） ---

  def test_every_posting_fills_display_fields
    parse_fixture.each do |posting|
      refute_empty posting.title, "案件名が空のカードは除外されるはず"
      assert_match(/\A\d+万円～\d+万円／月\z/, posting.reward, "一覧の単価は全件が月額の範囲表記")
      assert_equal "月額制（業務委託）", posting.work_format
      assert_instance_of Array, posting.skills
      refute_empty posting.skills, "スキル欄はフィクスチャ全件に存在する"
      assert_instance_of Array, posting.tags
      assert_includes posting.description, "内容: "
      assert_includes posting.description, "勤務地: "
    end
  end

  # --- URL正規化 ---
  # AC-10: 案件詳細URLは末尾スラッシュ付きで統一する。
  # スラッシュ無し（https://pe-bank.jp/project/csharp/54339-N08）は301後にhttp側が404になる
  # 壊れたリンクの実例があり、スラッシュ付きなら200になるため。

  def test_urls_are_normalized_absolute_with_trailing_slash_and_without_query
    postings = parse_fixture

    assert_equal 50, postings.map(&:url).uniq.size
    postings.each do |posting|
      assert_match %r{\Ahttps://pe-bank\.jp/project/[\w-]+/[A-Za-z0-9-]+/\z}, posting.url,
                   "末尾スラッシュ付き・クエリなしの正規化された絶対URLのはず"
    end
  end

  # 実例(54339-N08)そのもので、hrefにスラッシュが無くてもposting.urlは末尾スラッシュ付きになる想定。
  def test_parse_normalizes_job_url_to_always_have_a_trailing_slash_even_when_href_lacks_one
    fragment = build_card_html(href: "https://pe-bank.jp/project/csharp/54339-N08", title: "スラッシュ無しhref")
    posting = FreelanceJobs::Sources::PeBank.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    assert_equal "https://pe-bank.jp/project/csharp/54339-N08/", posting.url
  end

  def test_parse_keeps_job_url_trailing_slash_when_href_already_has_one
    fragment = build_card_html(href: "https://pe-bank.jp/project/csharp/54339-N08/", title: "スラッシュ付きhref")
    posting = FreelanceJobs::Sources::PeBank.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    assert_equal "https://pe-bank.jp/project/csharp/54339-N08/", posting.url
  end

  # --- category_hint が引数どおり全件に伝わる ---

  def test_category_hint_is_propagated_to_every_posting
    postings = parse_fixture(category_hint: "TypeScript")

    assert(postings.all? { |posting| posting.category_hint == "TypeScript" },
           "全件のcategory_hintが引数のTypeScriptになるはず")
  end

  # --- 単価の表記で reward / work_format が分岐する ---

  def test_reward_is_unknown_when_amount_is_empty
    fragment = build_card_html(href: "https://pe-bank.jp/project/ruby/1-A/", title: "単価非公開", reward_html: "")
    posting = FreelanceJobs::Sources::PeBank.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    assert_equal "要確認", posting.reward
    assert_equal "業務委託（フリーランス）", posting.work_format
  end

  # 数値spanの無い文言（応相談など）は単位を付けずそのまま表示する。
  def test_reward_keeps_text_without_monthly_unit_when_amount_span_is_missing
    fragment = build_card_html(href: "https://pe-bank.jp/project/ruby/2-A/", title: "単価応相談", reward_html: "応相談")
    posting = FreelanceJobs::Sources::PeBank.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    assert_equal "応相談", posting.reward
    assert_equal "業務委託（フリーランス）", posting.work_format
  end

  # --- 相対パスの href にも BASE_URL を補う ---

  def test_parse_accepts_relative_job_path
    fragment = build_card_html(href: "/project/ruby/3-A/", title: "相対パス")
    posting = FreelanceJobs::Sources::PeBank.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    assert_equal "https://pe-bank.jp/project/ruby/3-A/", posting.url
  end

  # --- 必須要素（案件URL・案件名）が欠けたカードは黙って除外する ---

  def test_parse_skips_card_whose_link_is_not_a_job_url
    fragment = build_card_html(href: "https://pe-bank.jp/project/ruby/", title: "言語別LPへのリンク")

    assert_equal [], FreelanceJobs::Sources::PeBank.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_skips_card_without_title_link
    fragment = <<~HTML
      <ul class="projectList"><div class="box">
        <li>
          <dl class="projectListCts">
            <dt><h3 class="projectTitle">リンクなし</h3></dt>
            <dd><span>単　価：</span><p><span>80</span>万円～<span>85</span>万円</p></dd>
          </dl>
        </li>
      </div></ul>
    HTML

    assert_equal [], FreelanceJobs::Sources::PeBank.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_skips_card_with_empty_title
    fragment = build_card_html(href: "https://pe-bank.jp/project/ruby/4-A/", title: "   ")

    assert_equal [], FreelanceJobs::Sources::PeBank.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  # 不正ネスト（ul > div.box > li）を前提にしているため、ul.projectList 直下の li は拾わない。
  # （実サイトがこの形になったらセレクタごと見直す必要がある。0件になって気付けるようにしておく）
  def test_parse_ignores_cards_that_are_direct_children_of_ul
    fragment = <<~HTML
      <ul class="projectList">
        <li>
          <dl class="projectListCts">
            <dt><h3 class="projectTitle"><a href="https://pe-bank.jp/project/ruby/5-A/">直下のli</a></h3></dt>
          </dl>
        </li>
      </ul>
    HTML

    assert_equal [], FreelanceJobs::Sources::PeBank.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  # --- fetch: search_targets × ページ数だけ取得し、URL重複を排除する ---

  # 取得URLを記録し、常に同じbodyを返すFakeフェッチャー（通信しない）。
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

  # 1頁目のフィクスチャには「次へ」リンクがあるので、max_pages まで同じbodyを取り続けても重複排除で50件のまま。
  def test_fetch_requests_each_target_and_page_and_deduplicates_urls
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    search_targets = [
      { language_slug: "ruby", category_hint: "Ruby" },
      { language_slug: "typescript", category_hint: "TypeScript" }
    ]
    source = FreelanceJobs::Sources::PeBank.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, max_pages: 2
    )

    postings = source.fetch

    assert_equal(
      [RUBY_PAGE1_URL, RUBY_PAGE2_URL, TYPESCRIPT_PAGE1_URL, TYPESCRIPT_PAGE2_URL],
      fetcher.requested_urls
    )
    assert_equal 50, postings.size, "同じURLの案件が4ページ分返っても重複排除され50件のままのはず"
  end

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

  # Ruby LPは2頁で全77件。2頁目には「次へ」リンクの href が無いので、max_pages に余裕があっても3頁目は取らない。
  def test_fetch_stops_paging_at_last_page_and_merges_both_pages
    fetcher = ScriptedFetcher.new(
      { RUBY_PAGE1_URL => [read_fixture(FIXTURE_NAME)], RUBY_PAGE2_URL => [read_fixture(PAGE2_FIXTURE_NAME)] },
      fallback_body: empty_list_body
    )
    postings = build_source(fetcher, ["ruby"], max_pages: 5).fetch

    assert_equal [RUBY_PAGE1_URL, RUBY_PAGE2_URL], fetcher.requested_urls
    assert_equal 77, postings.size
  end

  def test_fetch_stops_paging_when_a_page_has_no_card
    fetcher = ScriptedFetcher.new({ RUBY_PAGE1_URL => [read_fixture(FIXTURE_NAME)] }, fallback_body: empty_list_body)
    postings = build_source(fetcher, ["ruby"], max_pages: 3).fetch

    assert_equal [RUBY_PAGE1_URL, RUBY_PAGE2_URL], fetcher.requested_urls, "2ページ目が0件なら3ページ目は取得しないはず"
    assert_equal 50, postings.size
  end

  # --- ページ単位の取得失敗に耐える ---

  def test_fetch_retries_once_when_a_page_returns_server_error
    fetcher = ScriptedFetcher.new(
      { RUBY_PAGE1_URL => [server_error, read_fixture(FIXTURE_NAME)] },
      fallback_body: empty_list_body
    )
    postings = silencing_fetch_logs { build_source(fetcher, ["ruby"]).fetch }

    assert_equal 50, postings.size, "1回目が500でも再取得すれば取得できるはず"
    assert_equal [RUBY_PAGE1_URL, RUBY_PAGE1_URL, RUBY_PAGE2_URL], fetcher.requested_urls
  end

  # HTTPエラーだけでなく通信層の切断・タイムアウトも同じく1言語の打ち切りで済ませる。
  def test_fetch_skips_failing_skill_and_keeps_other_skills
    connection_error = Errno::ECONNRESET.new("Connection reset by peer")
    fetcher = ScriptedFetcher.new(
      {
        TYPESCRIPT_PAGE1_URL => [connection_error, connection_error],
        RUBY_PAGE1_URL => [read_fixture(FIXTURE_NAME)]
      },
      fallback_body: empty_list_body
    )
    postings = silencing_fetch_logs { build_source(fetcher, ["typescript", "ruby"]).fetch }

    assert_equal 50, postings.size, "Typescriptが落ちてもRubyの結果は返すはず"
    refute_includes fetcher.requested_urls, TYPESCRIPT_PAGE2_URL,
                    "2回とも失敗した言語はページ送りを打ち切るはず"
  end

  # 全滅を黙って0件で返すとサイト構造の崩れに気付けないため、最初の失敗を送出する。
  def test_fetch_raises_when_no_page_succeeds
    error = server_error
    fetcher = ScriptedFetcher.new({ RUBY_PAGE1_URL => [error, error] }, fallback_body: empty_list_body)

    raised = assert_raises(FreelanceJobs::FetchError) do
      silencing_fetch_logs { build_source(fetcher, ["ruby"]).fetch }
    end

    assert_equal error.message, raised.message
  end

  # WAFのアクセス制限は取り直しても解消しないので、再取得せずそのまま送出する。
  def test_fetch_does_not_retry_when_access_is_blocked
    fetcher = ScriptedFetcher.new(
      { RUBY_PAGE1_URL => [FreelanceJobs::AccessBlockedError.new("アクセス制限（WAF captcha）")] },
      fallback_body: empty_list_body
    )

    assert_raises(FreelanceJobs::AccessBlockedError) do
      silencing_fetch_logs { build_source(fetcher, ["ruby"]).fetch }
    end
    assert_equal 1, fetcher.requested_urls.size, "アクセス制限では再取得しないはず"
  end

  # --- 既定設定: 言語3種 × 2頁 = 最大6リクエストで予算40回に収まる ---

  def test_default_search_targets_cover_three_language_slugs_within_request_budget
    targets = FreelanceJobs::Sources::PeBank::DEFAULT_SEARCH_TARGETS

    assert_equal %w[ruby typescript react], targets.map { |target| target[:language_slug] }
    assert_equal %w[Ruby TypeScript React], targets.map { |target| target[:category_hint] }
    assert_equal 2, FreelanceJobs::Sources::PeBank::MAX_PAGES
    assert_operator targets.size * FreelanceJobs::Sources::PeBank::MAX_PAGES, :<=, 40
    assert_equal 1.5, FreelanceJobs::Sources::PeBank::REQUEST_INTERVAL
  end

  # --- Profile::ENGINEER にPeBankが含まれる（BEGINNERには含まれない） ---

  def test_engineer_profile_includes_pe_bank_source
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_includes source_classes, FreelanceJobs::Sources::PeBank
  end

  def test_beginner_profile_does_not_include_pe_bank_source
    source_classes = FreelanceJobs::Profile::BEGINNER.source_specs.map(&:first)

    refute_includes source_classes, FreelanceJobs::Sources::PeBank
  end

  private

  def build_source(fetcher, language_slugs, max_pages: 2)
    search_targets = language_slugs.map { |language_slug| { language_slug: language_slug, category_hint: "Ruby" } }
    FreelanceJobs::Sources::PeBank.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, max_pages: max_pages
    )
  end

  def server_error
    FreelanceJobs::FetchError.new("HTTP 500 #{RUBY_PAGE1_URL}")
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

  # PE-BANKのカード1件分のHTML片（実データのDOM構造を模したもの）。
  # 実サイトの <ul><div class="box"><li> という不正ネストもそのまま再現する。
  def build_card_html(href:, title:, reward_html: "<span>80</span>万円～<span>85</span>万円",
                      location: "23区　その他（非公開含む）", content: "・Railsでの開発", skills_text: "Ruby",
                      merits: ["リモート可"])
    merit_html = merits.map { |merit| "<li>#{merit}</li>" }.join

    <<~HTML
      <ul class="projectList"><div class="box">
        <li>
          <dl class="projectListCts">
            <dt><h3 class="projectTitle"><a href="#{href}">#{title}</a></h3></dt>
            <dd><span>単　価：</span><p>#{reward_html}</p></dd>
            <dd><span>勤務地：</span><p>#{location}</p></dd>
            <dd><span>内　容：</span><p>#{content}</p></dd>
            <dd><span>スキル：</span><p><span>#{skills_text}</span></p></dd>
          </dl>
          <ul class="projectListMerit">#{merit_html}</ul>
          <a href="#{href}" class="projectDetailBtn">詳細を見る</a>
        </li>
      </div></ul>
    HTML
  end
end
