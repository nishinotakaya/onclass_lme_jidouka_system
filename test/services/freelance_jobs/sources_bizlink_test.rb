# frozen_string_literal: true
# test/services/freelance_jobs/sources_bizlink_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesBizlinkTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 12)
  FIXTURE_NAME = "bizlink_ruby.html"

  def parse_fixture(category_hint: "Ruby")
    FreelanceJobs::Sources::Bizlink.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  # 一覧は9件/頁。RSC flight payload(<script>)にも同じカードのJSONが入っているが、
  # Nokogiriはscript内を要素化しないので18件ではなく9件になる。
  def test_parse_fixture_returns_nine_postings
    assert_equal 9, parse_fixture.size
  end

  # --- 1件目の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "ビズリンク", first.site
    assert_equal "https://freelance.bizlink.io/jobs/21187", first.url
    assert_equal "【Ruby｜経験5年】プロダクト開発支援｜バックエンドエンジニア", first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "1,000,000円／月", first.reward
    assert_equal "月額制（業務委託）", first.work_format
    assert_equal ["Ruby"], first.skills
    assert_equal ["リモート可能", "面接一回"], first.tags
    assert_equal "", first.client
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_equal Date.new(2026, 8, 28), first.posted_on
  end

  # descriptionは「業務内容 / 必須スキル / 使用技術 / 勤務地 / こだわり」の順に連結する。
  # 使用技術・勤務地を含めることでEngineerClassifierの技術判定・リモート判定が効く。
  def test_parse_first_posting_description_joins_sections_skills_location_and_tags
    description = parse_fixture.first.description

    assert_equal(
      "業務内容: 現在リリースしている２つのプロダクトの機能開発・運用に従事いただきます。 " \
      "Ai駆動開発を取り入れながら、バックエンド側を軸としてお力添えいただける方を募集してお... / " \
      "必須スキル: ・Rubyでのアプリケーション・APIの開発、運用経験4~5年以上 ・React/Vueなどのフロントエンド開発 " \
      "・コンテナ技術利用経験3年以上(Dockerな... / 使用技術: Ruby / 勤務地: リモート※週1出社必須 / リモート可能・面接一回",
      description
    )
  end

  # <br>を空白へ置換せずに.textすると「運用経験4~5年以上・React/Vue」のように行が連結してしまう。
  def test_detail_section_line_breaks_become_spaces
    description = parse_fixture.first.description

    assert_includes description, "運用経験4~5年以上 ・React/Vue"
    refute_includes description, "運用経験4~5年以上・React/Vue"
  end

  # --- 複数スキル・単価が別案件でも取れる ---

  def test_parse_posting_with_multiple_skills
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/21184") }

    refute_nil posting
    assert_equal "【Ruby on Rails｜経験5年】業務支援アプリケーション開発支援｜バックエンドエンジニア", posting.title
    assert_equal "550,000円／月", posting.reward
    assert_equal ["Ruby", "Ruby on Rails"], posting.skills
    assert_equal Date.new(2026, 8, 21), posting.posted_on
    assert_includes posting.description, "勤務地: 神田"
  end

  def test_parse_posting_with_five_skills
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/21003") }

    refute_nil posting
    assert_equal "850,000円／月", posting.reward
    assert_equal ["PHP", "Python", "Java", "Ruby", "Go"], posting.skills
    assert_equal ["リモート可能", "急募案件"], posting.tags
  end

  # --- 全件で必須フィールドが埋まる（"-"や""は許すがnilは許さない） ---

  def test_every_posting_fills_display_fields
    parse_fixture.each do |posting|
      refute_empty posting.title, "案件名が空のカードは除外されるはず"
      refute_empty posting.reward
      refute_empty posting.work_format
      assert_instance_of Array, posting.skills
      assert_instance_of Array, posting.tags
      refute_nil posting.posted_on, "掲載日はフィクスチャ全件に存在する"
    end
  end

  # --- URL正規化 ---

  def test_urls_are_normalized_absolute_without_trailing_slash_or_query
    parse_fixture.each do |posting|
      assert_match %r{\Ahttps://freelance\.bizlink\.io/jobs/\d+\z}, posting.url,
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

  def test_work_format_is_hourly_when_currency_is_per_hour
    fragment = build_card_html(href: "/jobs/1", title: "時間単価の案件", salary: "7,680", currency: "円／時")
    posting = FreelanceJobs::Sources::Bizlink.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    assert_equal "7,680円／時", posting.reward
    assert_equal "時間単価制", posting.work_format
  end

  def test_work_format_falls_back_when_currency_is_missing
    fragment = build_card_html(href: "/jobs/2", title: "単価非公開の案件", salary: "", currency: "")
    posting = FreelanceJobs::Sources::Bizlink.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    assert_equal "要確認", posting.reward
    assert_equal "業務委託（フリーランス）", posting.work_format
  end

  # --- 想定外の掲載日表記は posted_on を nil にする ---

  def test_posted_on_is_nil_when_date_format_is_unexpected
    fragment = build_card_html(href: "/jobs/3", title: "掲載日が想定外", date_posted: "2026年8月28日")
    posting = FreelanceJobs::Sources::Bizlink.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    assert_nil posting.posted_on
  end

  # --- 必須要素（案件パス・案件名）が欠けたカードは黙って除外する ---

  def test_parse_skips_card_without_job_path
    fragment = build_card_html(href: "/company/about", title: "案件ではないリンク")

    assert_equal [], FreelanceJobs::Sources::Bizlink.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_skips_card_without_href
    fragment = <<~HTML
      <a class="ProjectCardOnList_ProjectCard__BypSw">
        <p class="ProjectCardOnList_jobTitle__Z7bct">hrefなし</p>
      </a>
    HTML

    assert_equal [], FreelanceJobs::Sources::Bizlink.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_skips_card_without_title
    fragment = <<~HTML
      <a class="ProjectCardOnList_ProjectCard__BypSw" href="/jobs/4">
        <div class="ProjectCardOnList_datePosted__DL907">2026.08.28</div>
      </a>
    HTML

    assert_equal [], FreelanceJobs::Sources::Bizlink.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  # --- fetch: search_targets × ページ数だけ取得し、URL重複を排除する ---

  # 取得URLを記録し、ページ番号に応じたbodyを返すFakeフェッチャー（通信しない）。
  class RecordingFetcher
    def initialize(body:, empty_body: nil)
      @body = body
      @empty_body = empty_body
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      @empty_body && !url.end_with?("/p/1") ? @empty_body : @body
    end
  end

  def test_fetch_requests_each_target_and_page_and_deduplicates_urls
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    search_targets = [
      { skill_slug: "ruby", category_hint: "Ruby" },
      { skill_slug: "ruby-on-rails", category_hint: "Ruby" }
    ]
    source = FreelanceJobs::Sources::Bizlink.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, max_pages: 2
    )

    postings = source.fetch

    assert_equal 4, fetcher.requested_urls.size, "2スキル×2ページぶん取得するはず"
    assert_equal(
      [
        "https://freelance.bizlink.io/jobs/skill_cate/ruby/p/1",
        "https://freelance.bizlink.io/jobs/skill_cate/ruby/p/2",
        "https://freelance.bizlink.io/jobs/skill_cate/ruby-on-rails/p/1",
        "https://freelance.bizlink.io/jobs/skill_cate/ruby-on-rails/p/2"
      ],
      fetcher.requested_urls
    )
    assert_equal 9, postings.size, "同じURLの案件が4ページ分返っても重複排除され9件のままのはず"
  end

  def test_fetch_stops_paging_when_a_page_has_no_card
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME), empty_body: wrap_html("<div>該当なし</div>"))
    search_targets = [{ skill_slug: "ruby", category_hint: "Ruby" }]
    source = FreelanceJobs::Sources::Bizlink.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, max_pages: 3
    )

    postings = source.fetch

    assert_equal 2, fetcher.requested_urls.size, "2ページ目が0件なら3ページ目は取得しないはず"
    assert_equal 9, postings.size
  end

  # --- ページ単位の取得失敗（実データで散発する HTTP 500）に耐える ---

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

  LIST_URL_PREFIX = "https://freelance.bizlink.io/jobs/skill_cate"

  def test_fetch_retries_once_when_a_page_returns_server_error
    fetcher = ScriptedFetcher.new(
      { "#{LIST_URL_PREFIX}/ruby/p/1" => [server_error, read_fixture(FIXTURE_NAME)] },
      fallback_body: empty_list_body
    )
    postings = silencing_fetch_logs { build_source(fetcher, %w[ruby]).fetch }

    assert_equal 9, postings.size, "1回目が500でも再取得すれば取得できるはず"
    assert_equal(
      ["#{LIST_URL_PREFIX}/ruby/p/1", "#{LIST_URL_PREFIX}/ruby/p/1", "#{LIST_URL_PREFIX}/ruby/p/2"],
      fetcher.requested_urls
    )
  end

  # HTTPエラーだけでなく通信層の切断・タイムアウトも同じく1スキルの打ち切りで済ませる。
  def test_fetch_skips_failing_skill_and_keeps_other_skills
    connection_error = Errno::ECONNRESET.new("Connection reset by peer")
    fetcher = ScriptedFetcher.new(
      {
        "#{LIST_URL_PREFIX}/typescript/p/1" => [connection_error, connection_error],
        "#{LIST_URL_PREFIX}/ruby/p/1" => [read_fixture(FIXTURE_NAME)]
      },
      fallback_body: empty_list_body
    )
    postings = silencing_fetch_logs { build_source(fetcher, %w[typescript ruby]).fetch }

    assert_equal 9, postings.size, "typescriptが落ちてもrubyの結果は返すはず"
    refute_includes fetcher.requested_urls, "#{LIST_URL_PREFIX}/typescript/p/2",
                    "2回とも500だったスキルはページ送りを打ち切るはず"
  end

  # 全滅を黙って0件で返すとサイト構造の崩れに気付けないため、最初の失敗を送出する。
  def test_fetch_raises_when_no_page_succeeds
    error = server_error
    fetcher = ScriptedFetcher.new({ "#{LIST_URL_PREFIX}/ruby/p/1" => [error, error] }, fallback_body: empty_list_body)

    raised = assert_raises(FreelanceJobs::FetchError) do
      silencing_fetch_logs { build_source(fetcher, %w[ruby]).fetch }
    end

    assert_equal error.message, raised.message
  end

  # WAFのアクセス制限は取り直しても解消しないので、再取得せずそのまま送出する。
  def test_fetch_does_not_retry_when_access_is_blocked
    fetcher = ScriptedFetcher.new(
      { "#{LIST_URL_PREFIX}/ruby/p/1" => [FreelanceJobs::AccessBlockedError.new("アクセス制限（WAF captcha）")] },
      fallback_body: empty_list_body
    )

    assert_raises(FreelanceJobs::AccessBlockedError) do
      silencing_fetch_logs { build_source(fetcher, %w[ruby]).fetch }
    end
    assert_equal 1, fetcher.requested_urls.size, "アクセス制限では再取得しないはず"
  end

  def test_default_search_targets_cover_four_skill_slugs
    slugs = FreelanceJobs::Sources::Bizlink::DEFAULT_SEARCH_TARGETS.map { |target| target[:skill_slug] }

    assert_equal %w[ruby ruby-on-rails typescript react], slugs
    assert_equal 3, FreelanceJobs::Sources::Bizlink::MAX_PAGES
  end

  # --- Profile::ENGINEER にBizlinkが含まれる（BEGINNERには含まれない） ---

  def test_engineer_profile_includes_bizlink_source
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_includes source_classes, FreelanceJobs::Sources::Bizlink
  end

  def test_beginner_profile_does_not_include_bizlink_source
    source_classes = FreelanceJobs::Profile::BEGINNER.source_specs.map(&:first)

    refute_includes source_classes, FreelanceJobs::Sources::Bizlink
  end

  private

  def build_source(fetcher, skill_slugs, max_pages: 2)
    search_targets = skill_slugs.map { |skill_slug| { skill_slug: skill_slug, category_hint: "Ruby" } }
    FreelanceJobs::Sources::Bizlink.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, max_pages: max_pages
    )
  end

  def server_error
    FreelanceJobs::FetchError.new("HTTP 500 #{LIST_URL_PREFIX}/ruby/p/1")
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

  # ビズリンクのカード1件分のHTML片（実データのDOM構造を模したもの）。
  # CSS Modulesのハッシュ付きクラス名もそのまま再現し、前方一致セレクタが効くことを確認する。
  def build_card_html(href:, title:, date_posted: "2026.08.28", salary: "1,000,000", currency: "円／月",
                      location: "リモート", skills: ["Ruby"], labels: ["リモート可能"])
    href_attribute = href.nil? ? "" : %( href="#{href}")
    skill_html = skills.map { |skill| %(<li class="SkillSetLabel_skill__WDerZ">#{skill}</li>) }.join
    label_html = labels.map { |label| %(<span class="ProjectCardLabel_label__54kG0">#{label}</span>) }.join

    <<~HTML
      <a class="ProjectCardOnList_ProjectCard__BypSw"#{href_attribute}>
        <div class="ProjectCardOnList_cardHeader__mmwYt">
          <div class="ProjectCardOnList_datePosted__DL907">#{date_posted}</div>
        </div>
        <p class="ProjectCardOnList_jobTitle__Z7bct">#{title}</p>
        <div class="ProjectCardOnList_labelsContainer__A4Xy2">#{label_html}</div>
        <div class="ProjectCardOnList_infoBox__y3mqY">
          <div class="ProjectCardOnList_salaryInfo__bwJaf">
            <span class="ProjectCardOnList_salary__DbOs1">#{salary}</span><span class="ProjectCardOnList_currency__JHk_i">#{currency}</span>
          </div>
          <div class="ProjectCardOnList_locationInfo__49IGY"><span>#{location}</span></div>
        </div>
        <div class="ProjectCardOnList_skillsList__K5IlB"><ul>#{skill_html}</ul></div>
        <div class="ProjectCardDetails_detailsContainer__4dGCm">
          <div class="ProjectCardDetails_section__CU_7o">
            <p class="ProjectCardDetails_sectionTitle__2WKQn">必須スキル</p>
            <p class="ProjectCardDetails_content__pyURC">・Ruby経験3年以上<br>・AWS経験</p>
          </div>
          <div class="ProjectCardDetails_section__CU_7o">
            <p class="ProjectCardDetails_sectionTitle__2WKQn">業務内容</p>
            <p class="ProjectCardDetails_content__pyURC">バックエンド開発</p>
          </div>
        </div>
      </a>
    HTML
  end
end
