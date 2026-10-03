# frozen_string_literal: true
# test/services/freelance_jobs/sources_dym_tech_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"
require "logger"
require "stringio"
require "timeout"

class FreelanceJobsSourcesDymTechTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 10, 3)
  LIST_FIXTURE_NAME = "dym_tech_list.html"
  DETAIL_FIXTURE_NAME = "dym_tech_detail.html"
  SOURCE = FreelanceJobs::Sources::DymTech

  LIST_PAGE_1_URL = "https://dym-tech.jp/archives/project"
  LIST_PAGE_2_URL = "https://dym-tech.jp/archives/project/page/2"
  LIST_PAGE_3_URL = "https://dym-tech.jp/archives/project/page/3"
  FIRST_DETAIL_URL = "https://dym-tech.jp/archives/project/project-3494"

  # 一覧fixtureに載っている10件の案件ID（掲載順）。
  FIXTURE_PROJECT_IDS = [3494, 3495, 3496, 3498, 3484, 3485, 3486, 3487, 3488, 3489].freeze

  def parse_list_fixture
    SOURCE.parse(read_fixture(LIST_FIXTURE_NAME), today: TODAY)
  end

  def first_list_posting
    parse_list_fixture.first
  end

  def detail_body
    read_fixture(DETAIL_FIXTURE_NAME)
  end

  def detail_url(project_id)
    "https://dym-tech.jp/archives/project/project-#{project_id}"
  end

  # 一覧カード1件分のHTML片（実データのDOM構造を模したもの）。
  def build_card_html(project_id: 1, title: "テスト案件", salary: "50", area: "東京都", station: "渋谷駅",
                      badges: ["20代活躍中"])
    badge_html = badges.map { |badge| %(<div class="c-card-recruit__badge">#{badge}</div>) }.join
    salary_html = salary.nil? ? "" : %(<div class="c-card-recruit__salary"><span>#{salary}</span>万円/月額</div>)
    <<~HTML
      <div class="c-card-recruit" data-aos="fade-up">
        <a href="https://dym-tech.jp/archives/project/project-#{project_id}" class="c-card-recruit__inner">
          <div class="c-card-recruit__badges">#{badge_html}</div>
          <h2 class="c-card-recruit__title">
            #{title}
          </h2>
          <div class="c-card-recruit__datas">
            #{salary_html}
            <div class="c-card-recruit__area">#{area}</div>
            <div class="c-card-recruit__station">#{station}</div>
          </div>
        </a>
      </div>
    HTML
  end

  def wrap_list(fragment)
    wrap_html(%(<div class="c-card-recruit__row">#{fragment}</div>))
  end

  # --- 一覧 parse ---

  def test_parse_list_fixture_returns_ten_postings
    assert_equal 10, parse_list_fixture.size
  end

  def test_parse_list_urls_are_absolute_project_urls_in_listing_order
    urls = parse_list_fixture.map(&:url)

    assert_equal FIXTURE_PROJECT_IDS.map { |project_id| detail_url(project_id) }, urls
  end

  def test_parse_list_first_posting_has_expected_fields
    first = first_list_posting

    assert_equal "DYMテック", first.site
    assert_equal FIRST_DETAIL_URL, first.url
    assert_equal "Fintechで旅行支援サイトのシステム開発！PM募集！", first.title
    assert_equal "90万円／月", first.reward
    assert_equal %w[20代活躍中 30代活躍中 40代活躍中], first.tags, "badge(.c-card-recruit__badge)がtagsに入るはず"
    assert_equal "業務委託（フリーランス）", first.work_format
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_equal "", first.client
    assert_nil first.category_hint
    assert_nil first.posted_on, "掲載日は詳細のJSON-LDにしか無いので一覧だけでは nil のはず"
  end

  def test_parse_list_description_has_area_and_station
    description = first_list_posting.description

    assert_includes description, "東京都"
    assert_includes description, "外苑前駅"
    refute_match(/\n/, description, "normalize_descriptionで改行が畳まれているはず")
  end

  def test_parse_list_card_without_badges_has_empty_tags
    posting = parse_list_fixture.find { |candidate| candidate.url == detail_url(3495) }

    assert_equal [], posting.tags
    assert_equal "48万円／月", posting.reward
  end

  def test_parse_list_card_without_station_still_builds_posting
    posting = parse_list_fixture.find { |candidate| candidate.url == detail_url(3498) }

    assert_equal "ECサイト制作～保守/運用！フロントエンドエンジニア案件！", posting.title
    assert_includes posting.description, "岡山県"
    assert_equal "38万円／月", posting.reward
  end

  def test_parse_list_passes_category_hint_through
    postings = SOURCE.parse(read_fixture(LIST_FIXTURE_NAME), today: TODAY, category_hint: "PHP")

    assert(postings.all? { |posting| posting.category_hint == "PHP" })
  end

  def test_parse_list_blank_salary_becomes_youkakunin
    html = wrap_list(build_card_html(salary: " "))

    assert_equal "要確認", SOURCE.parse(html, today: TODAY).first.reward
  end

  def test_parse_list_missing_salary_element_becomes_youkakunin
    html = wrap_list(build_card_html(salary: nil))

    assert_equal "要確認", SOURCE.parse(html, today: TODAY).first.reward
  end

  def test_parse_list_skips_cards_without_link_or_title
    no_link = %(<div class="c-card-recruit"><h2 class="c-card-recruit__title">リンクなし</h2></div>)
    html = wrap_list(no_link + build_card_html(title: ""))

    assert_equal [], SOURCE.parse(html, today: TODAY)
  end

  def test_parse_list_deduplicates_postings_with_the_same_url
    html = wrap_list(build_card_html(project_id: 7, title: "A") + build_card_html(project_id: 7, title: "B"))

    postings = SOURCE.parse(html, today: TODAY)

    assert_equal 1, postings.size
    assert_equal "A", postings.first.title
  end

  def test_parse_list_empty_page_returns_empty_array
    assert_equal [], SOURCE.parse(wrap_list(""), today: TODAY)
  end

  # --- 詳細 apply_detail（一覧の JobPosting + 詳細HTML -> 補完済み JobPosting） ---

  def detailed_first_posting
    SOURCE.apply_detail(first_list_posting, detail_body)
  end

  def test_apply_detail_returns_a_job_posting_for_the_same_url
    posting = detailed_first_posting

    assert_instance_of FreelanceJobs::JobPosting, posting
    assert_equal FIRST_DETAIL_URL, posting.url
    assert_equal "Fintechで旅行支援サイトのシステム開発！PM募集！", posting.title
  end

  def test_apply_detail_extracts_skills_from_project_tags
    assert_equal %w[C# Laravel PHP Windows], detailed_first_posting.skills
  end

  def test_apply_detail_description_includes_required_skills_section
    description = detailed_first_posting.description

    assert_includes description, "求めるスキル: ・Webもしくはアプリの開発経験５年以上 ・プロジェクト管理の経験２年以上"
    assert_includes description, "募集背景: 事業拡大に伴う人員の補充"
    assert_includes description, "歓迎スキル: ・旅行/宿泊業界でのシステム開発・運用経験"
  end

  def test_apply_detail_description_includes_editor_body
    description = detailed_first_posting.description

    assert_includes description, "旅行支援サイト関連システムの開発"
    assert_includes description, "プロジェクトの進捗管理、調整"
  end

  def test_apply_detail_description_includes_data_items_and_list_info
    description = detailed_first_posting.description

    assert_includes description, "稼働日数：週5日"
    assert_includes description, "職種：プロジェクトマネージャー"
    assert_includes description, "東京都", "一覧由来の勤務地を失わないはず"
    assert_includes description, "外苑前駅"
    refute_match(/\n/, description)
  end

  def test_apply_detail_reads_posted_on_from_json_ld_date_posted
    assert_equal Date.new(2026, 9, 9), detailed_first_posting.posted_on,
                 "datePosted(2026-09-09T14:23:43+09:00)の日付部分のはず（UTC換算の datePublished ではない）"
  end

  def test_apply_detail_keeps_list_fields
    posting = detailed_first_posting

    assert_equal "90万円／月", posting.reward
    assert_equal %w[20代活躍中 30代活躍中 40代活躍中], posting.tags
    assert_equal "業務委託（フリーランス）", posting.work_format
    assert_equal "-", posting.application_status
    assert_equal "-", posting.deadline_text
    assert_equal "", posting.client
  end

  def test_apply_detail_posted_on_is_nil_without_json_ld
    body = detail_body.gsub(%r{<script type="application/ld\+json".*?</script>}m, "")

    assert_nil SOURCE.apply_detail(first_list_posting, body).posted_on
  end

  def test_apply_detail_posted_on_is_nil_when_json_ld_is_broken
    body = detail_body.sub(%r{(<script type="application/ld\+json"[^>]*>).*?(</script>)}m, '\1{not json\2')

    assert_nil SOURCE.apply_detail(first_list_posting, body).posted_on
  end

  def test_apply_detail_with_unrelated_html_keeps_list_information
    posting = SOURCE.apply_detail(first_list_posting, wrap_html("<p>メンテナンス中</p>"))

    assert_equal [], posting.skills
    assert_nil posting.posted_on
    assert_equal first_list_posting.title, posting.title
    assert_includes posting.description, "東京都"
  end

  # --- 定数 ---

  def test_constants
    assert_equal "DYMテック", SOURCE::SITE_NAME
    assert_equal "https://dym-tech.jp", SOURCE::BASE_URL
    assert_equal 1.5, SOURCE::REQUEST_INTERVAL
    assert_equal 3, SOURCE::DEFAULT_MAX_PAGES
  end

  # --- fetch ---

  # URL -> body のHashで返すフェイク。値が例外なら送出する。未登録URLは空文字（0件ページ）。
  class UrlMapFetcher
    def initialize(bodies_by_url:)
      @bodies_by_url = bodies_by_url
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      response = @bodies_by_url.fetch(url, "")
      raise response if response.is_a?(StandardError)

      response
    end
  end

  def detail_bodies_by_url
    FIXTURE_PROJECT_IDS.to_h { |project_id| [detail_url(project_id), detail_body] }
  end

  def list_page_1_bodies_by_url
    { LIST_PAGE_1_URL => read_fixture(LIST_FIXTURE_NAME) }.merge(detail_bodies_by_url)
  end

  def build_source(fetcher, **options)
    SOURCE.new(fetcher: fetcher, today: TODAY, **options)
  end

  def list_urls_requested(fetcher)
    fetcher.requested_urls.reject { |url| url.match?(/project-\d+\z/) }
  end

  def detail_urls_requested(fetcher)
    fetcher.requested_urls.select { |url| url.match?(/project-\d+\z/) }
  end

  # 警告ログの内容を検証しつつ、テスト出力を汚さないためにloggerを一時的に差し替えて必ず戻す。
  def capturing_fetch_logs
    original_logger = FreelanceJobs.logger
    log_io = StringIO.new
    FreelanceJobs.instance_variable_set(:@logger, Logger.new(log_io))
    yield
    log_io.string
  ensure
    FreelanceJobs.instance_variable_set(:@logger, original_logger)
  end

  def test_fetch_gets_ten_cards_with_details_and_stops_at_empty_second_page
    fetcher = UrlMapFetcher.new(bodies_by_url: list_page_1_bodies_by_url)

    postings = build_source(fetcher).fetch

    assert_equal 10, postings.size
    assert_equal [LIST_PAGE_1_URL, LIST_PAGE_2_URL], list_urls_requested(fetcher),
                 "0件の2ページ目で打ち切り、3ページ目は取らないはず"
    assert_equal FIXTURE_PROJECT_IDS.map { |project_id| detail_url(project_id) }.sort,
                 detail_urls_requested(fetcher).sort, "カードごとに詳細を1回ずつ取るはず"
  end

  def test_fetch_enriches_every_posting_with_detail
    fetcher = UrlMapFetcher.new(bodies_by_url: list_page_1_bodies_by_url)

    postings = build_source(fetcher).fetch

    assert(postings.all? { |posting| posting.skills == %w[C# Laravel PHP Windows] })
    assert(postings.all? { |posting| posting.posted_on == Date.new(2026, 9, 9) })
    assert(postings.all? { |posting| posting.description.include?("求めるスキル:") })
  end

  def test_fetch_keeps_list_fields_for_the_first_posting
    fetcher = UrlMapFetcher.new(bodies_by_url: list_page_1_bodies_by_url)

    first = build_source(fetcher).fetch.first

    assert_equal FIRST_DETAIL_URL, first.url
    assert_equal "90万円／月", first.reward
    assert_equal "DYMテック", first.site
  end

  def test_fetch_requests_default_three_pages_and_deduplicates_across_pages
    bodies = list_page_1_bodies_by_url.merge(
      LIST_PAGE_2_URL => read_fixture(LIST_FIXTURE_NAME),
      LIST_PAGE_3_URL => read_fixture(LIST_FIXTURE_NAME)
    )
    fetcher = UrlMapFetcher.new(bodies_by_url: bodies)

    postings = build_source(fetcher).fetch

    assert_equal [LIST_PAGE_1_URL, LIST_PAGE_2_URL, LIST_PAGE_3_URL], list_urls_requested(fetcher)
    assert_equal 10, postings.size
    assert_equal 10, detail_urls_requested(fetcher).uniq.size
    assert_equal detail_urls_requested(fetcher).size, detail_urls_requested(fetcher).uniq.size,
                 "同じ案件の詳細を重複取得しないはず"
  end

  def test_fetch_respects_max_pages_option
    bodies = list_page_1_bodies_by_url.merge(LIST_PAGE_2_URL => read_fixture(LIST_FIXTURE_NAME))
    fetcher = UrlMapFetcher.new(bodies_by_url: bodies)

    build_source(fetcher, max_pages: 1).fetch

    assert_equal [LIST_PAGE_1_URL], list_urls_requested(fetcher)
  end

  # --- 詳細取得の失敗 ---

  def test_fetch_builds_list_only_posting_when_detail_fails_and_enriches_others
    failing_url = detail_url(3495)
    detail_error = FreelanceJobs::FetchError.new("HTTP 500 #{failing_url}")
    fetcher = UrlMapFetcher.new(bodies_by_url: list_page_1_bodies_by_url.merge(failing_url => detail_error))

    postings = nil
    capturing_fetch_logs { postings = build_source(fetcher).fetch }

    assert_equal 10, postings.size, "1件の詳細失敗で全体を落とさないはず"
    failed = postings.find { |posting| posting.url == failing_url }
    assert_equal [], failed.skills
    assert_nil failed.posted_on
    assert_equal "【週3～OK】ファッション系サブスクサービスのデザイン周りお任せします！", failed.title
    assert_equal "48万円／月", failed.reward
    assert_includes failed.description, "東京都"
    refute_includes failed.description, "求めるスキル"

    others = postings.reject { |posting| posting.url == failing_url }
    assert(others.all? { |posting| posting.skills == %w[C# Laravel PHP Windows] })
    assert(others.all? { |posting| posting.posted_on == Date.new(2026, 9, 9) })
  end

  def test_fetch_survives_non_fetch_error_on_detail
    failing_url = FIRST_DETAIL_URL
    fetcher = UrlMapFetcher.new(bodies_by_url: list_page_1_bodies_by_url.merge(failing_url => Timeout::Error.new("timed out")))

    postings = nil
    capturing_fetch_logs { postings = build_source(fetcher).fetch }

    assert_equal 10, postings.size
    assert_equal [], postings.find { |posting| posting.url == failing_url }.skills
  end

  def test_fetch_logs_a_warning_when_detail_fails
    failing_url = detail_url(3495)
    detail_error = FreelanceJobs::FetchError.new("HTTP 500 #{failing_url}")
    fetcher = UrlMapFetcher.new(bodies_by_url: list_page_1_bodies_by_url.merge(failing_url => detail_error))

    log_output = capturing_fetch_logs { build_source(fetcher).fetch }

    assert_includes log_output, "WARN"
    assert_includes log_output, "[FreelanceJobs::Sources::DymTech]"
    assert_includes log_output, "HTTP 500"
  end

  # アクセス制限は取り直しても解消しないため、詳細だけ握りつぶさず送出する（他ソースと同じ扱い）。
  def test_fetch_raises_when_access_is_blocked_on_detail
    blocked = FreelanceJobs::AccessBlockedError.new("アクセス制限")
    fetcher = UrlMapFetcher.new(bodies_by_url: list_page_1_bodies_by_url.merge(FIRST_DETAIL_URL => blocked))

    assert_raises(FreelanceJobs::AccessBlockedError) do
      capturing_fetch_logs { build_source(fetcher).fetch }
    end
  end
end
