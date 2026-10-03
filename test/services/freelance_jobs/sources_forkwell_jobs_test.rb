# frozen_string_literal: true
# test/services/freelance_jobs/sources_forkwell_jobs_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesForkwellJobsTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 10, 3)
  FIXTURE_NAME = "forkwell_freelance.html"
  SOURCE = FreelanceJobs::Sources::ForkwellJobs
  FIRST_PAGE_URL = "https://jobs.forkwell.com/employment_types/freelance"
  SECOND_PAGE_URL = "https://jobs.forkwell.com/employment_types/freelance?page=2"

  def parse_fixture
    SOURCE.parse(read_fixture(FIXTURE_NAME), today: TODAY)
  end

  # 案件カード1件分のHTML片（実データのDOM構造を模したもの）。
  # 実HTMLでは案件カードは div.job-list > div.card > div.card-body。
  # reward_rows_html は報酬の li 群（年収・時給）を差し替えるための引数。
  def build_forkwell_card_html(href: "/sample-company/jobs/1", title: "テスト案件", client: "株式会社テスト",
                               tags: %w[ruby react], employment: "正社員,業務委託",
                               reward_rows_html: nil)
    reward_rows_html ||= <<~HTML
      <li class='list-inline-item'>
      <span class='text-muted'>
      年収
      </span>
      <strong class="text-jumped-larger">600</strong><span>万円</span><span>&nbsp;〜&nbsp;</span><strong class="text-jumped-larger">1,000</strong><span>万円</span>
      </li>
    HTML
    tag_items = tags.map { |tag| %(<li class='list-inline-item'><a class="tag" href="/t/#{tag}">#{tag}</a></li>) }.join
    <<~HTML
      <div class='card space-bottom-4'>
      <div class='card-body'>
      <div class='row'>
      <div class='col-md-3'>
      <a class="link-inherit" target="_blank" href="/sample-company"><div class='avatar'>
      <div class='avatar__body'>
      <div class='avatar__detail'>
      #{client}
      </div>
      </div>
      </div></a></div>
      <div class='col-md-9'>
      <h2 class='h4'>
      <a class="link-inherit job-list__link" target="_blank" href="#{href}"><span>#{title}</span>
      </a></h2>
      <ul class='list-inline space-bottom-1'>#{tag_items}</ul>
      <ul class='list-inline space-bottom-0'>
      <li class='list-inline-item'>
      <span class='text-muted'>
      雇用形態
      </span>
      #{employment}
      </li>
      <li class='list-inline-item'>
      <span class='text-muted'>
      <i class="fas fa-map-marker-alt"></i>
      </span>
      東京
      </li>
      </ul>
      <ul class='list-inline'>#{reward_rows_html}</ul>
      </div>
      </div>
      </div>
      </div>
    HTML
  end

  def wrap_forkwell_list(fragment)
    wrap_html(%(<div class="job-list">#{fragment}</div>))
  end

  def parse_fragment(fragment)
    SOURCE.parse(wrap_forkwell_list(fragment), today: TODAY)
  end

  # --- 件数（AC の「14件」は実HTMLと食い違う: 案件カードは13件、14個目の card は保存検索の枠） ---

  def test_parse_fixture_returns_thirteen_job_postings
    assert_equal 13, parse_fixture.size
  end

  def test_saved_search_card_without_job_link_is_ignored
    html = wrap_html(<<~HTML)
      <div class='card bg-light'><div class="card-body text-center">保存された検索条件はありません</div></div>
    HTML

    assert_equal [], SOURCE.parse(html, today: TODAY)
  end

  # --- 1件目の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "Forkwell Jobs", first.site
    assert_equal "https://jobs.forkwell.com/kencopa/jobs/34197", first.url
    assert_equal "【LLM×建設業界🦺×フルスタック】スピード感あふれるスタートアップで巨大産業を覆す課題解決に挑んでみませんか。自社開発プロダクトのフルスタックエンジニア募集！", first.title
    assert_equal "株式会社KENCOPA", first.client
    assert_nil first.category_hint, "言語絞り込みが無いのでhintはnilのはず"
    assert_equal "正社員,業務委託", first.work_format
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_nil first.posted_on, "一覧には「約1ヶ月前更新」の相対表記しか無いためnilのはず"
  end

  def test_parse_first_posting_skills_are_tag_slugs_kept_lowercase
    skills = parse_fixture.first.skills

    assert_equal %w[github slack python typescript terraform next.js docker dynamodb figma], skills.first(9)
    assert_includes skills, "react-hook-form"
    refute_includes skills, "…", "タグ省略表示の「…」はskillsに入れないはず"
    assert(skills.all? { |skill| skill == skill.downcase }, "分類器は大文字小文字を区別しないので小文字スラッグのままのはず")
  end

  def test_parse_first_posting_description_has_badges_and_location_but_not_title
    posting = parse_fixture.first
    description = posting.description

    assert_includes description, "Webエンジニア／プロダクトエンジニア"
    assert_includes description, "東京"
    assert_includes description, "一部リモート可"
    assert_includes description, "コードレビュー文化"
    assert_includes description, "正社員,業務委託"
    refute_includes description, "スピード感あふれるスタートアップ", "titleは重複させないはず"
    refute_match(/\n/, description, "normalize_descriptionで改行が畳まれているはず")
  end

  # --- 報酬: 時給があれば時給を優先、無ければ年収 ---

  def test_reward_prefers_hourly_rate_over_annual_salary
    reward = parse_fixture.first.reward

    assert_includes reward, "時給"
    assert_includes reward, "3,000"
    assert_includes reward, "6,000"
    refute_includes reward, "年収"
    refute_includes reward, "万円"
  end

  def test_reward_uses_hourly_rate_for_card_that_has_only_hourly_rate
    ispec = parse_fixture.find { |posting| posting.url == "https://jobs.forkwell.com/ispec/jobs/34171" }

    assert_includes ispec.reward, "時給"
    assert_includes ispec.reward, "3,750"
    assert_includes ispec.reward, "6,000"
  end

  # fixture には年収のみのカードが無いため、実カードを加工した最小HTMLで確認する。
  def test_reward_falls_back_to_annual_salary_when_no_hourly_rate
    reward = parse_fragment(build_forkwell_card_html).first.reward

    assert_includes reward, "年収"
    assert_includes reward, "600"
    assert_includes reward, "1,000"
    refute_includes reward, "時給"
  end

  def test_reward_is_placeholder_when_neither_hourly_nor_annual
    reward = parse_fragment(build_forkwell_card_html(reward_rows_html: "")).first.reward

    assert_equal "要確認", reward
  end

  # --- 雇用形態 → work_format ---

  def test_work_format_comes_from_employment_type_value
    work_formats = parse_fixture.map(&:work_format)

    assert_equal "業務委託", work_formats[1], "ispec は業務委託のみ"
    assert_equal "正社員,業務委託,その他", work_formats[10], "松尾研究所は3種"
    assert(work_formats.all? { |work_format| work_format.include?("業務委託") })
  end

  # --- 全件共通 ---

  def test_urls_are_absolute_job_urls
    parse_fixture.each do |posting|
      assert_match %r{\Ahttps://jobs\.forkwell\.com/[^/]+/jobs/\d+\z}, posting.url
    end
  end

  def test_every_posting_has_title_client_and_skills
    parse_fixture.each do |posting|
      refute_empty posting.title
      refute_empty posting.client
      refute_empty posting.skills, posting.url
    end
  end

  # 分類器は大文字小文字を区別しない(RUBY_RE は /i、TypeScript は (?i:)、React は /i)ため、
  # 小文字スラッグのskillsのままでRuby/TypeScript/Reactに分類できる。
  def test_lowercase_skills_are_classified_by_engineer_classifier
    postings = parse_fixture.to_h { |posting| [posting.url, posting] }
    ruby_posting = postings.fetch("https://jobs.forkwell.com/bcc-ltd/jobs/24999")
    react_posting = postings.fetch("https://jobs.forkwell.com/ispec/jobs/34171")

    assert_equal "Ruby", FreelanceJobs::EngineerClassifier.classify(ruby_posting, today: TODAY).category
    refute_nil FreelanceJobs::EngineerClassifier.classify(react_posting, today: TODAY).category
  end

  # --- 欠けたカード・重複 ---

  def test_parse_skips_cards_without_link_or_title
    fragment = <<~HTML
      <div class='card space-bottom-4'><div class='card-body'><h2>リンクなし</h2></div></div>
    HTML
    fragment += build_forkwell_card_html(title: "")

    assert_equal [], parse_fragment(fragment)
  end

  def test_parse_deduplicates_postings_with_the_same_url
    fragment = build_forkwell_card_html(href: "/x/jobs/1", title: "A") +
               build_forkwell_card_html(href: "/x/jobs/1", title: "B")

    postings = parse_fragment(fragment)

    assert_equal 1, postings.size
    assert_equal "A", postings.first.title
  end

  def test_parse_empty_page_returns_empty_array
    assert_equal [], parse_fragment("")
  end

  # --- 定数 ---

  def test_constants
    assert_equal "Forkwell Jobs", SOURCE::SITE_NAME
    assert_equal "https://jobs.forkwell.com", SOURCE::BASE_URL
    assert_equal 1.5, SOURCE::REQUEST_INTERVAL
    assert_equal 2, SOURCE::DEFAULT_MAX_PAGES
  end

  # --- fetch ---

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

  # URL -> body のHashで返すフェイク。未登録URLは空文字（0件ページ）。
  class UrlMapFetcher
    def initialize(bodies_by_url:)
      @bodies_by_url = bodies_by_url
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      @bodies_by_url.fetch(url, "")
    end
  end

  def test_fetch_builds_bare_first_page_url_and_page_query_for_second
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))

    SOURCE.new(fetcher: fetcher, today: TODAY).fetch

    assert_equal [FIRST_PAGE_URL, SECOND_PAGE_URL], fetcher.requested_urls
  end

  def test_fetch_respects_max_pages_option
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))

    SOURCE.new(fetcher: fetcher, today: TODAY, max_pages: 3).fetch

    assert_equal [FIRST_PAGE_URL, SECOND_PAGE_URL, "#{FIRST_PAGE_URL}?page=3"], fetcher.requested_urls
  end

  def test_fetch_never_uses_tag_filter_urls
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))

    SOURCE.new(fetcher: fetcher, today: TODAY, max_pages: 3).fetch

    refute(fetcher.requested_urls.any? { |url| url.include?("/t/") || url.include?("employment_types=") })
  end

  def test_fetch_stops_paging_when_a_page_has_no_cards
    fetcher = UrlMapFetcher.new(bodies_by_url: { FIRST_PAGE_URL => read_fixture(FIXTURE_NAME) })

    postings = SOURCE.new(fetcher: fetcher, today: TODAY, max_pages: 5).fetch

    assert_equal [FIRST_PAGE_URL, SECOND_PAGE_URL], fetcher.requested_urls, "0件の2ページ目で打ち切るはず"
    assert_equal 13, postings.size
  end

  def test_fetch_deduplicates_across_pages
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))

    postings = SOURCE.new(fetcher: fetcher, today: TODAY).fetch

    assert_equal 2, fetcher.requested_urls.size
    assert_equal 13, postings.size
  end
end
