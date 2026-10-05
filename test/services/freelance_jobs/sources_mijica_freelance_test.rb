# frozen_string_literal: true
# test/services/freelance_jobs/sources_mijica_freelance_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"
require "logger"
require "stringio"

class FreelanceJobsSourcesMijicaFreelanceTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 10, 5)
  FIXTURE_NAME = "mijica_ruby.html"
  SOURCE = FreelanceJobs::Sources::MijicaFreelance
  RUBY_URL = "https://mijica-job.com/jobs/skill-4"

  def parse_fixture(category_hint: "Ruby")
    SOURCE.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  # 警告ログを捕まえながらブロックを実行し、ログ文字列を返す。
  def capture_warnings
    original_logger = FreelanceJobs.instance_variable_get(:@logger)
    log_io = StringIO.new
    FreelanceJobs.instance_variable_set(:@logger, Logger.new(log_io))
    yield
    log_io.string
  ensure
    FreelanceJobs.instance_variable_set(:@logger, original_logger)
  end

  # --- 件数・1件目 ---

  def test_parse_fixture_returns_thirty_postings
    assert_equal 30, parse_fixture.size
  end

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "mijicaフリーランス", first.site
    assert_equal "https://mijica-job.com/jobs/detail/3433", first.url
    assert_equal "【Ruby on Rails/フルリモート/週5日】電子コミック配信サービス開発支援", first.title
    assert_equal "60〜75万円/月額", first.reward
    assert_equal %w[Ruby Ruby\ on\ Rails], first.skills
    assert_equal "リモートOK", first.work_format
    assert_equal "Ruby", first.category_hint
    assert_equal "-", first.application_status
    assert_nil first.client
    assert_nil first.posted_on
  end

  def test_parse_first_posting_tags_and_description
    first = parse_fixture.first

    assert_includes first.tags, "業務委託(フリーランス)"
    assert_includes first.tags, "面談1回"
    assert_includes first.description, "Ruby on Rails"
  end

  def test_first_posting_description_has_section_headings_and_body
    description = parse_fixture.first.description

    assert_includes description, "案件の内容: 電子コミック配信サービスを運営しているクライアント"
    assert_includes description, "求めるスキル:"
  end

  def test_description_falls_back_to_title_and_skills_without_sections
    body = "<html><body>#{build_card_html(numbers: ['60'])}<script>id_by_enterprise_id:11</script></body></html>"

    assert_equal "案件X", SOURCE.parse(body, today: TODAY).first.description
  end

  def test_work_format_is_default_when_not_remote
    assert_equal "要確認", parse_fixture.find { |posting| posting.work_format != "リモートOK" }.work_format
  end

  def test_urls_are_unique_detail_urls
    urls = parse_fixture.map(&:url)

    urls.each { |url| assert_match %r{\Ahttps://mijica-job\.com/jobs/detail/\d+\z}, url }
    assert_equal urls.uniq, urls
  end

  # --- 単価の組み立て（合成HTML） ---

  def build_card_html(numbers:, unit: "万円/月額")
    number_spans = numbers.map { |number| %(<span class="n">#{number}</span>) }.join('<span class="s">~</span>')
    <<~HTML
      <div class="cursor-pointer"><h3 class="fs-16">案件X<span>のフリーランス求人・案件</span></h3>
      <img alt="単価"><div><span><span>#{number_spans}</span></span><span>#{unit}</span></div>
      <img alt="契約形態"><span>業務委託</span></div>
    HTML
  end

  def test_single_number_reward
    body = "<html><body>#{build_card_html(numbers: ['60'])}<script>id_by_enterprise_id:11</script></body></html>"

    assert_equal "60万円/月額", SOURCE.parse(body, today: TODAY).first.reward
  end

  def test_reward_is_default_without_price_block
    body = '<html><body><div class="cursor-pointer"><h3 class="fs-16">案件X</h3></div><script>id_by_enterprise_id:11</script></body></html>'

    assert_equal "要確認", SOURCE.parse(body, today: TODAY).first.reward
  end

  # --- ID 件数不一致 ---

  def test_parse_skips_all_cards_and_warns_when_id_count_mismatches
    body = read_fixture(FIXTURE_NAME).sub(/id_by_enterprise_id:\d+/, "id_removed")
    postings = nil

    log = capture_warnings { postings = SOURCE.parse(body, today: TODAY) }

    assert_equal [], postings
    assert_includes log, "一致しない"
  end

  def test_parse_page_without_cards_returns_empty_array
    assert_equal [], SOURCE.parse("<html><body></body></html>", today: TODAY)
  end

  # --- 定数 ---

  def test_constants
    assert_equal "mijicaフリーランス", SOURCE::SITE_NAME
    assert_equal 1.5, SOURCE::REQUEST_INTERVAL
    assert_equal "https://mijica-job.com", SOURCE::BASE_URL
    assert_equal [
      { path: "/jobs/skill-4", hint: "Ruby" },
      { path: "/jobs/skill-379", hint: "TypeScript" },
      { path: "/jobs/skill-175", hint: "React" }
    ], SOURCE::DEFAULT_SEARCH_TARGETS
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

  def test_fetch_requests_one_page_per_target_without_query
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))

    SOURCE.new(fetcher: fetcher, today: TODAY).fetch

    assert_equal %w[https://mijica-job.com/jobs/skill-4 https://mijica-job.com/jobs/skill-379
                    https://mijica-job.com/jobs/skill-175], fetcher.requested_urls
  end

  def test_fetch_deduplicates_across_targets_and_keeps_first_hint
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))

    postings = SOURCE.new(fetcher: fetcher, today: TODAY).fetch

    assert_equal 30, postings.size
    assert(postings.all? { |posting| posting.category_hint == "Ruby" })
  end
end
