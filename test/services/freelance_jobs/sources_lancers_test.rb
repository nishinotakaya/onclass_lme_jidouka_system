# frozen_string_literal: true
# test/services/freelance_jobs/sources_lancers_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesLancersTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 4)

  def test_parse_task_input_fixture_returns_expected_count_and_first_fields
    body = read_fixture("ln_cat_task_input.html")
    postings = FreelanceJobs::Sources::Lancers.parse(body, today: TODAY)

    assert_equal 5, postings.size

    first = postings.first
    assert_equal "https://www.lancers.jp/work/detail/5596677", first.url
    assert_equal "【隙間時間で簡単作業！】バイマで出品作業して頂ける方募集◎", first.title
    assert_equal "20,000円〜50,000円／固定", first.reward
    assert_equal "プロジェクト", first.work_format
    assert_equal "ランサーズ", first.site
    assert_equal "0/5人", first.application_status
  end

  def test_parse_excel_fixture_excludes_non_detail_hrefs_and_dedupes_within_source
    body = read_fixture("ln_excel.html")
    postings = FreelanceJobs::Sources::Lancers.parse(body, today: TODAY)

    # 固定5件のうち「求人」(tech-agent.lancers.jp) を除外し、index0の重複(index16)も統合すると3件になる。
    assert_equal 3, postings.size
    refute(postings.any? { |posting| posting.url.include?("tech-agent.lancers.jp") },
           "/work/detail/以外のhref（求人枠）は除外されるべき")

    competition = postings.find { |posting| posting.work_format == "コンペ" }
    refute_nil competition
    assert_equal "33,000円", competition.reward
    assert_equal "提案 4件", competition.application_status
    assert_equal Date.new(2026, 9, 13), competition.deadline_on
    assert_equal "あと9日（2026-09-13）", competition.deadline_text
  end

  def test_parse_returns_empty_array_when_no_cards_present
    postings = FreelanceJobs::Sources::Lancers.parse("<html><body>案件はありません</body></html>", today: TODAY)

    assert_equal [], postings
  end

  # --- ラウンド2 C6: 形式バッジをCrowdWorks語彙に揃える ---

  def test_work_format_normalizes_fixed_price_badge
    body = wrap_html(build_lancers_card_html(job_id: 1, title: "固定報酬バッジの案件", badge_text: "固定報酬"))
    posting = FreelanceJobs::Sources::Lancers.parse(body, today: TODAY).first

    assert_equal "固定報酬制", posting.work_format
  end

  def test_work_format_normalizes_hourly_badge
    body = wrap_html(build_lancers_card_html(job_id: 2, title: "時間単価バッジの案件", badge_text: "時間単価"))
    posting = FreelanceJobs::Sources::Lancers.parse(body, today: TODAY).first

    assert_equal "時間単価制", posting.work_format
  end

  def test_work_format_leaves_unmapped_badge_untouched
    body = wrap_html(build_lancers_card_html(job_id: 3, title: "タスク案件", badge_text: "タスク"))
    posting = FreelanceJobs::Sources::Lancers.parse(body, today: TODAY).first

    assert_equal "タスク", posting.work_format
  end

  def test_work_format_is_dash_when_badge_missing
    fragment = <<~HTML
      <div class="p-search-job-media">
        <a class="p-search-job-media__title" href="/work/detail/999999">バッジ無し案件</a>
      </div>
    HTML
    posting = FreelanceJobs::Sources::Lancers.parse(wrap_html(fragment), today: TODAY).first

    assert_equal "-", posting.work_format
  end

  # --- ラウンド2 C7: 「本日締切」に絶対日付を併記する ---

  def test_deadline_today_includes_absolute_date_text
    body = wrap_html(build_lancers_card_html(job_id: 4, title: "本日締切の案件", remaining_text: "本日締切"))
    posting = FreelanceJobs::Sources::Lancers.parse(body, today: TODAY).first

    assert_equal TODAY, posting.deadline_on
    assert_equal "本日締切（2026-09-04）", posting.deadline_text
  end

  # === D2: initialize のオプション（fixed_paths / keywords）が fetch に反映される ===

  # urlをそのままキーに本文を返すFakeフェッチャー（呼び出されたURLを記録する）。
  class RecordingFetcher
    def initialize(body_by_url:)
      @body_by_url = body_by_url
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      @body_by_url.fetch(url) { raise "no fixture stubbed for #{url}" }
    end
  end

  def test_fetch_with_empty_fixed_paths_and_custom_keywords_requests_only_keyword_urls
    empty_body = "<html><body></body></html>"
    expected_urls = ["Ruby", "TypeScript", "React"].map { |keyword| "https://www.lancers.jp/work/search?keyword=#{CGI.escape(keyword)}&open=1&sort=started" }
    fetcher = RecordingFetcher.new(body_by_url: expected_urls.to_h { |url| [url, empty_body] })
    source = FreelanceJobs::Sources::Lancers.new(fetcher: fetcher, today: TODAY, fixed_paths: [],
                                                  keywords: ["Ruby", "TypeScript", "React"])

    source.fetch

    assert_equal expected_urls, fetcher.requested_urls, "fixed_paths: []なら固定パスへのリクエストは発生しない"
  end

  def test_fetch_with_default_options_requests_fixed_paths_then_default_keywords
    fixed_url_1 = "https://www.lancers.jp/work/search/task/input?open=1&sort=started"
    fixed_url_2 = "https://www.lancers.jp/work/search/task?open=1&sort=started"
    keyword_urls = FreelanceJobs::Sources::Lancers::KEYWORDS.map do |keyword|
      "https://www.lancers.jp/work/search?keyword=#{CGI.escape(keyword)}&open=1&sort=started"
    end
    empty_body = "<html><body></body></html>"
    all_urls = [fixed_url_1, fixed_url_2] + keyword_urls
    fetcher = RecordingFetcher.new(body_by_url: all_urls.to_h { |url| [url, empty_body] })
    source = FreelanceJobs::Sources::Lancers.new(fetcher: fetcher, today: TODAY)

    source.fetch

    assert_equal all_urls, fetcher.requested_urls, "オプション省略時は既定のFIXED_PATHS→KEYWORDSの順で従来通りリクエストする"
  end
end
