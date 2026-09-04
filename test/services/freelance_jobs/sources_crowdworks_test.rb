# frozen_string_literal: true
# test/services/freelance_jobs/sources_crowdworks_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesCrowdworksTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 4)

  def test_parse_cat16_html_css_category_returns_expected_count_and_first_fields
    body = read_fixture("cw_cat16.html")
    postings = FreelanceJobs::Sources::Crowdworks.parse(body, today: TODAY)

    assert_equal 14, postings.size

    first = postings.first
    assert_equal "https://crowdworks.jp/public/jobs/13426523", first.url
    assert_equal "ウェブサイトの新規コーディング", first.title
    assert_equal "100,000〜150,000円", first.reward
    assert_equal "固定報酬制", first.work_format
    assert_instance_of Date, first.deadline_on
    assert_equal Date.new(2026, 9, 17), first.deadline_on
    assert_equal "CrowdWorks", first.site
    assert_equal "HTML/CSS", first.category_hint
    assert_equal "応募 27件 / 契約 0/2人", first.application_status
    assert_equal "mhsc0803", first.client
  end

  def test_parse_cat249_p2_excel_category_and_hourly_and_pr_entries
    body = read_fixture("cw_cat249_p2.html")
    postings = FreelanceJobs::Sources::Crowdworks.parse(body, today: TODAY)

    # job_offers 50件 + pr_gold 1件 = 51件（同一URLの重複は無い）。
    assert_equal 51, postings.size

    first = postings.first
    assert_equal "https://crowdworks.jp/public/jobs/13426451", first.url
    assert_equal "Excel・スプレッドシート", first.category_hint
    assert_equal "0〜5,000円", first.reward
    assert_match(/\A[\d,]+〜[\d,]+円\z/, first.reward, "3桁区切りの金額表記になっていること")
    assert_equal "固定報酬制", first.work_format

    pr_posting = postings.find { |posting| posting.tags.include?("PR") }
    refute_nil pr_posting, "pr_gold由来のPR案件が1件含まれるはず"

    hourly_posting = postings.find { |posting| posting.work_format == "時間単価制" }
    refute_nil hourly_posting, "hourly_paymentの案件が含まれるはず"
    assert_match(/\A時給 /, hourly_posting.reward)
  end

  def test_parse_raises_fetch_error_when_vue_container_missing
    error = assert_raises(FreelanceJobs::FetchError) do
      FreelanceJobs::Sources::Crowdworks.parse("<html><body>no container here</body></html>", today: TODAY)
    end
    assert_includes error.message, "vue-container"
  end

  def test_parse_deduplicates_by_url_within_source
    entry = build_crowdworks_job_offer(id: 42, title: "重複テスト案件")
    search_result = { "job_offers" => [entry], "pr_gold" => [entry], "page" => { "total_page" => 1 } }
    body = build_crowdworks_body(search_result)

    postings = FreelanceJobs::Sources::Crowdworks.parse(body, today: TODAY)

    assert_equal 1, postings.size
  end

  # --- ラウンド2 C6: paymentキー => 形式表示名（生のキー名をそのまま出さない） ---

  def test_payment_work_format_maps_competition_payment_to_competition_label
    entry = build_crowdworks_job_offer(id: 100, payment: { "competition_payment" => { "first_prize" => 30_000 } })
    body = build_crowdworks_body({ "job_offers" => [entry], "page" => { "total_page" => 1 } })

    posting = FreelanceJobs::Sources::Crowdworks.parse(body, today: TODAY).first

    assert_equal "コンペ", posting.work_format
    refute_equal "competition_payment", posting.work_format
  end

  def test_payment_work_format_maps_task_payment_to_task_label
    entry = build_crowdworks_job_offer(id: 101, payment: { "task_payment" => { "unit_price" => 500 } })
    body = build_crowdworks_body({ "job_offers" => [entry], "page" => { "total_page" => 1 } })

    posting = FreelanceJobs::Sources::Crowdworks.parse(body, today: TODAY).first

    assert_equal "タスク", posting.work_format
  end

  def test_payment_work_format_unknown_key_falls_back_to_other_label_not_raw_key
    entry = build_crowdworks_job_offer(id: 102, payment: { "some_future_payment_type" => {} })
    body = build_crowdworks_body({ "job_offers" => [entry], "page" => { "total_page" => 1 } })

    posting = FreelanceJobs::Sources::Crowdworks.parse(body, today: TODAY).first

    assert_equal "その他", posting.work_format
    refute_equal "some_future_payment_type", posting.work_format
  end

  def test_reward_and_work_format_for_hourly_payment
    entry = build_crowdworks_job_offer(id: 103, payment: { "hourly_payment" => { "min_hourly_wage" => 1500, "max_hourly_wage" => 2000 } })
    body = build_crowdworks_body({ "job_offers" => [entry], "page" => { "total_page" => 1 } })

    posting = FreelanceJobs::Sources::Crowdworks.parse(body, today: TODAY).first

    assert_equal "時間単価制", posting.work_format
    assert_equal "時給 1,500〜2,000円", posting.reward
  end

  def test_reward_is_negotiable_when_both_budgets_are_nil
    entry = build_crowdworks_job_offer(id: 104, payment: { "fixed_price_payment" => { "min_budget" => nil, "max_budget" => nil } })
    body = build_crowdworks_body({ "job_offers" => [entry], "page" => { "total_page" => 1 } })

    posting = FreelanceJobs::Sources::Crowdworks.parse(body, today: TODAY).first

    assert_equal "要相談", posting.reward
  end

  # === D2: search_targets（キーワード検索）による初期化・fetch ===

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

  def test_fetch_with_keyword_target_requests_search_url_and_parses_fixture
    expected_url = "https://crowdworks.jp/public/jobs/search?search%5Bkeywords%5D=Ruby&order=new&hide_expired=true&page=1"
    fetcher = RecordingFetcher.new(body_by_url: { expected_url => read_fixture("cw_search_ruby.html") })
    source = FreelanceJobs::Sources::Crowdworks.new(fetcher: fetcher, today: TODAY,
                                                     search_targets: [{ keyword: "Ruby", hint: "Ruby", max_page: 2 }])

    postings = source.fetch

    assert_equal [expected_url], fetcher.requested_urls,
                 "total_page=1のfixtureなので2ページ目は要求されない（max_page:2でも打ち切られる）"
    assert_equal 14, postings.size
  end

  def test_fetch_with_keyword_target_sets_category_hint_from_target_hint_not_from_categories_table
    expected_url = "https://crowdworks.jp/public/jobs/search?search%5Bkeywords%5D=Ruby&order=new&hide_expired=true&page=1"
    fetcher = RecordingFetcher.new(body_by_url: { expected_url => read_fixture("cw_search_ruby.html") })
    source = FreelanceJobs::Sources::Crowdworks.new(fetcher: fetcher, today: TODAY,
                                                     search_targets: [{ keyword: "Ruby", hint: "Ruby", max_page: 2 }])

    postings = source.fetch

    refute_empty postings
    assert(postings.all? { |posting| posting.category_hint == "Ruby" },
           "fixtureのcategory_idはCATEGORIESに存在しないため、target由来のhintにフォールバックする想定")
  end

  def test_fetch_with_multiple_keyword_targets_requests_each_keywords_url
    ruby_url = "https://crowdworks.jp/public/jobs/search?search%5Bkeywords%5D=Ruby&order=new&hide_expired=true&page=1"
    typescript_url = "https://crowdworks.jp/public/jobs/search?search%5Bkeywords%5D=TypeScript&order=new&hide_expired=true&page=1"
    empty_body = build_crowdworks_body({ "job_offers" => [], "page" => { "total_page" => 1 } })
    fetcher = RecordingFetcher.new(body_by_url: { ruby_url => empty_body, typescript_url => empty_body })
    source = FreelanceJobs::Sources::Crowdworks.new(
      fetcher: fetcher, today: TODAY,
      search_targets: [{ keyword: "Ruby", hint: "Ruby", max_page: 1 }, { keyword: "TypeScript", hint: "TypeScript", max_page: 1 }]
    )

    source.fetch

    assert_equal [ruby_url, typescript_url], fetcher.requested_urls
  end

  def test_fetch_with_category_target_requests_category_url_as_before
    category_url = "https://crowdworks.jp/public/jobs/category/16?order=new&hide_expired=true&page=1"
    fetcher = RecordingFetcher.new(body_by_url: { category_url => read_fixture("cw_cat16.html") })
    source = FreelanceJobs::Sources::Crowdworks.new(fetcher: fetcher, today: TODAY,
                                                     search_targets: [{ category_id: 16, hint: "HTML/CSS", max_page: 1 }])

    postings = source.fetch

    assert_equal [category_url], fetcher.requested_urls, "category_idを持つtargetは従来通りカテゴリURLを組み立てる"
    assert_equal 14, postings.size
  end
end
