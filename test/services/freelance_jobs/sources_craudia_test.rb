# frozen_string_literal: true
# test/services/freelance_jobs/sources_craudia_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesCraudiaTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 4)

  def test_parse_fixture_returns_expected_count_and_first_fields
    body = read_fixture("craudia_list.html")
    postings = FreelanceJobs::Sources::Craudia.parse(body, today: TODAY)

    # fixtureは16アイテム中1件が空アイテム（href=""）のため15件になる。
    assert_equal 15, postings.size

    first = postings.first
    assert_equal "https://www.craudia.com/work_detail/ccq9106", first.url
    assert_equal "【長期】SNSを活用した「営業アシスタント」を募集！月収10万以上可★【完全在宅ワーク】", first.title
    assert_equal "1,100円", first.reward
    assert_equal "時間制", first.work_format
    assert_equal "クラウディア", first.site
    assert_equal "参加申請数2件", first.application_status
    assert_instance_of Date, first.deadline_on
    assert_equal Date.new(2026, 9, 30), first.deadline_on
    assert_equal "あと26日（2026-09-30）", first.deadline_text
  end

  # --- 除外ルール: 空アイテム（href=""）は除外される ---

  def test_parse_excludes_blank_href_placeholder_item
    body = read_fixture("craudia_list.html")
    postings = FreelanceJobs::Sources::Craudia.parse(body, today: TODAY)

    refute(postings.any? { |posting| posting.url == "https://www.craudia.com" || posting.title.to_s.empty? },
           "href=\"\"の空アイテムは除外されるはず")
  end

  def test_parse_skips_items_without_title_link
    fragment = <<~HTML
      <div class="work-list__item-inner">
        <div class="work-list__detail">
          <div class="work-list__data">
            <div class="work-list__reward">1,000円</div>
          </div>
        </div>
      </div>
    HTML
    postings = FreelanceJobs::Sources::Craudia.parse(wrap_html(fragment), today: TODAY)

    assert_equal [], postings
  end

  def test_deadline_is_dash_and_nil_when_recruitment_period_missing
    item_html = build_craudia_item_html(href: "/work_detail/nodeadline", title: "締切情報なしの案件")
    # 募集期間のdiv自体を取り除く（"あと N 日"を含まない状態を作る）。
    require "nokogiri"
    doc = Nokogiri::HTML(wrap_html(item_html))
    doc.css(".work-list__status > div").each { |node| node.remove if node.text.include?("募集期間") }

    posting = FreelanceJobs::Sources::Craudia.parse(doc.to_html, today: TODAY).first

    assert_nil posting.deadline_on
    assert_equal "-", posting.deadline_text
  end

  def test_application_status_falls_back_to_dash_when_no_recognized_label
    item_html = build_craudia_item_html(href: "/work_detail/nostatus", title: "参加申請数の記載が無い案件")
    require "nokogiri"
    doc = Nokogiri::HTML(wrap_html(item_html))
    doc.css(".work-list__status > div").each { |node| node.remove if node.text.include?("参加申請数") }

    posting = FreelanceJobs::Sources::Craudia.parse(doc.to_html, today: TODAY).first

    assert_equal "-", posting.application_status
  end

  def test_parse_returns_empty_array_when_no_items_present
    postings = FreelanceJobs::Sources::Craudia.parse("<html><body>該当なし</body></html>", today: TODAY)

    assert_equal [], postings
  end
end
