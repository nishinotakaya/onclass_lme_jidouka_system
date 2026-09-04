# frozen_string_literal: true
# test/services/freelance_jobs/sources_shufti_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"
require "json"

class FreelanceJobsSourcesShuftiTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 4)

  def build_job(id: 1, name: "テスト案件", type: 1, price: "1000", unit_price: "100", period: "あと7日",
                job_hourly_wage: 400, job_tags: [], view_count: 5, continuous_order: false, client_name: "client_a")
    {
      "type" => "jobs", "id" => id,
      "attributes" => {
        "name" => name, "type" => type, "price" => price, "unit_price" => unit_price, "remaining_count" => nil,
        "period" => period, "job_hourly_wage" => job_hourly_wage, "limited" => false, "job_tags" => job_tags,
        "view_count" => view_count, "continuous_order" => continuous_order, "client_id" => 1,
        "client_name" => client_name, "client_hourly_wage" => 400
      }
    }
  end

  def build_body(jobs)
    JSON.generate({ "data" => jobs, "meta" => { "total" => jobs.size, "per_page" => 20, "current_page" => 1, "last_page" => 1 } })
  end

  def test_parse_fixture_returns_expected_count_and_first_fields
    body = read_fixture("shufti_api_p1.json")
    postings = FreelanceJobs::Sources::Shufti.parse(body, today: TODAY)

    assert_equal 20, postings.size

    first = postings.first
    assert_equal "https://app.shufti.jp/jobs/view/386479", first.url
    assert_equal "【完全在宅・未経験OK】商品のリサーチ・価格チェック♪長期継続あり", first.title
    assert_equal "1,000円（単価100円／想定時給400円）", first.reward
    assert_equal "プロジェクト", first.work_format
    assert_equal "シュフティ", first.site
    assert_equal "閲覧 7", first.application_status
    assert_instance_of Date, first.deadline_on
    assert_equal Date.new(2026, 9, 11), first.deadline_on
    assert_equal "あと7日（2026-09-11）", first.deadline_text
    assert_equal "ringodo", first.client
    assert_equal ["初心者歓迎", "マニュアルあり", "スキル不要", "継続発注あり"], first.tags
  end

  def test_reward_uses_three_digit_separators_for_all_three_amounts
    body = build_body([build_job(price: "12345", unit_price: "200", job_hourly_wage: 1500)])
    posting = FreelanceJobs::Sources::Shufti.parse(body, today: TODAY).first

    assert_equal "12,345円（単価200円／想定時給1,500円）", posting.reward
  end

  def test_work_format_type_2_is_task
    body = build_body([build_job(type: 2)])
    posting = FreelanceJobs::Sources::Shufti.parse(body, today: TODAY).first

    assert_equal "タスク", posting.work_format
  end

  def test_work_format_unknown_type_falls_back_to_generic_label
    body = build_body([build_job(type: 99)])
    posting = FreelanceJobs::Sources::Shufti.parse(body, today: TODAY).first

    assert_equal "形式99", posting.work_format
  end

  def test_deadline_hours_remaining_resolves_to_today
    body = build_body([build_job(period: "あと3時間")])
    posting = FreelanceJobs::Sources::Shufti.parse(body, today: TODAY).first

    assert_equal TODAY, posting.deadline_on
    assert_equal "あと3時間（2026-09-04）", posting.deadline_text
  end

  def test_deadline_blank_period_is_dash_with_nil_deadline_on
    body = build_body([build_job(period: "")])
    posting = FreelanceJobs::Sources::Shufti.parse(body, today: TODAY).first

    assert_nil posting.deadline_on
    assert_equal "-", posting.deadline_text
  end

  def test_tags_include_continuous_order_and_pr_prefix
    body = build_body([build_job(name: "【PR】高単価データ入力", job_tags: [{ "id" => 9, "name" => "データ入力" }],
                                  continuous_order: true)])
    posting = FreelanceJobs::Sources::Shufti.parse(body, today: TODAY).first

    assert_equal ["データ入力", "継続発注あり", "PR"], posting.tags
  end

  def test_parse_returns_empty_array_when_data_key_is_empty
    postings = FreelanceJobs::Sources::Shufti.parse(build_body([]), today: TODAY)

    assert_equal [], postings
  end
end
