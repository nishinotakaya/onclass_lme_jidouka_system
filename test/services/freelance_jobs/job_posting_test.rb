# frozen_string_literal: true
# test/services/freelance_jobs/job_posting_test.rb

require_relative "../../support/freelance_jobs_loader"

class FreelanceJobsJobPostingTest < Minitest::Test
  # --- normalize_url ---

  def test_normalize_url_forces_https_and_downcases_host
    assert_equal "https://example.com/path",
                 FreelanceJobs::JobPosting.normalize_url("http://Example.COM/path")
  end

  def test_normalize_url_strips_query_and_fragment
    assert_equal "https://example.com/path",
                 FreelanceJobs::JobPosting.normalize_url("https://example.com/path?foo=1&bar=2#section")
  end

  # --- 識別用クエリを残すホスト（求人ボックス経由の転載求人） ---

  def test_normalize_url_keeps_job_id_query_for_bizreach
    assert_equal "https://www.bizreach.jp/lp/members_register?job_id=9549222",
                 FreelanceJobs::JobPosting.normalize_url(
                   "https://www.bizreach.jp/lp/members_register/?job_id=9549222&job_category=1#top"
                 )
  end

  def test_normalize_url_keeps_jno_query_for_mynavi_agent
    assert_equal "https://mynavi-agent.jp/entry?jno=10750486",
                 FreelanceJobs::JobPosting.normalize_url("http://MYNAVI-AGENT.jp/entry/?jno=10750486&x=1")
  end

  def test_normalize_url_keeps_rid_query_for_aidea_career
    assert_equal "https://aidea-career.co.jp/recruitment_form_02?rid=22012491",
                 FreelanceJobs::JobPosting.normalize_url("https://aidea-career.co.jp/recruitment_form_02/?utm=1&rid=22012491")
  end

  def test_normalize_url_distinguishes_bizreach_urls_by_job_id
    first_key = FreelanceJobs::JobPosting.normalize_url("https://www.bizreach.jp/lp/members_register/?job_id=1")
    second_key = FreelanceJobs::JobPosting.normalize_url("https://www.bizreach.jp/lp/members_register/?job_id=2")

    refute_equal first_key, second_key
  end

  def test_normalize_url_drops_query_for_target_host_when_identifying_key_is_missing
    assert_equal "https://www.bizreach.jp/lp/members_register",
                 FreelanceJobs::JobPosting.normalize_url("https://www.bizreach.jp/lp/members_register/?job_category=1")
    assert_equal "https://www.bizreach.jp/lp/members_register",
                 FreelanceJobs::JobPosting.normalize_url("https://www.bizreach.jp/lp/members_register/")
  end

  def test_normalize_url_still_drops_job_id_query_for_other_hosts
    assert_equal "https://example.com/path",
                 FreelanceJobs::JobPosting.normalize_url("https://example.com/path?job_id=1&jno=2&rid=3")
  end

  def test_normalize_url_strips_trailing_slash
    assert_equal "https://example.com/path",
                 FreelanceJobs::JobPosting.normalize_url("https://example.com/path/")
  end

  def test_normalize_url_root_path_trailing_slash_is_stripped
    assert_equal "https://example.com",
                 FreelanceJobs::JobPosting.normalize_url("https://example.com/")
  end

  def test_normalize_url_equivalent_variants_produce_same_key
    canonical = FreelanceJobs::JobPosting.normalize_url("https://Example.com/work/detail/123")
    variants = [
      "http://example.com/work/detail/123",
      "https://EXAMPLE.COM/work/detail/123/",
      "https://example.com/work/detail/123?utm_source=x",
      "https://example.com/work/detail/123#top"
    ]

    variants.each do |variant|
      assert_equal canonical, FreelanceJobs::JobPosting.normalize_url(variant),
                   "expected #{variant.inspect} to normalize to #{canonical.inspect}"
    end
  end

  def test_normalize_url_blank_input_returns_empty_string
    assert_equal "", FreelanceJobs::JobPosting.normalize_url("")
    assert_equal "", FreelanceJobs::JobPosting.normalize_url(nil)
    assert_equal "", FreelanceJobs::JobPosting.normalize_url("   ")
  end

  def test_normalize_url_without_scheme_is_left_mostly_untouched
    assert_equal "example.com/foo", FreelanceJobs::JobPosting.normalize_url("example.com/foo/")
  end

  # --- normalize_description ---

  def test_normalize_description_collapses_whitespace_and_newlines
    raw = "行1\r\n\r\n行2\t\t行3   行4"
    assert_equal "行1 行2 行3 行4", FreelanceJobs::JobPosting.normalize_description(raw)
  end

  def test_normalize_description_replaces_urls_with_placeholder
    raw = "詳細はこちら https://example.com/foo?x=1 をご覧ください。参考: http://other.example/bar"
    assert_equal "詳細はこちら [url] をご覧ください。参考: [url]",
                 FreelanceJobs::JobPosting.normalize_description(raw)
  end

  def test_normalize_description_nil_returns_empty_string
    assert_equal "", FreelanceJobs::JobPosting.normalize_description(nil)
  end

  def test_normalize_description_strips_leading_and_trailing_space
    assert_equal "本文", FreelanceJobs::JobPosting.normalize_description("   本文   ")
  end

  # --- JobPosting struct itself ---

  def test_job_posting_is_keyword_init_struct_with_expected_members
    posting = FreelanceJobs::JobPosting.new(
      site: "CrowdWorks", url: "https://crowdworks.jp/public/jobs/1", title: "案件",
      description: "説明", category_hint: "HTML/CSS", reward: "要相談", work_format: "固定報酬制",
      application_status: "-", deadline_text: "-", deadline_on: nil, skills: ["HTML"],
      client: "client_a", tags: [], posted_on: nil
    )

    assert_equal "CrowdWorks", posting.site
    assert_equal ["HTML"], posting.skills
    assert_nil posting.deadline_on
  end

  # --- AC-02: closed? 判定 ---

  def build_posting(application_status:)
    FreelanceJobs::JobPosting.new(
      site: "CrowdWorks", url: "https://crowdworks.jp/public/jobs/1", title: "案件",
      description: "説明", category_hint: "HTML/CSS", reward: "要相談", work_format: "固定報酬制",
      application_status: application_status, deadline_text: "-", deadline_on: nil, skills: [],
      client: "", tags: [], posted_on: nil
    )
  end

  def test_closed_status_constant_is_boshuu_shuuryou
    assert_equal "募集終了", FreelanceJobs::JobPosting::CLOSED_STATUS
  end

  def test_closed_returns_true_when_application_status_exactly_matches_closed_status
    posting = build_posting(application_status: "募集終了")

    assert posting.closed?
  end

  def test_closed_returns_false_for_hyphen_status
    posting = build_posting(application_status: "-")

    refute posting.closed?
  end

  def test_closed_returns_false_for_open_status
    posting = build_posting(application_status: "募集中")

    refute posting.closed?
  end

  def test_closed_returns_false_for_nil_status
    posting = build_posting(application_status: nil)

    refute posting.closed?
  end

  def test_closed_returns_false_for_partial_match_status
    posting = build_posting(application_status: "募集終了しました")

    refute posting.closed?, "「募集終了」との完全一致でなければfalseのはず"
  end
end
