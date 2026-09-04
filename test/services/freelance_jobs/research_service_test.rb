# frozen_string_literal: true
# test/services/freelance_jobs/research_service_test.rb
#
# Fake fetcher（サイトのホスト名でルーティングし、fixtureの本文を返す）と
# Fake sheets client（read_values/replace_sheetの呼び出しを記録するだけ）を使い、
# ネットワーク・スプレッドシートへの実アクセスを一切行わずにオーケストレーションを検証する。

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsResearchServiceTest < Minitest::Test
  include FreelanceJobsTestHelpers

  NOW = Time.new(2026, 9, 4, 6, 5, 0, "+09:00")

  # url のホスト名で振り分けて本文を返すFakeフェッチャー。
  # raising_hosts に含まれるホストへのリクエストはFetchErrorを送出する。
  class RoutingFakeFetcher
    def initialize(routes:, raising_hosts: [])
      @routes = routes
      @raising_hosts = raising_hosts
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      matched_host = @routes.keys.find { |host| url.include?(host) }
      raise FreelanceJobs::FetchError, "stubbed failure for #{url}" if matched_host && @raising_hosts.include?(matched_host)
      raise "no fixture route stubbed for #{url}" unless matched_host

      @routes.fetch(matched_host)
    end
  end

  class AlwaysFailingFetcher
    def get(_url, headers: {})
      raise FreelanceJobs::FetchError, "network is unreachable"
    end
  end

  class FakeSheetsClient
    attr_reader :replace_sheet_calls, :read_values_calls

    def initialize(existing_values:)
      @existing_values = existing_values
      @replace_sheet_calls = []
      @read_values_calls = []
    end

    def read_values(range)
      @read_values_calls << range
      @existing_values
    end

    def replace_sheet(**kwargs)
      @replace_sheet_calls << kwargs
    end

    def backup_sheet_name
      "_backup_gid0"
    end
  end

  def real_fixture_routes
    {
      "crowdworks.jp" => read_fixture("cw_cat16.html"),
      "lancers.jp" => read_fixture("ln_cat_task_input.html"),
      "coconala.com" => read_fixture("coconala_input.html"),
      "shufti.jp" => read_fixture("shufti_api_p1.json"),
      "mamaworks.jp" => read_fixture("mama_jobs_noquery.html"),
      "craudia.com" => read_fixture("craudia_list.html")
    }
  end

  def header_only_existing_values
    [FreelanceJobs::RowBuilder::HEADER]
  end

  # --- (a) 正常系: replace_sheetが1回呼ばれ、banner_textに最終実行・成功6/6が含まれる ---

  def test_call_success_path_replaces_sheet_once_with_expected_banner
    fetcher = RoutingFakeFetcher.new(routes: real_fixture_routes)
    sheets_client = FakeSheetsClient.new(existing_values: header_only_existing_values)

    service = FreelanceJobs::ResearchService.new(profile: FreelanceJobs::Profile::BEGINNER,
                                                  run_window_label: "毎朝 06:00〜06:30（日本時間）",
                                                  fetcher: fetcher, sheets_client: sheets_client, now: NOW)
    summary = service.call

    refute summary[:aborted]
    assert_equal 1, sheets_client.replace_sheet_calls.size

    banner_text = sheets_client.replace_sheet_calls.first[:banner_text]
    assert_includes banner_text, "最終実行 2026-09-04 06:05"
    assert_includes banner_text, "成功 6/6"
    assert_equal 6, summary[:succeeded_sites].size
    assert_equal [], summary[:failures]
    assert_operator summary[:candidates], :>, 0
  end

  # --- (b) 全サイト失敗: abortedかつreplace_sheet未呼び出し ---

  def test_call_aborts_when_all_sources_fail
    sheets_client = FakeSheetsClient.new(existing_values: header_only_existing_values)
    service = FreelanceJobs::ResearchService.new(profile: FreelanceJobs::Profile::BEGINNER,
                                                  run_window_label: "毎朝 06:00〜06:30（日本時間）",
                                                  fetcher: AlwaysFailingFetcher.new, sheets_client: sheets_client, now: NOW)

    summary = service.call

    assert summary[:aborted]
    assert_equal "取得に成功したサイトがありません", summary[:reason]
    assert_equal [], sheets_client.replace_sheet_calls
    assert_equal [], sheets_client.read_values_calls, "全滅時はシート読み取りにも進まない"
    assert_equal 6, summary[:failures].size
  end

  # --- (c) ヘッダー未検出: aborted ---

  def test_call_aborts_when_existing_header_row_is_not_found
    fetcher = RoutingFakeFetcher.new(routes: real_fixture_routes)
    sheets_client = FakeSheetsClient.new(existing_values: [["古いA列", "古いB列"], ["データ", "データ"]])

    service = FreelanceJobs::ResearchService.new(profile: FreelanceJobs::Profile::BEGINNER,
                                                  run_window_label: "毎朝 06:00〜06:30（日本時間）",
                                                  fetcher: fetcher, sheets_client: sheets_client, now: NOW)
    summary = service.call

    assert summary[:aborted]
    assert_includes summary[:reason], "🌟おすすめ"
    assert_equal [], sheets_client.replace_sheet_calls
  end

  # --- (d) 1サイトだけ失敗しても続行し、bannerに⚠ 取得失敗が入る ---

  def test_call_continues_when_only_one_source_fails
    fetcher = RoutingFakeFetcher.new(routes: real_fixture_routes, raising_hosts: ["craudia.com"])
    sheets_client = FakeSheetsClient.new(existing_values: header_only_existing_values)

    service = FreelanceJobs::ResearchService.new(profile: FreelanceJobs::Profile::BEGINNER,
                                                  run_window_label: "毎朝 06:00〜06:30（日本時間）",
                                                  fetcher: fetcher, sheets_client: sheets_client, now: NOW)
    summary = service.call

    refute summary[:aborted]
    assert_equal 1, summary[:failures].size
    assert_equal 5, summary[:succeeded_sites].size
    refute_includes summary[:succeeded_sites], "クラウディア"

    banner_text = sheets_client.replace_sheet_calls.first[:banner_text]
    assert_includes banner_text, "⚠ 取得失敗"
    assert_includes banner_text, "クラウディア"
    assert_includes banner_text, "成功 5/6"
  end

  # === ラウンド2 C2: 新規追加は🌟付きだけ。既存URL一致行は🌟の有無に関わらず更新対象 ===

  def test_call_drops_unstarred_new_candidates_but_keeps_starred_and_existing_matches
    starred_new_href = "/work_detail/starred-new"
    unstarred_new_href = "/work_detail/unstarred-new"
    existing_match_href = "/work_detail/existing-match"

    craudia_body = wrap_html(
      build_craudia_item_html(href: starred_new_href, title: "未経験歓迎のHTMLコーディング作業", deadline_days: 10) +
      build_craudia_item_html(href: unstarred_new_href, title: "エクセルでの集計業務", deadline_days: 10) +
      build_craudia_item_html(href: existing_match_href, title: "VBAマクロでの集計業務（経験者向け）", deadline_days: 10)
    )

    empty_routes = {
      "crowdworks.jp" => build_crowdworks_body({ "job_offers" => [], "page" => { "total_page" => 1 } }),
      "lancers.jp" => "<html><body></body></html>",
      "coconala.com" => "<html><body></body></html>",
      "shufti.jp" => '{"data":[],"meta":{}}',
      "mamaworks.jp" => "<html><body></body></html>",
      "craudia.com" => craudia_body
    }
    fetcher = RoutingFakeFetcher.new(routes: empty_routes)

    existing_url = FreelanceJobs::JobPosting.normalize_url("https://www.craudia.com#{existing_match_href}")
    existing_row = Array.new(15, "")
    existing_row[0] = "" # A: 🌟(手入力・空)
    existing_row[2] = "Excel・スプレッドシート" # C: 分類(手入力)
    existing_row[3] = "既存の案件名(手入力保持)" # D
    existing_row[4] = "クラウディア" # E
    existing_row[5] = existing_url # F
    existing_row[12] = "2020-01-01" # M: 締切(古い値、更新される想定)
    existing_row[14] = "2026-08-01 00:00" # O: 取得日時(更新される想定)
    sheets_client = FakeSheetsClient.new(existing_values: [FreelanceJobs::RowBuilder::HEADER, existing_row])

    service = FreelanceJobs::ResearchService.new(profile: FreelanceJobs::Profile::BEGINNER,
                                                  run_window_label: "毎朝 06:00〜06:30（日本時間）",
                                                  fetcher: fetcher, sheets_client: sheets_client, now: NOW)
    summary = service.call

    refute summary[:aborted]
    assert_equal 3, summary[:candidates], "3件とも分類はされる（フィルタ前）"
    assert_equal 1, summary[:starred_candidates], "🌟が付くのはHTML/CSS未経験向けの1件だけ"
    assert_equal 1, summary[:added], "新規で追加されるのは🌟付きの1件だけ"
    assert_equal 1, summary[:updated], "既存URL一致の1件は🌟が無くても更新される"

    written_rows = sheets_client.replace_sheet_calls.first[:rows]
    written_urls = written_rows.map { |row| row[5] }
    assert_includes written_urls, FreelanceJobs::JobPosting.normalize_url("https://www.craudia.com#{starred_new_href}")
    assert_includes written_urls, existing_url
    refute_includes written_urls, FreelanceJobs::JobPosting.normalize_url("https://www.craudia.com#{unstarred_new_href}")

    updated_existing_row = written_rows.find { |row| row[5] == existing_url }
    assert_equal "既存の案件名(手入力保持)", updated_existing_row[3], "D列(手入力)は保持される"
  end

  # === D1: ランサーズ本番対象外（WAF CAPTCHA）対応: excluded_sites ===

  def real_fixture_routes_excluding_lancers
    real_fixture_routes.reject { |host, _| host == "lancers.jp" }
  end

  def test_call_with_excluded_sites_never_requests_the_excluded_site
    fetcher = RoutingFakeFetcher.new(routes: real_fixture_routes_excluding_lancers)
    sheets_client = FakeSheetsClient.new(existing_values: header_only_existing_values)

    service = FreelanceJobs::ResearchService.new(profile: FreelanceJobs::Profile::BEGINNER,
                                                  run_window_label: "毎朝 06:00〜06:30（日本時間）",
                                                  excluded_sites: ["ランサーズ"],
                                                  fetcher: fetcher, sheets_client: sheets_client, now: NOW)
    summary = service.call

    refute summary[:aborted]
    refute fetcher.requested_urls.any? { |requested_url| requested_url.include?("lancers.jp") },
           "対象外サイトへのリクエストは1件も発生しない想定"
    assert_equal 5, summary[:succeeded_sites].size
    assert_equal ["ランサーズ"], summary[:excluded_sites]
  end

  def test_call_with_excluded_sites_banner_shows_source_count_and_excluded_notice
    fetcher = RoutingFakeFetcher.new(routes: real_fixture_routes_excluding_lancers)
    sheets_client = FakeSheetsClient.new(existing_values: header_only_existing_values)

    service = FreelanceJobs::ResearchService.new(profile: FreelanceJobs::Profile::BEGINNER,
                                                  run_window_label: "毎朝 06:00〜06:30（日本時間）",
                                                  excluded_sites: ["ランサーズ"],
                                                  fetcher: fetcher, sheets_client: sheets_client, now: NOW)
    summary = service.call

    refute summary[:aborted]
    banner_text = sheets_client.replace_sheet_calls.first[:banner_text]
    assert_includes banner_text, "取得元 5サイト（成功 5/5）"
    assert_includes banner_text, "対象外: ランサーズ（アクセス制限のため自動取得できません）"
  end

  def test_excluded_sites_from_env_splits_on_full_width_and_half_width_comma_and_trims_whitespace
    parsed = FreelanceJobs::ResearchService.excluded_sites_from_env("ランサーズ、 ココナラ（公開依頼）")

    assert_equal 2, parsed.size
    assert_equal ["ランサーズ", "ココナラ（公開依頼）"], parsed
  end

  def test_excluded_sites_from_env_returns_empty_array_for_blank_value
    assert_equal [], FreelanceJobs::ResearchService.excluded_sites_from_env("")
  end

  def test_call_failure_message_omits_error_class_name
    fetcher = RoutingFakeFetcher.new(routes: real_fixture_routes, raising_hosts: ["craudia.com"])
    sheets_client = FakeSheetsClient.new(existing_values: header_only_existing_values)

    service = FreelanceJobs::ResearchService.new(profile: FreelanceJobs::Profile::BEGINNER,
                                                  run_window_label: "毎朝 06:00〜06:30（日本時間）",
                                                  fetcher: fetcher, sheets_client: sheets_client, now: NOW)
    summary = service.call

    assert_equal 1, summary[:failures].size
    failure_message = summary[:failures].first
    assert failure_message.start_with?("クラウディア（"), "「サイト名（メッセージ）」形式である想定: #{failure_message}"
    assert failure_message.end_with?("）"), "全角カッコで閉じる想定: #{failure_message}"
    refute_includes failure_message, "FetchError", "クラス名はバナーの失敗メッセージに出さない設計"
  end

  def test_call_continues_when_excluded_sites_has_unknown_site_name
    fetcher = RoutingFakeFetcher.new(routes: real_fixture_routes)
    sheets_client = FakeSheetsClient.new(existing_values: header_only_existing_values)

    service = FreelanceJobs::ResearchService.new(profile: FreelanceJobs::Profile::BEGINNER,
                                                  run_window_label: "毎朝 06:00〜06:30（日本時間）",
                                                  excluded_sites: ["存在しないサイト"],
                                                  fetcher: fetcher, sheets_client: sheets_client, now: NOW)
    summary = service.call

    refute summary[:aborted]
    assert_equal 6, summary[:succeeded_sites].size, "未知のサイト名は実在するどのサイトも除外しない"
    assert_equal [], summary[:excluded_sites], "実在しないサイト名は対象外一覧に含めない"
    refute_includes sheets_client.replace_sheet_calls.last[:banner_text], "存在しないサイト"
  end

  # === D2: engineerプロファイル ===

  # CrowdWorksだけ実データfixture（cw_search_ruby.html）を返し、他5サイトは空データを返す。
  # cw_search_ruby.htmlの14件中13件がRubyに分類され、うち1件は🌟が付かない（recommend=""）。
  # new_rows_require_star:falseのengineerではこの🌟無し行も新規追加される想定。
  def engineer_fixture_routes
    {
      "crowdworks.jp" => read_fixture("cw_search_ruby.html"),
      "lancers.jp" => "<html><body></body></html>",
      "coconala.com" => "<html><body></body></html>",
      "shufti.jp" => '{"data":[],"meta":{}}',
      "mamaworks.jp" => "<html><body></body></html>",
      "craudia.com" => "<html><body></body></html>"
    }
  end

  def test_call_with_engineer_profile_keeps_unstarred_classified_rows_and_reports_profile_summary
    fetcher = RoutingFakeFetcher.new(routes: engineer_fixture_routes)
    sheets_client = FakeSheetsClient.new(existing_values: header_only_existing_values)

    service = FreelanceJobs::ResearchService.new(profile: FreelanceJobs::Profile::ENGINEER,
                                                  run_window_label: "毎朝 06:00〜06:30（日本時間）",
                                                  fetcher: fetcher, sheets_client: sheets_client, now: NOW)
    summary = service.call

    refute summary[:aborted]
    assert_equal "engineer", summary[:profile]
    assert_equal 1_065_736_587, summary[:sheet_gid]
    assert_equal 13, summary[:candidates], "CrowdWorks14件中「WEBデザイナー案件」の1件だけがcategory nilで除外される"
    assert_equal 12, summary[:starred_candidates]
    assert_equal 13, summary[:added], "new_rows_require_star:falseのため🌟無し行も含めて13件とも新規追加される"

    written_rows = sheets_client.replace_sheet_calls.first[:rows]
    assert_equal 13, written_rows.size

    unstarred_row = written_rows.find { |written_row| written_row[3].include?("不動産SaaS開発") }
    refute_nil unstarred_row, "🌟が付かない分類済み行も新規追加される想定（new_rows_require_star: false）"
    assert_equal "", unstarred_row[0], "この行にはおすすめの🌟が付いていない想定"

    refute(written_rows.any? { |written_row| written_row[3].include?("WEBデザイナー") },
           "category nilと分類された行は除外される")
  end

  def test_call_with_engineer_profile_writes_engineer_header_not_beginner_header
    fetcher = RoutingFakeFetcher.new(routes: engineer_fixture_routes)
    sheets_client = FakeSheetsClient.new(existing_values: header_only_existing_values)

    service = FreelanceJobs::ResearchService.new(profile: FreelanceJobs::Profile::ENGINEER,
                                                  run_window_label: "毎朝 06:00〜06:30（日本時間）",
                                                  fetcher: fetcher, sheets_client: sheets_client, now: NOW)
    service.call

    written_header = sheets_client.replace_sheet_calls.first[:header]
    assert_equal FreelanceJobs::Profile::ENGINEER.header, written_header
    assert_includes written_header, "レベル（求められる経験）"
    assert_includes written_header, "一言メモ（条件・注意点）"
    refute_includes written_header, "難易度"
  end
end
