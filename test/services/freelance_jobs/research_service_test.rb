# frozen_string_literal: true
# test/services/freelance_jobs/research_service_test.rb
#
# Fake fetcher（サイトのホスト名でルーティングし、fixtureの本文を返す）と
# Fake sheets client（read_rows/replace_sheetの呼び出しを記録するだけ）を使い、
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

      route = @routes.fetch(matched_host)
      # ルートがcallableなら（一覧URLと詳細URLでhostが同じEFのように）urlごとに本文を出し分ける。
      # 既存のString値のルートはこれまでどおりそのまま返る。
      route.respond_to?(:call) ? route.call(url) : route
    end
  end

  class AlwaysFailingFetcher
    def get(_url, headers: {})
      raise FreelanceJobs::FetchError, "network is unreachable"
    end
  end

  class FakeSheetsClient
    attr_reader :replace_sheet_calls, :read_rows_calls

    # checked_urls: SheetMerger.mergeへprotected_urls:として渡される値をテストから注入するため。
    def initialize(existing_values:, checked_urls: [])
      @existing_values = existing_values
      @checked_urls = checked_urls
      @replace_sheet_calls = []
      @read_rows_calls = []
    end

    def read_rows(max_row_count: 2000)
      @read_rows_calls << max_row_count
      @existing_values
    end

    def checked_urls
      @checked_urls
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
    assert_includes banner_text, "⏰ 毎朝 06:00〜06:30（日本時間）更新"
    assert_includes banner_text, "09/04 06:05"
    assert_includes banner_text, "6/6サイト"
    refute_includes banner_text, "⚠失敗", "全サイト成功時は失敗の欄を出さない想定"
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
    assert_equal [], sheets_client.read_rows_calls, "全滅時はシート読み取りにも進まない"
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

  # --- (d) 1サイトだけ失敗しても続行し、bannerに⚠失敗が入る ---

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
    assert_includes banner_text, "⚠失敗 クラウディア"
    assert_includes banner_text, "5/6サイト"
    # 失敗の原因（HTTPステータス・URL・例外クラス）はログとsummaryだけに残し、バナーには出さない。
    refute_includes banner_text, "FetchError"
    refute_includes banner_text, "http"
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

  def test_call_with_excluded_sites_banner_shows_source_count_and_excluded_site_name
    fetcher = RoutingFakeFetcher.new(routes: real_fixture_routes_excluding_lancers)
    sheets_client = FakeSheetsClient.new(existing_values: header_only_existing_values)

    service = FreelanceJobs::ResearchService.new(profile: FreelanceJobs::Profile::BEGINNER,
                                                  run_window_label: "毎朝 06:00〜06:30（日本時間）",
                                                  excluded_sites: ["ランサーズ"],
                                                  fetcher: fetcher, sheets_client: sheets_client, now: NOW)
    summary = service.call

    refute summary[:aborted]
    banner_text = sheets_client.replace_sheet_calls.first[:banner_text]
    assert_includes banner_text, "5/5サイト"
    assert_includes banner_text, "除外 ランサーズ"
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
    refute_includes failure_message, "FetchError", "クラス名は失敗メッセージに出さない設計"
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

  # === AC-02: closed / open の振り分け ===
  # 通信なし。source_specsに渡した固定postings配列をそのまま返すFakeソースで、
  # closed?なpostingが分類器に渡らず、closed_urlsとしてSheetMergerへ渡って
  # 既存行を削除することを検証する。

  # postings: にJobPosting配列をそのまま渡すだけのFakeソース。
  class FixedPostingsSource
    SITE_NAME = "テストソース"
    REQUEST_INTERVAL = 0

    def initialize(fetcher:, today:, postings:)
      @postings = postings
    end

    def fetch
      @postings
    end
  end

  # AC-02b用。SITE_NAMEだけがFixedPostingsSourceと異なる（ココナラテックの実SITE_NAMEに揃える）。
  class CoconalaTechFixedPostingsSource
    SITE_NAME = "ココナラテック"
    REQUEST_INTERVAL = 0

    def initialize(fetcher:, today:, postings:)
      @postings = postings
    end

    def fetch
      @postings
    end
  end

  # classifyに渡されたposting列を記録するだけのFakeクラス（class:)。
  class RecordingClassifier
    attr_reader :classified_postings

    def initialize
      @classified_postings = []
    end

    def classify(posting, today:)
      @classified_postings << posting
      FreelanceJobs::Classifier::Result.new(category: "テスト分類", difficulty: "★☆☆", recommend: "",
                                             memo: "memo", skills_text: "skill")
    end
  end

  def build_job_posting(site:, url:, title:, application_status: "-", category_hint: "テスト分類")
    FreelanceJobs::JobPosting.new(
      site: site, url: FreelanceJobs::JobPosting.normalize_url(url), title: title,
      description: "説明", category_hint: category_hint, reward: "要確認", work_format: "業務委託（フリーランス）",
      application_status: application_status, deadline_text: "-", deadline_on: nil, skills: [],
      client: "", tags: [], posted_on: nil
    )
  end

  def build_test_profile(source_specs:, classifier:, site_list_sheet_gid: nil)
    FreelanceJobs::Profile::Definition.new(
      key: "test_profile", label: "テスト", sheet_gid: 999_999,
      header: FreelanceJobs::RowBuilder::HEADER, category_order: ["テスト分類"],
      classifier: classifier, source_specs: source_specs,
      new_rows_require_star: false, checkbox_column: false, hidden_level_marker: nil,
      site_list_sheet_gid: site_list_sheet_gid
    )
  end

  # 転載サイト名（Green等）を掲載サイト列に出す取得元。印（reports_original_site_names?）を持つ。
  class OriginalSiteNamesSource
    SITE_NAME = "転載元まとめ"
    REQUEST_INTERVAL = 0

    def self.reports_original_site_names? = true

    def initialize(fetcher:, today:, postings:)
      @postings = postings
    end

    def fetch
      @postings
    end
  end

  # 印を持たない以外は OriginalSiteNamesSource と同じ（従来挙動の確認用）。
  class PlainReSiteSource < OriginalSiteNamesSource
    class << self
      undef_method :reports_original_site_names?
    end
  end

  def build_expired_existing_row(site:, url:)
    row = Array.new(15, "")
    row[2] = "テスト分類"
    row[3] = "期限切れの既存案件"
    row[4] = site
    row[5] = url
    row[12] = "2026-09-01"
    row[14] = "2026-08-01 00:00"
    row
  end

  def run_with_original_site_source(source_class)
    posting = build_job_posting(site: "Green", url: "https://example.com/jobs/green-new", title: "新規案件")
    profile = build_test_profile(source_specs: [[source_class, { postings: [posting] }]],
                                 classifier: RecordingClassifier.new)
    green_row = build_expired_existing_row(site: "Green", url: "https://example.com/jobs/green-old")
    bizreach_row = build_expired_existing_row(site: "ビズリーチ", url: "https://example.com/jobs/bizreach-old")
    sheets_client = FakeSheetsClient.new(existing_values: [FreelanceJobs::RowBuilder::HEADER, green_row, bizreach_row])

    summary = FreelanceJobs::ResearchService.new(profile: profile, run_window_label: "テスト実行",
                                                  fetcher: nil, sheets_client: sheets_client, now: NOW).call
    [summary, sheets_client.replace_sheet_calls.first[:rows].map { |row| row[5] }]
  end

  def test_call_treats_original_site_names_as_succeeded_so_expired_rows_of_those_sites_are_removed
    summary, written_urls = run_with_original_site_source(OriginalSiteNamesSource)

    assert_equal 2, summary[:removed], "今回返した Green と、今回取得の無いビズリーチの期限切れ行は消えるはず"
    refute_includes written_urls, "https://example.com/jobs/green-old"
    refute_includes written_urls, "https://example.com/jobs/bizreach-old"
    assert_includes summary[:succeeded_sites], "Green"
    assert_includes summary[:succeeded_sites], "ビズリーチ"
    assert_equal({ "転載元まとめ" => 1 }, summary[:fetched], "件数更新用は SITE_NAME キーのまま")
  end

  def test_call_without_original_site_marker_keeps_expired_rows_of_unknown_sites
    summary, written_urls = run_with_original_site_source(PlainReSiteSource)

    assert_equal 0, summary[:removed], "印の無い取得元は従来どおり、取得失敗サイト扱いの行を触らない"
    assert_includes written_urls, "https://example.com/jobs/green-old"
    assert_equal ["転載元まとめ"], summary[:succeeded_sites]
  end

  def test_call_does_not_pass_closed_postings_to_the_classifier
    open_posting = build_job_posting(site: "テストソース", url: "https://example.com/jobs/open-1", title: "募集中案件")
    closed_posting = build_job_posting(site: "テストソース", url: "https://example.com/jobs/closed-1",
                                        title: "募集終了案件", application_status: "募集終了")
    classifier = RecordingClassifier.new
    profile = build_test_profile(
      source_specs: [[FixedPostingsSource, { postings: [open_posting, closed_posting] }]],
      classifier: classifier
    )
    sheets_client = FakeSheetsClient.new(existing_values: header_only_existing_values)

    service = FreelanceJobs::ResearchService.new(profile: profile, run_window_label: "テスト実行",
                                                  fetcher: nil, sheets_client: sheets_client, now: NOW)
    service.call

    classified_urls = classifier.classified_postings.map(&:url)
    assert_includes classified_urls, open_posting.url, "募集中postingは分類器に渡されるはず"
    refute_includes classified_urls, closed_posting.url, "募集終了postingは分類器に渡されない（build_rowsの対象外）はず"
  end

  def test_call_passes_closed_posting_urls_to_merge_and_removes_matching_existing_row
    closed_url = FreelanceJobs::JobPosting.normalize_url("https://example.com/jobs/closed-2")
    open_posting = build_job_posting(site: "テストソース", url: "https://example.com/jobs/open-2", title: "募集中案件")
    closed_posting = build_job_posting(site: "テストソース", url: closed_url, title: "募集終了案件",
                                        application_status: "募集終了")
    classifier = RecordingClassifier.new
    profile = build_test_profile(
      source_specs: [[FixedPostingsSource, { postings: [open_posting, closed_posting] }]],
      classifier: classifier
    )

    existing_row = Array.new(15, "")
    existing_row[2] = "テスト分類"
    existing_row[3] = "既存の案件名(手入力保持)"
    existing_row[4] = "テストソース"
    existing_row[5] = closed_url
    # 締切は遠い未来にしておく。should_remove?（締切ルール）では消えないはずの行が、
    # closed_urlsのおかげで削除されることを確認する。
    existing_row[12] = "2026-12-31"
    existing_row[14] = "2026-09-01 00:00"
    sheets_client = FakeSheetsClient.new(existing_values: [FreelanceJobs::RowBuilder::HEADER, existing_row])

    service = FreelanceJobs::ResearchService.new(profile: profile, run_window_label: "テスト実行",
                                                  fetcher: nil, sheets_client: sheets_client, now: NOW)
    summary = service.call

    refute summary[:aborted]
    assert_equal 1, summary[:removed], "closed_urlsに含まれる既存行は締切が先でも削除されるはず"

    written_urls = sheets_client.replace_sheet_calls.first[:rows].map { |row| row[5] }
    refute_includes written_urls, closed_url, "募集終了postingの既存行はシートから消えているはず"
  end

  # === AC-02b: ココナラテック由来のclosed postingでも同様にclosed_urlsとして扱われる ===

  def test_call_treats_coconala_tech_closed_posting_url_as_closed_url_too
    coconala_closed_url = FreelanceJobs::JobPosting.normalize_url("https://tech.coconala.com/job-postings/closed-1")
    open_posting = build_job_posting(site: "テストソース", url: "https://example.com/jobs/open-3", title: "募集中案件")
    coconala_closed_posting = build_job_posting(site: "ココナラテック", url: coconala_closed_url,
                                                 title: "ココナラテック募集終了案件", application_status: "募集終了")

    classifier = RecordingClassifier.new
    profile = build_test_profile(
      source_specs: [
        [FixedPostingsSource, { postings: [open_posting] }],
        [CoconalaTechFixedPostingsSource, { postings: [coconala_closed_posting] }]
      ],
      classifier: classifier
    )

    existing_row = Array.new(15, "")
    existing_row[2] = "テスト分類"
    existing_row[3] = "ココナラテックの既存案件"
    existing_row[4] = "ココナラテック"
    existing_row[5] = coconala_closed_url
    existing_row[12] = "-"
    existing_row[14] = "2026-09-01 00:00"
    sheets_client = FakeSheetsClient.new(existing_values: [FreelanceJobs::RowBuilder::HEADER, existing_row])

    service = FreelanceJobs::ResearchService.new(profile: profile, run_window_label: "テスト実行",
                                                  fetcher: nil, sheets_client: sheets_client, now: NOW)
    summary = service.call

    refute summary[:aborted]
    assert_equal 1, summary[:removed], "ココナラテック由来のclosed postingのURLも既存行の削除に使われるはず"

    written_urls = sheets_client.replace_sheet_calls.first[:rows].map { |row| row[5] }
    refute_includes written_urls, coconala_closed_url
  end

  # === AC-04: 本日追加の行をnew_today_row_indexesとしてreplace_sheetへ渡す ===
  # merge後の行のうち、追加日(index15)が「今日」の行だけがnew_today_row_indexesに入り、
  # 今日ではない既存行のindexは含まれない想定。

  def test_call_passes_only_rows_added_today_as_new_today_row_indexes
    today_posting = build_job_posting(site: "テストソース", url: "https://example.com/jobs/today-1", title: "本日追加案件")
    classifier = RecordingClassifier.new
    profile = build_test_profile(
      source_specs: [[FixedPostingsSource, { postings: [today_posting] }]],
      classifier: classifier
    )

    existing_row = Array.new(16, "")
    existing_row[2] = "テスト分類"
    existing_row[3] = "既存案件（本日追加ではない）"
    existing_row[4] = "テストソース"
    existing_row[5] = FreelanceJobs::JobPosting.normalize_url("https://example.com/jobs/existing-old")
    existing_row[12] = "2030-01-01"
    existing_row[14] = "2026-08-01 00:00"
    existing_row[15] = "2026-08-01" # 追加日は本日ではない
    sheets_client = FakeSheetsClient.new(existing_values: [FreelanceJobs::RowBuilder::HEADER, existing_row])

    service = FreelanceJobs::ResearchService.new(profile: profile, run_window_label: "テスト実行",
                                                  fetcher: nil, sheets_client: sheets_client, now: NOW)
    service.call

    written_rows = sheets_client.replace_sheet_calls.first[:rows]
    today_added_on_text = NOW.to_date.strftime("%Y-%m-%d")
    new_today_row_index = written_rows.index { |written_row| written_row[5] == today_posting.url }
    existing_row_index = written_rows.index { |written_row| written_row[3] == "既存案件（本日追加ではない）" }

    refute_nil new_today_row_index
    refute_nil existing_row_index
    assert_equal today_added_on_text, written_rows[new_today_row_index][15], "新規行の追加日は今日のはず"
    refute_equal today_added_on_text, written_rows[existing_row_index][15], "既存行の追加日は保持され今日にはならないはず"

    new_today_row_indexes = sheets_client.replace_sheet_calls.first[:new_today_row_indexes]
    assert_equal [new_today_row_index], new_today_row_indexes
    refute_includes new_today_row_indexes, existing_row_index
  end

  # === 容量都合の退避（evicted）をsummary・バナーへ反映する ===

  def build_evict_test_existing_row(site:, url:)
    row = Array.new(16, "")
    row[2] = "テスト分類"
    row[3] = "既存の案件名(手入力保持)"
    row[4] = site
    row[5] = url
    row
  end

  # OverflowSite(460件)がMAX_ROWS_PER_SITE(40)を大幅に超えており、予算が0のため、
  # NewSiteの保証枠3件を確保するにはOverflowSiteから3件退避する必要がある構成。
  def test_call_evicts_overflow_site_rows_and_reports_evicted_in_summary_and_banner
    overflow_site_rows = Array.new(460) { |index| build_evict_test_existing_row(site: "OverflowSite", url: "https://example.com/OverflowSite/existing-#{index}") }
    filler_site_rows = Array.new(40) { |index| build_evict_test_existing_row(site: "FillerSite", url: "https://example.com/FillerSite/existing-#{index}") }
    new_postings = Array.new(5) do |index|
      build_job_posting(site: "NewSite", url: "https://example.com/NewSite/new-#{index}", title: "新規案件#{index}")
    end
    classifier = RecordingClassifier.new
    profile = build_test_profile(
      source_specs: [[FixedPostingsSource, { postings: new_postings }]],
      classifier: classifier
    )
    sheets_client = FakeSheetsClient.new(
      existing_values: [FreelanceJobs::RowBuilder::HEADER] + overflow_site_rows + filler_site_rows
    )

    service = FreelanceJobs::ResearchService.new(profile: profile, run_window_label: "テスト実行",
                                                  fetcher: nil, sheets_client: sheets_client, now: NOW)
    summary = service.call

    refute summary[:aborted]
    assert_equal 3, summary[:evicted], "OverflowSiteから保証枠の不足分(3件)だけ退避されるはず"
    assert_equal 3, summary[:added]

    banner_text = sheets_client.replace_sheet_calls.first[:banner_text]
    assert_includes banner_text, "入替3", "容量都合の退避件数がバナーに入替Nとして表示されるはず"
  end

  def test_call_success_path_summary_includes_zero_evicted_when_no_eviction_happens
    fetcher = RoutingFakeFetcher.new(routes: real_fixture_routes)
    sheets_client = FakeSheetsClient.new(existing_values: header_only_existing_values)

    service = FreelanceJobs::ResearchService.new(profile: FreelanceJobs::Profile::BEGINNER,
                                                  run_window_label: "毎朝 06:00〜06:30（日本時間）",
                                                  fetcher: fetcher, sheets_client: sheets_client, now: NOW)
    summary = service.call

    refute summary[:aborted]
    assert_equal 0, summary[:evicted]
    refute_includes sheets_client.replace_sheet_calls.first[:banner_text], "入替", "退避が無いときはバナーに入替を出さない想定"
  end

  def test_call_aborted_summary_includes_zero_evicted
    sheets_client = FakeSheetsClient.new(existing_values: header_only_existing_values)
    service = FreelanceJobs::ResearchService.new(profile: FreelanceJobs::Profile::BEGINNER,
                                                  run_window_label: "毎朝 06:00〜06:30（日本時間）",
                                                  fetcher: AlwaysFailingFetcher.new, sheets_client: sheets_client, now: NOW)

    summary = service.call

    assert summary[:aborted]
    assert_equal 0, summary[:evicted]
  end

  # === AC-13: ClosureVerifierをResearchServiceへ組み込む（エンジニアファクトリー由来）===
  # EFは一覧に募集終了の印が出ないため、一覧に載っている案件の詳細ページを取りに行って
  # 「こちらの案件は終了しました」を検出し、closed_urls経由で新規追加をせず既存行も消す想定。
  # 実装(AC-13)はまだ無いため、このテストは現時点でRed（詳細確認が呼ばれず落ちる）でよい。

  ENGINEER_FACTORY_DETAIL_HOST = "engineer-factory.com"

  # EF一覧カード1件分の最小HTML片（案件パス・案件名だけがあれば十分。Rubyキーワードを
  # 含めてEngineerClassifierでRuby分類されるようにする）。
  def build_engineer_factory_list_card_html(job_id:, title: "【Ruby】バックエンドエンジニア募集")
    <<~HTML
      <section class="modJobBlock modJobBlock--large">
        <h3 class="modJobBlock__title"><a href="/freelance/jobs/#{job_id}">#{title}<span>東京都の案件・求人</span></a></h3>
      </section>
    HTML
  end

  # 一覧URL(/freelance/jobs/skill/<id>[?page=N])には常に同じ1カードを、
  # 詳細URL(/freelance/jobs/<job_id>)には募集終了フィクスチャを返すcallableルート。
  def engineer_factory_routed_body(job_id:)
    detail_url_pattern = %r{/freelance/jobs/#{job_id}\z}
    list_body = wrap_html(build_engineer_factory_list_card_html(job_id: job_id))
    closed_detail_body = read_fixture("engineer_factory_detail_closed.html")

    lambda do |url|
      url.match?(detail_url_pattern) ? closed_detail_body : list_body
    end
  end

  def test_call_with_engineer_profile_excludes_engineer_factory_listing_whose_detail_page_is_closed
    job_id = "999001"
    ef_url = FreelanceJobs::JobPosting.normalize_url("https://www.engineer-factory.com/freelance/jobs/#{job_id}")

    fetcher = RoutingFakeFetcher.new(
      routes: engineer_fixture_routes.merge(ENGINEER_FACTORY_DETAIL_HOST => engineer_factory_routed_body(job_id: job_id))
    )

    existing_ef_row = Array.new(16, "")
    existing_ef_row[2] = "Ruby"
    existing_ef_row[3] = "既存のエンジニアファクトリー案件（募集終了予定）"
    existing_ef_row[4] = "エンジニアファクトリー"
    existing_ef_row[5] = ef_url
    existing_ef_row[12] = "2030-01-01" # 締切は先の未来（締切ルールでは消えないはず）
    existing_ef_row[14] = "2026-08-01 00:00"
    sheets_client = FakeSheetsClient.new(existing_values: [FreelanceJobs::RowBuilder::HEADER, existing_ef_row])

    service = FreelanceJobs::ResearchService.new(profile: FreelanceJobs::Profile::ENGINEER,
                                                  run_window_label: "毎朝 06:00〜06:30（日本時間）",
                                                  fetcher: fetcher, sheets_client: sheets_client, now: NOW)
    summary = service.call

    refute summary[:aborted]
    written_urls = sheets_client.replace_sheet_calls.first[:rows].map { |row| row[5] }
    refute_includes written_urls, ef_url,
                    "詳細確認で募集終了と判定されたEF案件は新規追加も既存維持もされないはず"
  end

  # === AC-13回帰: 一覧取得に失敗したサイトの既存行は誤って削除されない ===
  # verify_closuresはfailed_sitesを詳細確認の対象から除外している。これはSheetMerger側の
  # 「取得失敗サイトの行は触らない」保護よりclosed_urls判定が先に効くため、除外が壊れると
  # 一覧取得が失敗しただけで既存行が誤って削除されてしまう最重要の防波堤。
  # EFの一覧取得(list_url)だけを失敗させ、詳細ページ(もし確認されれば「募集終了」と判定される
  # フィクスチャ)は取得できる状態にしておくことで、「詳細確認が走っていれば消えていたはずの行が、
  # 一覧取得失敗により詳細確認自体が行われず残る」ことを検証する。
  def engineer_factory_route_with_failing_list_but_closable_detail(job_id:)
    detail_url_pattern = %r{/freelance/jobs/#{job_id}\z}
    closed_detail_body = read_fixture("engineer_factory_detail_closed.html")

    lambda do |url|
      # 一覧URL(/freelance/jobs/skill/<skill_id>[?page=N])は常に失敗させる。
      # 詳細URL(/freelance/jobs/<job_id>)はもし確認されれば募集終了と判定される本文を返す。
      url.match?(detail_url_pattern) ? closed_detail_body : (raise FreelanceJobs::FetchError, "stubbed EF list failure for #{url}")
    end
  end

  def test_call_keeps_existing_row_of_a_site_whose_listing_fetch_failed_even_if_its_detail_page_would_be_closed
    job_id = "999002"
    ef_url = FreelanceJobs::JobPosting.normalize_url("https://www.engineer-factory.com/freelance/jobs/#{job_id}")

    fetcher = RoutingFakeFetcher.new(
      routes: engineer_fixture_routes.merge(
        ENGINEER_FACTORY_DETAIL_HOST => engineer_factory_route_with_failing_list_but_closable_detail(job_id: job_id)
      )
    )

    existing_ef_row = Array.new(16, "")
    existing_ef_row[2] = "Ruby"
    existing_ef_row[3] = "既存のエンジニアファクトリー案件（一覧取得失敗時は残るはず）"
    existing_ef_row[4] = "エンジニアファクトリー"
    existing_ef_row[5] = ef_url
    existing_ef_row[12] = "2030-01-01" # 締切は先の未来（締切ルールでは消えないはず）
    existing_ef_row[14] = "2026-08-01 00:00"
    sheets_client = FakeSheetsClient.new(existing_values: [FreelanceJobs::RowBuilder::HEADER, existing_ef_row])

    service = FreelanceJobs::ResearchService.new(profile: FreelanceJobs::Profile::ENGINEER,
                                                  run_window_label: "毎朝 06:00〜06:30（日本時間）",
                                                  fetcher: fetcher, sheets_client: sheets_client, now: NOW)
    summary = service.call

    refute summary[:aborted]
    assert_includes summary[:failures].map { |failure_message| failure_message.split("（").first }, "エンジニアファクトリー",
                     "EFの一覧取得は失敗として記録されるはず"
    refute_includes summary[:succeeded_sites], "エンジニアファクトリー"

    banner_text = sheets_client.replace_sheet_calls.first[:banner_text]
    assert_includes banner_text, "⚠失敗", "バナーにも失敗サイトが載るはず"
    assert_includes banner_text, "エンジニアファクトリー"

    written_urls = sheets_client.replace_sheet_calls.first[:rows].map { |row| row[5] }
    assert_includes written_urls, ef_url,
                     "一覧取得が失敗したサイトの既存行は、詳細を確認すれば募集終了と判定されるものでも削除されないはず"
    refute_includes fetcher.requested_urls, ef_url,
                    "一覧取得が失敗したサイトは詳細確認の対象からも除外され、詳細URLへのリクエスト自体が発生しないはず"
  end

  # === AC-05: サイト一覧タブへの案件数書き戻し ===

  # update の引数を記録するだけのFake。raise_error を渡すと update で例外を投げる。
  class FakeSiteListSheet
    attr_reader :update_calls

    def initialize(raise_error: nil)
      @raise_error = raise_error
      @update_calls = []
    end

    def update(**kwargs)
      @update_calls << kwargs
      raise @raise_error if @raise_error

      kwargs[:sheet_rows].size
    end
  end

  # 一覧取得で必ず失敗するFakeソース（failed_sitesに入る）。
  class AlwaysFailingSource
    SITE_NAME = "失敗ソース"
    REQUEST_INTERVAL = 0

    def initialize(fetcher:, today:, **)
    end

    def fetch
      raise FreelanceJobs::FetchError, "boom"
    end
  end

  # 除外指定されるだけで一度も取得されないFakeソース。
  class ExcludedTargetSource
    SITE_NAME = "除外ソース"
    REQUEST_INTERVAL = 0

    def initialize(fetcher:, today:, **)
    end

    def fetch
      []
    end
  end

  def site_list_test_profile(site_list_sheet_gid:)
    open_posting = build_job_posting(site: "テストソース", url: "https://example.com/jobs/site-list-1", title: "募集中案件")
    build_test_profile(
      source_specs: [
        [FixedPostingsSource, { postings: [open_posting] }],
        [AlwaysFailingSource, {}],
        [ExcludedTargetSource, {}]
      ],
      classifier: RecordingClassifier.new,
      site_list_sheet_gid: site_list_sheet_gid
    )
  end

  def run_site_list_service(site_list_sheet_gid:, site_list_sheet:)
    sheets_client = FakeSheetsClient.new(existing_values: header_only_existing_values)
    service = FreelanceJobs::ResearchService.new(
      profile: site_list_test_profile(site_list_sheet_gid: site_list_sheet_gid),
      run_window_label: "テスト実行", fetcher: nil, sheets_client: sheets_client,
      site_list_sheet: site_list_sheet, excluded_sites: ["除外ソース"], now: NOW
    )
    [service.call, sheets_client]
  end

  def test_call_updates_site_list_with_fetched_counts_failed_sites_excluded_sites_and_merged_rows
    site_list_sheet = FakeSiteListSheet.new

    summary, sheets_client = run_site_list_service(site_list_sheet_gid: 123, site_list_sheet: site_list_sheet)

    refute summary[:aborted]
    assert_equal 1, site_list_sheet.update_calls.size
    update_call = site_list_sheet.update_calls.first
    assert_equal({ "テストソース" => 1 }, update_call[:fetched_counts])
    assert_equal ["失敗ソース"], update_call[:failed_sites]
    assert_equal ["除外ソース"], update_call[:excluded_sites]
    assert_equal sheets_client.replace_sheet_calls.first[:rows], update_call[:sheet_rows],
                 "シートへ書いたマージ後の行をそのまま渡すはず"
  end

  def test_call_reports_site_list_updated_true_on_success
    summary, = run_site_list_service(site_list_sheet_gid: 123, site_list_sheet: FakeSiteListSheet.new)

    assert_equal true, summary[:site_list_updated]
  end

  def test_call_does_not_touch_site_list_when_profile_has_no_site_list_sheet_gid
    site_list_sheet = FakeSiteListSheet.new

    summary, = run_site_list_service(site_list_sheet_gid: nil, site_list_sheet: site_list_sheet)

    assert_equal [], site_list_sheet.update_calls
    assert summary.key?(:site_list_updated), "対象外でもキー自体は持つはず（値はnil）"
    assert_nil summary[:site_list_updated]
  end

  def test_call_succeeds_with_site_list_updated_false_when_site_list_update_raises
    site_list_sheet = FakeSiteListSheet.new(raise_error: FreelanceJobs::FetchError.new("tab missing"))

    summary, sheets_client = run_site_list_service(site_list_sheet_gid: 123, site_list_sheet: site_list_sheet)

    refute summary[:aborted], "サイト一覧の更新失敗でメインの結果を失敗にしない"
    assert_equal false, summary[:site_list_updated]
    assert_equal 1, sheets_client.replace_sheet_calls.size
    assert_equal 1, site_list_sheet.update_calls.size
  end
end
