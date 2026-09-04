# frozen_string_literal: true
# test/services/freelance_jobs/sheet_merger_test.rb

require_relative "../../support/freelance_jobs_loader"
require "date"

class FreelanceJobsSheetMergerTest < Minitest::Test
  TODAY = Date.new(2026, 9, 4)

  # 15列のテスト用行を組み立てる（列インデックスはSheetMergerのコメント通り）。
  def row(recommend: "", category: "HTML/CSS", title: "title", site: "CrowdWorks", url:, difficulty: "★☆☆",
          summary: "summary", skills: "skills", reward: "reward", work_format: "work_format",
          application_status: "status", deadline_text: "-", memo: "memo", fetched_on: "2026-09-04 09:00")
    [recommend, 0, category, title, site, url, difficulty, summary, skills, reward, work_format,
     application_status, deadline_text, memo, fetched_on]
  end

  # --- 既存行の自動更新列(J/K/L/M/O)だけが更新され、手入力列(A/C/D/G/H/I/N)は保持される ---

  def test_matched_existing_row_updates_only_auto_update_columns
    existing = row(
      recommend: "OLD_A", category: "OLD_C", title: "OLD_D", site: "CrowdWorks", url: "https://crowdworks.jp/public/jobs/1",
      difficulty: "OLD_G", summary: "OLD_H", skills: "OLD_I", reward: "OLD_J", work_format: "OLD_K",
      application_status: "OLD_L", deadline_text: "OLD_M", memo: "OLD_N", fetched_on: "OLD_O"
    )
    new_row = row(
      recommend: "NEW_A", category: "NEW_C", title: "NEW_D", site: "CrowdWorks", url: "https://crowdworks.jp/public/jobs/1",
      difficulty: "NEW_G", summary: "NEW_H", skills: "NEW_I", reward: "NEW_J", work_format: "NEW_K",
      application_status: "NEW_L", deadline_text: "NEW_M", memo: "NEW_N", fetched_on: "NEW_O"
    )

    result = FreelanceJobs::SheetMerger.merge(existing_rows: [existing], new_rows: [new_row],
                                               succeeded_sites: ["CrowdWorks"], today: TODAY)

    merged = result.rows.first
    # 保持される列（A/C/D/G/H/I/N）
    assert_equal "OLD_A", merged[0]
    assert_equal "OLD_C", merged[2]
    assert_equal "OLD_D", merged[3]
    assert_equal "OLD_G", merged[6]
    assert_equal "OLD_H", merged[7]
    assert_equal "OLD_I", merged[8]
    assert_equal "OLD_N", merged[13]
    # 更新される列（J/K/L/M/O）
    assert_equal "NEW_J", merged[9]
    assert_equal "NEW_K", merged[10]
    assert_equal "NEW_L", merged[11]
    assert_equal "NEW_M", merged[12]
    assert_equal "NEW_O", merged[14]

    assert_equal 1, result.updated
    assert_equal 0, result.added
    assert_equal 0, result.removed
  end

  # --- 取得失敗サイト（succeeded_sites外）の既存行は削除されない ---

  def test_existing_row_of_failed_site_is_never_removed_even_if_expired
    existing = row(site: "ランサーズ", url: "https://www.lancers.jp/work/detail/1", deadline_text: "2020-01-01")

    result = FreelanceJobs::SheetMerger.merge(existing_rows: [existing], new_rows: [],
                                               succeeded_sites: [], today: TODAY)

    assert_equal 0, result.removed
    assert_equal 1, result.rows.size
    assert_equal "https://www.lancers.jp/work/detail/1", result.rows.first[5]
  end

  # --- 締切が昨日の行は削除・今日の行は残る ---

  def test_deadline_yesterday_is_removed_and_today_is_kept
    yesterday_text = (TODAY - 1).strftime("%Y-%m-%d")
    today_text = TODAY.strftime("%Y-%m-%d")

    expired = row(site: "CrowdWorks", url: "https://crowdworks.jp/public/jobs/expired", deadline_text: yesterday_text)
    due_today = row(site: "CrowdWorks", url: "https://crowdworks.jp/public/jobs/today", deadline_text: today_text)

    result = FreelanceJobs::SheetMerger.merge(existing_rows: [expired, due_today], new_rows: [],
                                               succeeded_sites: ["CrowdWorks"], today: TODAY)

    urls = result.rows.map { |r| r[5] }
    assert_equal 1, result.removed
    refute_includes urls, "https://crowdworks.jp/public/jobs/expired"
    assert_includes urls, "https://crowdworks.jp/public/jobs/today"
  end

  # --- 締切不明かつ取得日時61日前は削除・59日前は残る（60日ちょうどは残る） ---

  def test_unknown_deadline_removed_after_61_days_but_kept_at_59_and_60_days
    fetched_61_days_ago = (TODAY - 61).strftime("%Y-%m-%d 09:00")
    fetched_60_days_ago = (TODAY - 60).strftime("%Y-%m-%d 09:00")
    fetched_59_days_ago = (TODAY - 59).strftime("%Y-%m-%d 09:00")

    stale = row(site: "CrowdWorks", url: "https://crowdworks.jp/public/jobs/stale", deadline_text: "-",
                fetched_on: fetched_61_days_ago)
    boundary = row(site: "CrowdWorks", url: "https://crowdworks.jp/public/jobs/boundary", deadline_text: "-",
                   fetched_on: fetched_60_days_ago)
    fresh = row(site: "CrowdWorks", url: "https://crowdworks.jp/public/jobs/fresh", deadline_text: "-",
                fetched_on: fetched_59_days_ago)

    result = FreelanceJobs::SheetMerger.merge(existing_rows: [stale, boundary, fresh], new_rows: [],
                                               succeeded_sites: ["CrowdWorks"], today: TODAY)

    urls = result.rows.map { |r| r[5] }
    assert_equal 1, result.removed
    refute_includes urls, "https://crowdworks.jp/public/jobs/stale"
    assert_includes urls, "https://crowdworks.jp/public/jobs/boundary"
    assert_includes urls, "https://crowdworks.jp/public/jobs/fresh"
  end

  # --- URL重複既存行は先頭のみ更新、2件目以降は変更しない ---

  def test_duplicate_existing_url_only_first_occurrence_is_updated
    first_dup = row(recommend: "FIRST_OLD", url: "https://crowdworks.jp/public/jobs/dup", reward: "FIRST_OLD_J",
                     site: "CrowdWorks")
    second_dup = row(recommend: "SECOND_OLD", url: "https://crowdworks.jp/public/jobs/dup", reward: "SECOND_OLD_J",
                      site: "CrowdWorks")
    new_row = row(url: "https://crowdworks.jp/public/jobs/dup", reward: "NEW_J", site: "CrowdWorks")

    result = FreelanceJobs::SheetMerger.merge(existing_rows: [first_dup, second_dup], new_rows: [new_row],
                                               succeeded_sites: ["CrowdWorks"], today: TODAY)

    assert_equal 2, result.rows.size
    assert_equal 1, result.updated
    first, second = result.rows.sort_by { |r| r[0] == "FIRST_OLD" ? 0 : 1 }
    assert_equal "NEW_J", first[9]
    assert_equal "SECOND_OLD_J", second[9], "2件目以降は変更されない"
    assert_equal "SECOND_OLD", second[0]
  end

  # --- F列(URL)が空の既存行はキーにせず保持する ---

  def test_existing_row_with_blank_url_is_kept_untouched
    blank_url_row = row(url: "", deadline_text: "2020-01-01") # 締切が過去でも削除対象にならない

    result = FreelanceJobs::SheetMerger.merge(existing_rows: [blank_url_row], new_rows: [],
                                               succeeded_sites: ["CrowdWorks"], today: TODAY)

    assert_equal 0, result.removed
    assert_equal 1, result.rows.size
  end

  # --- 300件上限は新規行にだけ効く（既存行は上限で落ちない） ---

  def test_300_row_cap_only_drops_new_rows_never_existing_rows
    existing_rows = Array.new(290) do |index|
      row(site: "CrowdWorks", url: "https://crowdworks.jp/public/jobs/existing-#{index}", deadline_text: "2030-01-01")
    end
    new_rows = Array.new(30) do |index|
      # 締切が遠いほど優先度が高い（インデックスが大きいほど生き残りやすい）。
      deadline_text = (TODAY + index + 1).strftime("%Y-%m-%d")
      row(recommend: "🌟", url: "https://crowdworks.jp/public/jobs/new-#{index}", deadline_text: deadline_text)
    end

    result = FreelanceJobs::SheetMerger.merge(existing_rows: existing_rows, new_rows: new_rows,
                                               succeeded_sites: ["CrowdWorks"], today: TODAY)

    assert_equal 300, result.total
    assert_equal 10, result.added
    existing_urls_present = result.rows.count { |r| r[5].start_with?("https://crowdworks.jp/public/jobs/existing-") }
    assert_equal 290, existing_urls_present, "既存行は300件上限の影響を受けない"

    surviving_new_urls = result.rows.map { |r| r[5] }.select { |url| url.include?("/new-") }
    assert_equal 10, surviving_new_urls.size
    # 締切が最も遠い(index 20..29)ものだけが残る。
    (20..29).each { |index| assert_includes surviving_new_urls, "https://crowdworks.jp/public/jobs/new-#{index}" }
    (0..19).each { |index| refute_includes surviving_new_urls, "https://crowdworks.jp/public/jobs/new-#{index}" }
  end

  # --- 並び順（HTML/CSS→Excel、分類内はrow_priority_key）とNo.採番 ---
  # このテストの既存行は🌟0件・新規行は🌟1件以上のため、🌟優先ルールの結果として
  # 新規行が先に並ぶ（ラウンド2 C10以降、「新規優先」は🌟数が同数のときのタイブレークに過ぎない。
  # 既存行が新規行より🌟が多い場合に既存行が先に来ることはC10セクションの専用テストで検証する）。

  def test_ordering_by_category_then_priority_within_category_and_renumbering
    existing_html = row(category: "HTML/CSS", url: "https://crowdworks.jp/public/jobs/existing-html")
    existing_excel = row(category: "Excel・スプレッドシート", url: "https://crowdworks.jp/public/jobs/existing-excel")
    new_html_double_star = row(recommend: "🌟🌟", category: "HTML/CSS", url: "https://crowdworks.jp/public/jobs/new-html-2star")
    new_html_single_star = row(recommend: "🌟", category: "HTML/CSS", url: "https://crowdworks.jp/public/jobs/new-html-1star")
    new_excel_single_star = row(recommend: "🌟", category: "Excel・スプレッドシート", url: "https://crowdworks.jp/public/jobs/new-excel-1star")

    result = FreelanceJobs::SheetMerger.merge(
      existing_rows: [existing_html, existing_excel],
      new_rows: [new_html_single_star, new_html_double_star, new_excel_single_star],
      succeeded_sites: ["CrowdWorks"], today: TODAY
    )

    ordered_urls = result.rows.map { |r| r[5] }
    assert_equal [
      "https://crowdworks.jp/public/jobs/new-html-2star",
      "https://crowdworks.jp/public/jobs/new-html-1star",
      "https://crowdworks.jp/public/jobs/existing-html",
      "https://crowdworks.jp/public/jobs/new-excel-1star",
      "https://crowdworks.jp/public/jobs/existing-excel"
    ], ordered_urls

    assert_equal [1, 2, 3, 4, 5], result.rows.map { |r| r[1] }
  end

  # === ラウンド2 C3: 1回あたりの新規追加上限（MAX_NEW_ROWS_PER_RUN = 80） ===

  def test_max_80_new_rows_per_run_keeps_highest_priority_ones
    new_rows = Array.new(100) do |index|
      deadline_text = (TODAY + index + 1).strftime("%Y-%m-%d") # indexが大きいほど締切が遠い＝優先度が高い
      row(recommend: "🌟", url: "https://crowdworks.jp/public/jobs/new-#{index}", deadline_text: deadline_text)
    end

    result = FreelanceJobs::SheetMerger.merge(existing_rows: [], new_rows: new_rows,
                                               succeeded_sites: ["CrowdWorks"], today: TODAY)

    assert_equal 80, result.added
    assert_equal 80, result.total
    surviving_indexes = result.rows.map { |r| r[5][%r{new-(\d+)}, 1].to_i }
    assert_equal (20..99).to_a.sort, surviving_indexes.sort, "締切が最も遠い上位80件だけが残る"
  end

  def test_added_reflects_actual_count_after_both_80_cap_and_300_cap
    existing_rows = Array.new(250) do |index|
      row(site: "CrowdWorks", url: "https://crowdworks.jp/public/jobs/existing-#{index}", deadline_text: "2030-01-01")
    end
    new_rows = Array.new(100) do |index|
      deadline_text = (TODAY + index + 1).strftime("%Y-%m-%d")
      row(recommend: "🌟", url: "https://crowdworks.jp/public/jobs/new-#{index}", deadline_text: deadline_text)
    end

    result = FreelanceJobs::SheetMerger.merge(existing_rows: existing_rows, new_rows: new_rows,
                                               succeeded_sites: ["CrowdWorks"], today: TODAY)

    assert_equal 300, result.total
    assert_equal 50, result.added, "80件キャップ後、さらに300件上限で削られた実数になっているべき"
    surviving_indexes = result.rows.map { |r| r[5] }.select { |url| url.include?("/new-") }
                               .map { |url| url[%r{new-(\d+)}, 1].to_i }
    assert_equal (50..99).to_a.sort, surviving_indexes.sort, "元の100件のうち上位50件（締切が遠い順）と一致するはず"
  end

  # === ラウンド2 C4: 新規行の並び順（🌟が多い順→締切が遠い順、締切不明は最後） ===

  def test_new_row_ordering_prioritizes_star_count_then_farthest_deadline_then_unknown_last
    near = row(recommend: "🌟", url: "https://crowdworks.jp/public/jobs/near", deadline_text: (TODAY + 10).strftime("%Y-%m-%d"))
    far = row(recommend: "🌟", url: "https://crowdworks.jp/public/jobs/far", deadline_text: (TODAY + 60).strftime("%Y-%m-%d"))
    middle = row(recommend: "🌟", url: "https://crowdworks.jp/public/jobs/middle", deadline_text: (TODAY + 30).strftime("%Y-%m-%d"))
    unknown = row(recommend: "🌟", url: "https://crowdworks.jp/public/jobs/unknown", deadline_text: "-")
    double_star = row(recommend: "🌟🌟", url: "https://crowdworks.jp/public/jobs/double-star",
                       deadline_text: (TODAY + 1).strftime("%Y-%m-%d"))

    result = FreelanceJobs::SheetMerger.merge(existing_rows: [], new_rows: [near, far, middle, unknown, double_star],
                                               succeeded_sites: ["CrowdWorks"], today: TODAY)

    ordered_urls = result.rows.map { |r| r[5] }
    assert_equal [
      "https://crowdworks.jp/public/jobs/double-star", # 🌟🌟が最優先（締切が近くても）
      "https://crowdworks.jp/public/jobs/far",
      "https://crowdworks.jp/public/jobs/middle",
      "https://crowdworks.jp/public/jobs/near",
      "https://crowdworks.jp/public/jobs/unknown" # 締切不明は最後
    ], ordered_urls
  end

  # === ラウンド2 C10: 分類内の並び順は新規/既存を区別しない統一キー ===
  # 優先順位: 🌟が多い順 → 新規行が既存行より先（🌟同数のときのタイブレーク） →
  #           締切が遠い順（不明は最後） → 既存行同士は元の順。80件/300件上限の選定も同じキー。

  def test_existing_row_with_more_stars_outranks_new_row_with_fewer_stars
    existing_double_star = row(recommend: "🌟🌟", category: "HTML/CSS",
                                url: "https://crowdworks.jp/public/jobs/existing-2star")
    new_unstarred = row(recommend: "", category: "HTML/CSS", url: "https://crowdworks.jp/public/jobs/new-0star")

    result = FreelanceJobs::SheetMerger.merge(
      existing_rows: [existing_double_star], new_rows: [new_unstarred],
      succeeded_sites: ["CrowdWorks"], today: TODAY
    )

    ordered_urls = result.rows.map { |r| r[5] }
    assert_equal [
      "https://crowdworks.jp/public/jobs/existing-2star", # 🌟が多い既存行が🌟なし新規行より先
      "https://crowdworks.jp/public/jobs/new-0star"
    ], ordered_urls
  end

  def test_new_row_precedes_existing_row_when_star_count_ties
    existing_same_star = row(recommend: "🌟", category: "HTML/CSS",
                              url: "https://crowdworks.jp/public/jobs/existing-1star", deadline_text: "-")
    new_same_star = row(recommend: "🌟", category: "HTML/CSS",
                         url: "https://crowdworks.jp/public/jobs/new-1star", deadline_text: "-")

    result = FreelanceJobs::SheetMerger.merge(
      existing_rows: [existing_same_star], new_rows: [new_same_star],
      succeeded_sites: ["CrowdWorks"], today: TODAY
    )

    ordered_urls = result.rows.map { |r| r[5] }
    assert_equal [
      "https://crowdworks.jp/public/jobs/new-1star", # 🌟が同数なら新規行が既存行より先
      "https://crowdworks.jp/public/jobs/existing-1star"
    ], ordered_urls
  end

  def test_existing_rows_preserve_original_relative_order_when_priority_ties
    existing_first = row(recommend: "🌟", category: "HTML/CSS",
                          url: "https://crowdworks.jp/public/jobs/existing-first", deadline_text: "-")
    existing_second = row(recommend: "🌟", category: "HTML/CSS",
                           url: "https://crowdworks.jp/public/jobs/existing-second", deadline_text: "-")

    result = FreelanceJobs::SheetMerger.merge(
      existing_rows: [existing_first, existing_second], new_rows: [],
      succeeded_sites: ["CrowdWorks"], today: TODAY
    )

    ordered_urls = result.rows.map { |r| r[5] }
    assert_equal [
      "https://crowdworks.jp/public/jobs/existing-first", # 🌟・新既区分・締切が全て同点なら元の順序を保持
      "https://crowdworks.jp/public/jobs/existing-second"
    ], ordered_urls
  end

  def test_existing_rows_tied_on_stars_sort_by_deadline_farthest_first_unknown_last
    existing_near = row(recommend: "🌟", category: "HTML/CSS", url: "https://crowdworks.jp/public/jobs/existing-near",
                         deadline_text: (TODAY + 10).strftime("%Y-%m-%d"))
    existing_far = row(recommend: "🌟", category: "HTML/CSS", url: "https://crowdworks.jp/public/jobs/existing-far",
                        deadline_text: (TODAY + 60).strftime("%Y-%m-%d"))
    existing_unknown = row(recommend: "🌟", category: "HTML/CSS",
                            url: "https://crowdworks.jp/public/jobs/existing-unknown", deadline_text: "-")

    result = FreelanceJobs::SheetMerger.merge(
      existing_rows: [existing_near, existing_far, existing_unknown], new_rows: [],
      succeeded_sites: ["CrowdWorks"], today: TODAY
    )

    ordered_urls = result.rows.map { |r| r[5] }
    assert_equal [
      "https://crowdworks.jp/public/jobs/existing-far", # C10より前は既存行同士は常に元の順（締切は無視）だった
      "https://crowdworks.jp/public/jobs/existing-near",
      "https://crowdworks.jp/public/jobs/existing-unknown" # 締切不明は最後
    ], ordered_urls
  end

  # === D1: ランサーズ本番対象外（WAF CAPTCHA）対応: excluded_sites ===

  def test_excluded_site_row_with_expired_deadline_is_removed
    expired_deadline_text = (TODAY - 1).strftime("%Y-%m-%d")
    lancers_row = row(site: "ランサーズ", url: "https://www.lancers.jp/work/detail/1", deadline_text: expired_deadline_text)

    result = FreelanceJobs::SheetMerger.merge(existing_rows: [lancers_row], new_rows: [],
                                               succeeded_sites: [], excluded_sites: ["ランサーズ"], today: TODAY)

    assert_equal 1, result.removed
    assert_equal 0, result.rows.size
  end

  def test_excluded_site_row_with_future_deadline_is_kept_but_auto_update_columns_stay_unchanged
    future_deadline_text = (TODAY + 30).strftime("%Y-%m-%d")
    lancers_row = row(site: "ランサーズ", url: "https://www.lancers.jp/work/detail/2", reward: "OLD_J",
                       work_format: "OLD_K", application_status: "OLD_L", deadline_text: future_deadline_text,
                       fetched_on: "OLD_O")

    result = FreelanceJobs::SheetMerger.merge(existing_rows: [lancers_row], new_rows: [],
                                               succeeded_sites: [], excluded_sites: ["ランサーズ"], today: TODAY)

    assert_equal 0, result.removed
    assert_equal 0, result.updated
    assert_equal 1, result.rows.size
    kept_row = result.rows.first
    assert_equal "OLD_J", kept_row[9], "J列(報酬)は対象外サイトなら更新されない"
    assert_equal "OLD_K", kept_row[10], "K列(形式)は対象外サイトなら更新されない"
    assert_equal "OLD_L", kept_row[11], "L列(応募状況)は対象外サイトなら更新されない"
    assert_equal future_deadline_text, kept_row[12], "M列(締切)は対象外サイトなら更新されない"
    assert_equal "OLD_O", kept_row[14], "O列(取得日時)は対象外サイトなら更新されない"
  end

  def test_row_of_a_site_neither_succeeded_nor_excluded_is_kept_even_when_expired
    expired_deadline_text = (TODAY - 1).strftime("%Y-%m-%d")
    shufti_row = row(site: "シュフティ", url: "https://www.shufti.jp/works/detail/1", deadline_text: expired_deadline_text)

    result = FreelanceJobs::SheetMerger.merge(existing_rows: [shufti_row], new_rows: [],
                                               succeeded_sites: [], excluded_sites: ["ランサーズ"], today: TODAY)

    assert_equal 0, result.removed, "取得失敗サイト(succeededにもexcludedにも無い)の行は締切超過でも削除されない"
    assert_equal 1, result.rows.size
    assert_equal "https://www.shufti.jp/works/detail/1", result.rows.first[5]
  end

  # === D2: category_order パラメータ（engineerプロファイルの並び順） ===

  def test_merge_orders_rows_by_custom_category_order_parameter
    react_row = row(category: "React", url: "https://crowdworks.jp/public/jobs/react-1")
    ruby_row = row(category: "Ruby", url: "https://crowdworks.jp/public/jobs/ruby-1")
    typescript_row = row(category: "TypeScript", url: "https://crowdworks.jp/public/jobs/ts-1")

    result = FreelanceJobs::SheetMerger.merge(
      existing_rows: [react_row, ruby_row, typescript_row], new_rows: [],
      succeeded_sites: ["CrowdWorks"], today: TODAY,
      category_order: ["Ruby", "TypeScript", "React"]
    )

    assert_equal ["Ruby", "TypeScript", "React"], result.rows.map { |merged_row| merged_row[2] }
  end

  def test_merge_uses_default_category_order_constant_when_parameter_omitted
    excel_row = row(category: "Excel・スプレッドシート", url: "https://crowdworks.jp/public/jobs/excel-1")
    html_row = row(category: "HTML/CSS", url: "https://crowdworks.jp/public/jobs/html-1")

    result = FreelanceJobs::SheetMerger.merge(
      existing_rows: [excel_row, html_row], new_rows: [],
      succeeded_sites: ["CrowdWorks"], today: TODAY
    )

    assert_equal ["HTML/CSS", "Excel・スプレッドシート"], result.rows.map { |merged_row| merged_row[2] },
                 "category_order省略時は既存のCATEGORY_ORDER（HTML/CSS→Excel）のまま変わらない想定"
  end
end
