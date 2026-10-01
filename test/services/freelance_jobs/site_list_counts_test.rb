# frozen_string_literal: true
# test/services/freelance_jobs/site_list_counts_test.rb
#
# FreelanceJobs::SiteListCounts（サイト一覧タブA列の件数文言の組み立て・通信なし）を検証する。

require_relative "../../support/freelance_jobs_loader"

class FreelanceJobsSiteListCountsTest < Minitest::Test
  SITE_COLUMN_INDEX = FreelanceJobs::SheetMerger::SITE_COLUMN_INDEX

  def row_for(site_name)
    row = Array.new(16, "")
    row[SITE_COLUMN_INDEX] = site_name
    row
  end

  def build(site_list_names:, fetched_counts: {}, sheet_rows: [], failed_sites: [], excluded_sites: [])
    FreelanceJobs::SiteListCounts.build(site_list_names: site_list_names, fetched_counts: fetched_counts,
                                         sheet_rows: sheet_rows, failed_sites: failed_sites,
                                         excluded_sites: excluded_sites)
  end

  def test_header_text
    assert_equal "案件数（実測）\n掲載=シート行数／取得=一覧取得数\n（毎朝バッチが自動更新）",
                 FreelanceJobs::SiteListCounts::HEADER_TEXT
  end

  def test_normal_site_shows_listed_and_fetched_counts_with_full_width_slash
    counts = build(site_list_names: ["Levtech"], fetched_counts: { "Levtech" => 12 },
                   sheet_rows: [row_for("Levtech"), row_for("Levtech"), row_for("Levtech")])

    assert_equal({ 0 => "掲載3件／取得12件" }, counts)
  end

  def test_excluded_site_shows_excluded_text
    counts = build(site_list_names: ["ランサーズ"], sheet_rows: [row_for("ランサーズ")], excluded_sites: ["ランサーズ"])

    assert_equal({ 0 => "除外中（掲載1件）" }, counts)
  end

  def test_failed_site_shows_failure_text
    counts = build(site_list_names: ["ランサーズ"], sheet_rows: [row_for("ランサーズ"), row_for("ランサーズ")],
                   failed_sites: ["ランサーズ"])

    assert_equal({ 0 => "取得失敗（掲載2件）" }, counts)
  end

  def test_excluded_takes_precedence_over_failed_when_both_include_the_site
    counts = build(site_list_names: ["X"], sheet_rows: [row_for("X")], failed_sites: ["X"], excluded_sites: ["X"])

    assert_equal({ 0 => "除外中（掲載1件）" }, counts)
  end

  def test_parenthesized_names_on_tab_side_match_after_normalization
    counts = build(
      site_list_names: ["クラウドテック（クラウドワークス テック）", "ココナラテック（旧フリエン）"],
      fetched_counts: { "クラウドテック" => 5, "ココナラテック" => 0 },
      sheet_rows: [row_for("クラウドテック"), row_for("ココナラテック")]
    )

    assert_equal({ 0 => "掲載1件／取得5件", 1 => "掲載1件／取得0件" }, counts)
  end

  def test_parenthesized_site_name_on_batch_side_is_normalized_too
    counts = build(site_list_names: ["ココナラ"], fetched_counts: { "ココナラ（公開依頼）" => 7 },
                   sheet_rows: [row_for("ココナラ（公開依頼）")])

    assert_equal({ 0 => "掲載1件／取得7件" }, counts)
  end

  def test_half_width_parentheses_are_removed_and_surrounding_whitespace_is_trimmed
    counts = build(site_list_names: ["  Foo (bar)  "], fetched_counts: { "Foo" => 2 }, sheet_rows: [row_for("Foo")])

    assert_equal({ 0 => "掲載1件／取得2件" }, counts)
  end

  def test_unmatched_rows_are_not_included
    counts = build(
      site_list_names: ["───── 2026-09-12 追加調査…", "", nil, "手動調査のみのサイト", "Levtech"],
      fetched_counts: { "Levtech" => 1 },
      sheet_rows: [row_for("Levtech")]
    )

    assert_equal({ 4 => "掲載1件／取得1件" }, counts)
  end

  def test_zero_listed_and_zero_fetched
    counts = build(site_list_names: ["Levtech"], fetched_counts: { "Levtech" => 0 }, sheet_rows: [])

    assert_equal({ 0 => "掲載0件／取得0件" }, counts)
  end

  def test_failed_site_with_zero_listed_rows
    counts = build(site_list_names: ["Levtech"], failed_sites: ["Levtech"])

    assert_equal({ 0 => "取得失敗（掲載0件）" }, counts)
  end

  def test_same_site_on_multiple_tab_rows_gets_text_on_each_row
    counts = build(site_list_names: ["Levtech", "手動のみ", "Levtech"], fetched_counts: { "Levtech" => 4 },
                   sheet_rows: [row_for("Levtech"), row_for("Levtech")])

    assert_equal({ 0 => "掲載2件／取得4件", 2 => "掲載2件／取得4件" }, counts)
    assert_equal [0, 2], counts.keys, "index昇順で返す"
  end

  def test_only_rows_of_the_matching_site_are_counted_as_listed
    counts = build(site_list_names: ["A", "B"], fetched_counts: { "A" => 1, "B" => 1 },
                   sheet_rows: [row_for("A"), row_for("B"), row_for("B"), row_for("手動サイト")])

    assert_equal({ 0 => "掲載1件／取得1件", 1 => "掲載2件／取得1件" }, counts)
  end

  def test_empty_inputs_return_empty_hash
    assert_equal({}, build(site_list_names: []))
  end
end
