# frozen_string_literal: true
# test/services/freelance_jobs/site_list_sheet_test.rb
#
# FreelanceJobs::SiteListSheet の純粋メソッド（通信不要）だけを対象にする。
# initializeは認証を要するため、`.allocate`でinitializeを経由せず`.send`で直接呼ぶ
# （sheets_client_test.rb と同じ流儀）。実Sheetsへは一切通信しない。

require_relative "../../support/freelance_jobs_loader"

class FreelanceJobsSiteListSheetTest < Minitest::Test
  HEADER_TEXT = FreelanceJobs::SiteListCounts::HEADER_TEXT

  def sheet
    FreelanceJobs::SiteListSheet.allocate
  end

  # --- header_row_index ---

  def test_header_row_index_returns_zero_based_index_of_first_row_whose_b_column_is_site_name
    values = [["", "タイトル"], ["案件数", "サイト名"], ["", "Levtech"]]

    assert_equal 1, sheet.send(:header_row_index, values)
  end

  def test_header_row_index_returns_first_match_when_multiple_header_rows_exist
    values = [["", "サイト名"], ["", "サイト名"]]

    assert_equal 0, sheet.send(:header_row_index, values)
  end

  def test_header_row_index_tolerates_short_rows_missing_the_b_cell
    values = [[], ["A列だけ"], ["", "サイト名"]]

    assert_equal 2, sheet.send(:header_row_index, values)
  end

  def test_header_row_index_ignores_site_name_in_a_column
    values = [["サイト名"], ["サイト名", "別の見出し"]]

    assert_raises(FreelanceJobs::FetchError) { sheet.send(:header_row_index, values) }
  end

  def test_header_row_index_raises_fetch_error_when_not_found
    assert_raises(FreelanceJobs::FetchError) { sheet.send(:header_row_index, [["", "Levtech"]]) }
  end

  def test_header_row_index_raises_fetch_error_for_empty_values
    assert_raises(FreelanceJobs::FetchError) { sheet.send(:header_row_index, []) }
  end

  # --- site_list_names ---

  def test_site_list_names_returns_b_column_below_header_with_nil_for_missing_cells
    values = [["", "サイト名"], ["x", "Levtech"], ["y"], [], ["", "Bizlink"]]

    assert_equal ["Levtech", nil, nil, "Bizlink"], sheet.send(:site_list_names, values, 0)
  end

  def test_site_list_names_skips_rows_above_the_header
    values = [["", "上の行"], ["", "サイト名"], ["", "Levtech"]]

    assert_equal ["Levtech"], sheet.send(:site_list_names, values, 1)
  end

  def test_site_list_names_is_empty_when_header_is_last_row
    assert_equal [], sheet.send(:site_list_names, [["", "サイト名"]], 0)
  end

  # --- build_column_values ---

  def test_build_column_values_starts_with_header_text_and_writes_counts_text_on_matched_rows
    values = [["", "サイト名"], ["古い", "Levtech"], ["古い2", "Bizlink"]]

    column = sheet.send(:build_column_values, values, 0, { 0 => "掲載1件／取得2件", 1 => "取得失敗（掲載0件）" })

    assert_equal [[HEADER_TEXT], ["掲載1件／取得2件"], ["取得失敗（掲載0件）"]], column
  end

  def test_build_column_values_writes_back_existing_a_value_for_unmatched_rows
    values = [["", "サイト名"], ["手書きメモ", "手動のみ"], ["古い", "Levtech"]]

    column = sheet.send(:build_column_values, values, 0, { 1 => "掲載1件／取得1件" })

    assert_equal [[HEADER_TEXT], ["手書きメモ"], ["掲載1件／取得1件"]], column
  end

  def test_build_column_values_writes_empty_string_for_rows_missing_the_a_cell
    values = [["", "サイト名"], [], ["", "手動のみ"]]

    column = sheet.send(:build_column_values, values, 0, {})

    assert_equal [[HEADER_TEXT], [""], [""]], column
  end

  def test_build_column_values_starts_from_the_header_row_not_from_the_top
    values = [["上の行", "タイトル"], ["案件数", "サイト名"], ["", "Levtech"]]

    column = sheet.send(:build_column_values, values, 1, { 0 => "掲載1件／取得1件" })

    assert_equal [[HEADER_TEXT], ["掲載1件／取得1件"]], column
    assert_equal 2, column.size, "ヘッダー行から最終行まで（ヘッダーより上は含めない）"
  end

  def test_build_column_values_with_header_only_returns_just_header_cell
    assert_equal [[HEADER_TEXT]], sheet.send(:build_column_values, [["", "サイト名"]], 0, {})
  end

  # --- update（Fake の Sheets サービスで通信なしに通す） ---

  # update が呼ぶ 3 つの API のシグネチャに合わせ、引数を記録する。
  class FakeSheetsService
    Properties = Struct.new(:sheet_id, :title)
    SheetEntry = Struct.new(:properties)
    Metadata = Struct.new(:sheets)
    ValuesResponse = Struct.new(:values)

    attr_reader :read_ranges, :writes

    def initialize(sheet_gid:, sheet_title:, values:)
      @sheet_gid = sheet_gid
      @sheet_title = sheet_title
      @values = values
      @read_ranges = []
      @writes = []
    end

    def get_spreadsheet(_spreadsheet_id, fields:)
      Metadata.new([SheetEntry.new(Properties.new(@sheet_gid, @sheet_title))])
    end

    def get_spreadsheet_values(_spreadsheet_id, range, *)
      @read_ranges << range
      ValuesResponse.new(@values)
    end

    def update_spreadsheet_value(spreadsheet_id, range, value_range, value_input_option:)
      @writes << { spreadsheet_id: spreadsheet_id, range: range, values: value_range.values,
                   value_input_option: value_input_option }
    end
  end

  SHEET_GID = 777
  SHEET_TITLE = "エンジニア申込サイト一覧"

  def sheet_with(values:, sheet_gid: SHEET_GID)
    service = FakeSheetsService.new(sheet_gid: sheet_gid, sheet_title: SHEET_TITLE, values: values)
    instance = FreelanceJobs::SiteListSheet.allocate
    instance.instance_variable_set(:@service, service)
    instance.instance_variable_set(:@spreadsheet_id, "spreadsheet-1")
    instance.instance_variable_set(:@sheet_gid, SHEET_GID)
    [instance, service]
  end

  def run_update(instance)
    instance.update(
      fetched_counts: { "Levtech" => 5 },
      sheet_rows: [Array.new(FreelanceJobs::SheetMerger::SITE_COLUMN_INDEX + 1, "Levtech")],
      failed_sites: [],
      excluded_sites: []
    )
  end

  def test_update_with_header_on_second_row_reads_and_writes_expected_ranges_and_values
    values = [["", "タイトル"], ["案件数", "サイト名"], ["古い", "Levtech"], ["区切り", ""], ["手書きメモ", "手動のみ"]]
    instance, service = sheet_with(values: values)

    updated_row_count = run_update(instance)

    assert_equal ["'#{SHEET_TITLE}'!A1:B200"], service.read_ranges
    assert_equal 1, service.writes.size
    write = service.writes.first
    assert_equal "spreadsheet-1", write[:spreadsheet_id]
    assert_equal "'#{SHEET_TITLE}'!A2:A5", write[:range], "ヘッダー行(2行目)から最終行(5行目)まで"
    assert_equal "RAW", write[:value_input_option]
    assert_equal [[HEADER_TEXT], ["掲載1件／取得5件"], ["区切り"], ["手書きメモ"]], write[:values]
    assert_equal 4, updated_row_count
    assert_kind_of Integer, updated_row_count
  end

  def test_update_with_header_on_third_row_starts_write_range_at_a3
    values = [["", "タイトル"], ["", "注記"], ["案件数", "サイト名"], ["古い", "Levtech"]]
    instance, service = sheet_with(values: values)

    updated_row_count = run_update(instance)

    assert_equal "'#{SHEET_TITLE}'!A3:A4", service.writes.first[:range], "off-by-one 防止: 0始まり index 2 -> 3行目"
    assert_equal 2, updated_row_count
  end

  def test_update_raises_fetch_error_and_does_not_write_when_gid_matches_no_tab
    instance, service = sheet_with(values: [["", "サイト名"]])
    instance.instance_variable_set(:@sheet_gid, 999)

    assert_raises(FreelanceJobs::FetchError) { run_update(instance) }
    assert_empty service.writes
  end

  def test_update_raises_fetch_error_and_does_not_write_when_header_is_missing
    instance, service = sheet_with(values: [["", "タイトル"], ["古い", "Levtech"]])

    assert_raises(FreelanceJobs::FetchError) { run_update(instance) }
    assert_empty service.writes
  end
end
