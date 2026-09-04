# frozen_string_literal: true
# test/services/freelance_jobs/sheets_client_test.rb
#
# SheetsClientの純粋メソッド（認証不要）だけを対象にする。
# initializeはGOOGLE_APPLICATION_CREDENTIALSが無いとFetchErrorを送出するため、
# `.allocate`でinitializeを経由せずインスタンスを作り、認証を必要としないprivateメソッドを
# `.send`で直接呼び出す（これらのメソッドは@service/@spreadsheet_id等の状態を参照しない）。

require_relative "../../support/freelance_jobs_loader"

class FreelanceJobsSheetsClientTest < Minitest::Test
  def client
    FreelanceJobs::SheetsClient.allocate
  end

  # --- escape_formula ---

  def test_escape_formula_prefixes_strings_starting_with_formula_trigger_chars
    ["=SUM(A1:A2)", "+81-90-1234-5678", "-値引き", "@ユーザー"].each do |value|
      escaped = client.send(:escape_formula, value)
      assert_equal "'#{value}", escaped
    end
  end

  def test_escape_formula_leaves_normal_strings_untouched
    assert_equal "普通の文字列", client.send(:escape_formula, "普通の文字列")
    assert_equal "10,000円", client.send(:escape_formula, "10,000円")
  end

  def test_escape_formula_leaves_empty_string_untouched
    assert_equal "", client.send(:escape_formula, "")
  end

  def test_escape_formula_leaves_non_string_values_untouched
    assert_equal 0, client.send(:escape_formula, 0)
    assert_nil client.send(:escape_formula, nil)
  end

  # --- build_write_values ---

  def test_build_write_values_prepends_banner_row_padded_with_blanks
    header = ["🌟おすすめ", "No.", "分類"]
    rows = [["🌟", 1, "HTML/CSS"]]

    values = client.send(:build_write_values, "バナー本文", header, rows)

    assert_equal ["バナー本文", "", ""], values[0]
    assert_equal header, values[1]
    assert_equal rows, values[2..]
  end

  def test_build_write_values_escapes_formula_like_cells_in_every_row
    header = ["🌟おすすめ", "No.", "分類"]
    rows = [["=HACK()", 1, "-危険"]]

    values = client.send(:build_write_values, "普通のバナー", header, rows)

    assert_equal "'=HACK()", values[2][0]
    assert_equal "'-危険", values[2][2]
  end

  # --- full_width_range ---

  def test_full_width_range_spans_all_15_columns
    range = client.send(:full_width_range, 999, 2, 5)

    assert_equal 999, range[:sheet_id]
    assert_equal 2, range[:start_row_index]
    assert_equal 5, range[:end_row_index]
    assert_equal 0, range[:start_column_index]
    assert_equal FreelanceJobs::SheetsClient::COLUMN_COUNT, range[:end_column_index]
  end

  # --- basic_filter_request（A6: データ0件でも範囲が壊れない） ---

  def test_basic_filter_request_clamps_end_row_index_to_at_least_3_when_few_rows
    request = client.send(:basic_filter_request, 1, 2)
    range = request[:set_basic_filter][:filter][:range]

    assert_equal 1, range[:start_row_index]
    assert_equal 3, range[:end_row_index], "データが2行(バナー+ヘッダーのみ)でも最低3を確保する"
  end

  def test_basic_filter_request_uses_actual_row_count_when_larger_than_3
    request = client.send(:basic_filter_request, 1, 10)
    range = request[:set_basic_filter][:filter][:range]

    assert_equal 10, range[:end_row_index]
  end

  # --- starred_row_requests（データ行indexをシート行indexに+2オフセット） ---

  def test_starred_row_requests_offsets_data_row_index_by_two
    requests = client.send(:starred_row_requests, 42, [0, 3])

    assert_equal 2, requests.size
    first_range = requests[0][:repeat_cell][:range]
    assert_equal 2, first_range[:start_row_index]
    assert_equal 3, first_range[:end_row_index]

    second_range = requests[1][:repeat_cell][:range]
    assert_equal 5, second_range[:start_row_index]
    assert_equal 6, second_range[:end_row_index]
  end

  def test_starred_row_requests_empty_when_no_starred_rows
    assert_equal [], client.send(:starred_row_requests, 42, [])
  end

  # --- find_sheet ---

  FakeSheetProperties = Struct.new(:title)
  FakeSheet = Struct.new(:properties)
  FakeMetadata = Struct.new(:sheets)

  def test_find_sheet_returns_sheet_matching_title
    main_sheet = FakeSheet.new(FakeSheetProperties.new("シート1"))
    backup_sheet = FakeSheet.new(FakeSheetProperties.new("_backup_シート1"))
    metadata = FakeMetadata.new([main_sheet, backup_sheet])

    assert_same main_sheet, client.send(:find_sheet, metadata, "シート1")
    assert_same backup_sheet, client.send(:find_sheet, metadata, "_backup_シート1")
  end

  def test_find_sheet_returns_nil_when_not_found
    metadata = FakeMetadata.new([FakeSheet.new(FakeSheetProperties.new("シート1"))])

    assert_nil client.send(:find_sheet, metadata, "存在しないシート")
  end
end
