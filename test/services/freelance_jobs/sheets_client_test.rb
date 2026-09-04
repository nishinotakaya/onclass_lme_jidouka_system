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

  # === D2: gidによるシート名解決 ===
  # initializeを経由せずに@service/@spreadsheet_id/@sheet_gidを注入し、
  # 認証不要なフェイクGoogle Sheets APIサービスで read_values / backup_sheet_name を検証する。

  GidFakeSheetProperties = Struct.new(:sheet_id, :title)
  GidFakeSheet = Struct.new(:properties)
  GidFakeMetadata = Struct.new(:sheets)
  GidFakeValuesResponse = Struct.new(:values)

  class GidFakeGoogleSheetsService
    attr_reader :get_spreadsheet_calls, :get_spreadsheet_values_calls

    def initialize(metadata:, values_response: nil)
      @metadata = metadata
      @values_response = values_response
      @get_spreadsheet_calls = []
      @get_spreadsheet_values_calls = []
    end

    def get_spreadsheet(spreadsheet_id, include_grid_data:)
      @get_spreadsheet_calls << { spreadsheet_id: spreadsheet_id, include_grid_data: include_grid_data }
      @metadata
    end

    def get_spreadsheet_values(spreadsheet_id, range)
      @get_spreadsheet_values_calls << { spreadsheet_id: spreadsheet_id, range: range }
      @values_response
    end
  end

  def build_client_with_fake_service(spreadsheet_id:, sheet_gid:, service:)
    client_instance = FreelanceJobs::SheetsClient.allocate
    client_instance.instance_variable_set(:@spreadsheet_id, spreadsheet_id)
    client_instance.instance_variable_set(:@sheet_gid, sheet_gid)
    client_instance.instance_variable_set(:@service, service)
    client_instance
  end

  def test_read_values_resolves_sheet_name_by_gid_and_returns_values
    metadata = GidFakeMetadata.new([
      GidFakeSheet.new(GidFakeSheetProperties.new(0, "HTML CSS求人")),
      GidFakeSheet.new(GidFakeSheetProperties.new(1_065_736_587, "Ruby TypeScript 求人"))
    ])
    service = GidFakeGoogleSheetsService.new(metadata: metadata, values_response: GidFakeValuesResponse.new([["a", "b"]]))
    client_instance = build_client_with_fake_service(spreadsheet_id: "SHEET_ID", sheet_gid: 1_065_736_587, service: service)

    result = client_instance.read_values("A1:O2000")

    assert_equal [["a", "b"]], result
    assert_equal "SHEET_ID", service.get_spreadsheet_values_calls.first[:spreadsheet_id]
    assert_equal "'Ruby TypeScript 求人'!A1:O2000", service.get_spreadsheet_values_calls.first[:range],
                 "gid 1065736587 に対応するシート名で範囲文字列を組み立てる想定"
  end

  def test_read_values_resolves_gid_zero_to_its_own_sheet_name_not_the_first_sheet
    metadata = GidFakeMetadata.new([
      GidFakeSheet.new(GidFakeSheetProperties.new(1_065_736_587, "Ruby TypeScript 求人")),
      GidFakeSheet.new(GidFakeSheetProperties.new(0, "HTML CSS求人"))
    ])
    service = GidFakeGoogleSheetsService.new(metadata: metadata, values_response: GidFakeValuesResponse.new([]))
    client_instance = build_client_with_fake_service(spreadsheet_id: "SHEET_ID", sheet_gid: 0, service: service)

    client_instance.read_values("A1:O10")

    assert_equal "'HTML CSS求人'!A1:O10", service.get_spreadsheet_values_calls.first[:range],
                 "gid=0はsheets配列内の並び順ではなくsheet_idの一致で解決する想定"
  end

  def test_read_values_returns_empty_array_when_response_values_is_nil
    metadata = GidFakeMetadata.new([GidFakeSheet.new(GidFakeSheetProperties.new(0, "HTML CSS求人"))])
    service = GidFakeGoogleSheetsService.new(metadata: metadata, values_response: GidFakeValuesResponse.new(nil))
    client_instance = build_client_with_fake_service(spreadsheet_id: "SHEET_ID", sheet_gid: 0, service: service)

    assert_equal [], client_instance.read_values("A1:O2000")
  end

  def test_read_values_raises_fetch_error_when_gid_not_found
    metadata = GidFakeMetadata.new([GidFakeSheet.new(GidFakeSheetProperties.new(0, "HTML CSS求人"))])
    service = GidFakeGoogleSheetsService.new(metadata: metadata)
    client_instance = build_client_with_fake_service(spreadsheet_id: "SHEET_ID", sheet_gid: 999_999, service: service)

    error = assert_raises(FreelanceJobs::FetchError) { client_instance.read_values("A1:O2000") }
    assert_equal "Sheet gid not found: 999999", error.message
  end

  def test_sheet_name_resolution_is_memoized_across_multiple_read_values_calls
    metadata = GidFakeMetadata.new([GidFakeSheet.new(GidFakeSheetProperties.new(0, "HTML CSS求人"))])
    service = GidFakeGoogleSheetsService.new(metadata: metadata, values_response: GidFakeValuesResponse.new([]))
    client_instance = build_client_with_fake_service(spreadsheet_id: "SHEET_ID", sheet_gid: 0, service: service)

    client_instance.read_values("A1:O10")
    client_instance.read_values("A1:O20")

    assert_equal 1, service.get_spreadsheet_calls.size, "gid解決のためのメタデータ取得は初回の1回だけの想定"
  end

  # --- backup_sheet_name（gidごとに一意。@serviceに依存しない純粋メソッド） ---

  def test_backup_sheet_name_includes_gid_for_beginner_sheet
    client_instance = build_client_with_fake_service(spreadsheet_id: "SHEET_ID", sheet_gid: 0, service: nil)

    assert_equal "_backup_gid0", client_instance.backup_sheet_name
  end

  def test_backup_sheet_name_includes_gid_for_engineer_sheet
    client_instance = build_client_with_fake_service(spreadsheet_id: "SHEET_ID", sheet_gid: 1_065_736_587, service: nil)

    assert_equal "_backup_gid1065736587", client_instance.backup_sheet_name
  end
end
