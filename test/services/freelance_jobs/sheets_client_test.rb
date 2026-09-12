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
  # 行モデルはROW_COLUMN_COUNT(15)列で、index URL_COLUMN_INDEX(5)に案件URLを持つ。
  # build_write_valuesはこの案件URL列を落として、シート上のSHEET_COLUMN_COUNT(14)列で書く。

  def full_width_row_model(overrides = {})
    row = Array.new(FreelanceJobs::SheetsClient::ROW_COLUMN_COUNT) { |index| "値#{index}" }
    overrides.each { |index, value| row[index] = value }
    row
  end

  def test_build_write_values_prepends_banner_row_with_sidekiq_link_label_and_padded_blanks
    header = full_width_row_model(0 => "🌟おすすめ")
    rows = [full_width_row_model]

    values = client.send(:build_write_values, "バナー本文", header, rows)

    expected_banner_row = [FreelanceJobs::SheetsClient::SIDEKIQ_LINK_LABEL, "バナー本文"] +
                           Array.new(FreelanceJobs::SheetsClient::SHEET_COLUMN_COUNT - 2, "")
    assert_equal expected_banner_row, values[0], "A1はSidekiqリンク用ラベル、B1がバナー本文の想定"
    assert_equal FreelanceJobs::SheetsClient::SHEET_COLUMN_COUNT, values[0].size
  end

  def test_build_write_values_drops_url_column_from_header_and_rows
    header = full_width_row_model(0 => "🌟おすすめ")
    rows = [full_width_row_model(5 => "https://example.com/job")]

    values = client.send(:build_write_values, "バナー本文", header, rows)

    assert_equal FreelanceJobs::SheetsClient::SHEET_COLUMN_COUNT, values[1].size, "ヘッダーも14列に落ちる想定"
    assert_equal FreelanceJobs::SheetsClient::SHEET_COLUMN_COUNT, values[2].size, "データ行も14列に落ちる想定"
    refute_includes values[2], "https://example.com/job", "案件URL列(index5)はシート書き込み値から落ちる想定"
    assert_equal "値4", values[2][4], "URL列より前の列はindexがずれない"
    assert_equal "値6", values[2][5], "URL列より後の列はindexが1つ前にずれる"
  end

  def test_build_write_values_escapes_formula_like_cells_in_every_row
    header = full_width_row_model(0 => "🌟おすすめ")
    rows = [full_width_row_model(0 => "=HACK()", 6 => "-危険")]

    values = client.send(:build_write_values, "普通のバナー", header, rows)

    assert_equal "'=HACK()", values[2][0]
    assert_equal "'-危険", values[2][5], "index6はURL列(index5)除去後にindex5へずれる"
  end

  # --- strip_url_column ---

  def test_strip_url_column_converts_15_column_row_model_to_14_column_sheet_row
    row = full_width_row_model(5 => "https://example.com/job")

    sheet_row = client.send(:strip_url_column, row)

    assert_equal 14, sheet_row.size
    refute_includes sheet_row, "https://example.com/job"
    assert_equal "値4", sheet_row[4]
    assert_equal "値6", sheet_row[5]
  end

  # --- full_width_range ---

  def test_full_width_range_spans_all_14_sheet_columns
    range = client.send(:full_width_range, 999, 2, 5)

    assert_equal 999, range[:sheet_id]
    assert_equal 2, range[:start_row_index]
    assert_equal 5, range[:end_row_index]
    assert_equal 0, range[:start_column_index]
    assert_equal FreelanceJobs::SheetsClient::SHEET_COLUMN_COUNT, range[:end_column_index]
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

  # === D2: gidによるシート名解決 & 案件名セルのハイパーリンク復元 ===
  # initializeを経由せずに@service/@spreadsheet_id/@sheet_gidを注入し、
  # 認証不要なフェイクGoogle Sheets APIサービスで read_rows / backup_sheet_name を検証する。
  # フェイクサービスはgoogle-apis-sheets_v4の実クラス（Spreadsheet/GridData/RowData/
  # CellData/CellFormat/TextFormat/Link）をそのまま使い、get_spreadsheetの呼ばれ方
  # （rangesの有無）でメタデータ取得とグリッドデータ取得を判別する。

  GidFakeSheetProperties = Struct.new(:sheet_id, :title)
  GidFakeSheet = Struct.new(:properties)
  GidFakeMetadata = Struct.new(:sheets)

  # get_spreadsheetは呼び出し方が2種類ある:
  #   ・メタデータ取得: get_spreadsheet(id, include_grid_data: false) → sheets/properties/sheet_id/titleだけ持つ
  #   ・グリッドデータ取得: get_spreadsheet(id, ranges:, include_grid_data: true, fields:) → セル値まで持つ
  # rangesの有無で応答を出し分けることで、実サービスと同じメソッドシグネチャのまま両方を検証する。
  class GidFakeGoogleSheetsService
    attr_reader :get_spreadsheet_calls

    def initialize(metadata:, grid_spreadsheet: nil)
      @metadata = metadata
      @grid_spreadsheet = grid_spreadsheet
      @get_spreadsheet_calls = []
    end

    def get_spreadsheet(spreadsheet_id, include_grid_data: false, ranges: nil, fields: nil)
      @get_spreadsheet_calls << {
        spreadsheet_id: spreadsheet_id, include_grid_data: include_grid_data, ranges: ranges, fields: fields
      }
      ranges ? @grid_spreadsheet : @metadata
    end
  end

  def build_client_with_fake_service(spreadsheet_id:, sheet_gid:, service:)
    client_instance = FreelanceJobs::SheetsClient.allocate
    client_instance.instance_variable_set(:@spreadsheet_id, spreadsheet_id)
    client_instance.instance_variable_set(:@sheet_gid, sheet_gid)
    client_instance.instance_variable_set(:@service, service)
    client_instance
  end

  # 14セル（A〜N）を作る。titleセルにだけhyperlink/リンク書式を差し込めるようにする。
  def build_sheet_cell(formatted_value, hyperlink: nil, link_uri: nil)
    user_entered_format = if link_uri
      Google::Apis::SheetsV4::CellFormat.new(
        text_format: Google::Apis::SheetsV4::TextFormat.new(link: Google::Apis::SheetsV4::Link.new(uri: link_uri))
      )
    end
    Google::Apis::SheetsV4::CellData.new(formatted_value: formatted_value, hyperlink: hyperlink,
                                          user_entered_format: user_entered_format)
  end

  def build_sheet_row(title_hyperlink: nil, title_link_uri: nil)
    Array.new(FreelanceJobs::SheetsClient::SHEET_COLUMN_COUNT) do |index|
      if index == FreelanceJobs::SheetsClient::TITLE_COLUMN_INDEX
        build_sheet_cell("案件#{index}", hyperlink: title_hyperlink, link_uri: title_link_uri)
      else
        build_sheet_cell("値#{index}")
      end
    end
  end

  def build_grid_spreadsheet(rows_of_cells)
    row_data = rows_of_cells.map { |cells| Google::Apis::SheetsV4::RowData.new(values: cells) }
    sheet = Google::Apis::SheetsV4::Sheet.new(data: [Google::Apis::SheetsV4::GridData.new(row_data: row_data)])
    Google::Apis::SheetsV4::Spreadsheet.new(sheets: [sheet])
  end

  def test_read_rows_resolves_sheet_name_by_gid_and_requests_grid_data_with_expected_range_and_fields
    metadata = GidFakeMetadata.new([
      GidFakeSheet.new(GidFakeSheetProperties.new(0, "HTML CSS求人")),
      GidFakeSheet.new(GidFakeSheetProperties.new(1_065_736_587, "Ruby TypeScript 求人"))
    ])
    grid_spreadsheet = build_grid_spreadsheet([build_sheet_row(title_hyperlink: "https://example.com/a")])
    service = GidFakeGoogleSheetsService.new(metadata: metadata, grid_spreadsheet: grid_spreadsheet)
    client_instance = build_client_with_fake_service(spreadsheet_id: "SHEET_ID", sheet_gid: 1_065_736_587,
                                                      service: service)

    client_instance.read_rows

    grid_call = service.get_spreadsheet_calls.find { |call| call[:ranges] }
    assert_equal "SHEET_ID", grid_call[:spreadsheet_id]
    assert_equal ["'Ruby TypeScript 求人'!A1:N2000"], grid_call[:ranges],
                 "gid 1065736587 に対応するシート名で範囲文字列を組み立てる想定"
    assert grid_call[:include_grid_data]
    assert_equal "sheets.data.rowData.values(formattedValue,hyperlink,userEnteredFormat.textFormat.link)",
                 grid_call[:fields]
  end

  def test_read_rows_resolves_gid_zero_to_its_own_sheet_name_not_the_first_sheet
    metadata = GidFakeMetadata.new([
      GidFakeSheet.new(GidFakeSheetProperties.new(1_065_736_587, "Ruby TypeScript 求人")),
      GidFakeSheet.new(GidFakeSheetProperties.new(0, "HTML CSS求人"))
    ])
    grid_spreadsheet = build_grid_spreadsheet([])
    service = GidFakeGoogleSheetsService.new(metadata: metadata, grid_spreadsheet: grid_spreadsheet)
    client_instance = build_client_with_fake_service(spreadsheet_id: "SHEET_ID", sheet_gid: 0, service: service)

    client_instance.read_rows

    grid_call = service.get_spreadsheet_calls.find { |call| call[:ranges] }
    assert_equal ["'HTML CSS求人'!A1:N2000"], grid_call[:ranges],
                 "gid=0はsheets配列内の並び順ではなくsheet_idの一致で解決する想定"
  end

  def test_read_rows_inserts_title_cell_hyperlink_into_url_column_index_producing_15_columns
    metadata = GidFakeMetadata.new([GidFakeSheet.new(GidFakeSheetProperties.new(0, "HTML CSS求人"))])
    grid_spreadsheet = build_grid_spreadsheet([build_sheet_row(title_hyperlink: "https://example.com/hyperlink")])
    service = GidFakeGoogleSheetsService.new(metadata: metadata, grid_spreadsheet: grid_spreadsheet)
    client_instance = build_client_with_fake_service(spreadsheet_id: "SHEET_ID", sheet_gid: 0, service: service)

    rows = client_instance.read_rows

    assert_equal 1, rows.size
    assert_equal FreelanceJobs::SheetsClient::ROW_COLUMN_COUNT, rows.first.size, "URL列を挟んで15列になる想定"
    assert_equal "https://example.com/hyperlink", rows.first[FreelanceJobs::SheetsClient::URL_COLUMN_INDEX]
    assert_equal "案件3", rows.first[FreelanceJobs::SheetsClient::TITLE_COLUMN_INDEX],
                 "案件名セルの表示値自体はhyperlinkの有無に関わらずそのまま"
  end

  def test_read_rows_falls_back_to_user_entered_format_link_when_hyperlink_field_is_absent
    metadata = GidFakeMetadata.new([GidFakeSheet.new(GidFakeSheetProperties.new(0, "HTML CSS求人"))])
    grid_spreadsheet = build_grid_spreadsheet([build_sheet_row(title_link_uri: "https://example.com/text-format-link")])
    service = GidFakeGoogleSheetsService.new(metadata: metadata, grid_spreadsheet: grid_spreadsheet)
    client_instance = build_client_with_fake_service(spreadsheet_id: "SHEET_ID", sheet_gid: 0, service: service)

    rows = client_instance.read_rows

    assert_equal "https://example.com/text-format-link", rows.first[FreelanceJobs::SheetsClient::URL_COLUMN_INDEX]
  end

  def test_read_rows_sets_empty_string_url_when_title_cell_has_no_link
    metadata = GidFakeMetadata.new([GidFakeSheet.new(GidFakeSheetProperties.new(0, "HTML CSS求人"))])
    grid_spreadsheet = build_grid_spreadsheet([build_sheet_row])
    service = GidFakeGoogleSheetsService.new(metadata: metadata, grid_spreadsheet: grid_spreadsheet)
    client_instance = build_client_with_fake_service(spreadsheet_id: "SHEET_ID", sheet_gid: 0, service: service)

    rows = client_instance.read_rows

    assert_equal "", rows.first[FreelanceJobs::SheetsClient::URL_COLUMN_INDEX]
  end

  def test_read_rows_drops_trailing_fully_blank_rows
    metadata = GidFakeMetadata.new([GidFakeSheet.new(GidFakeSheetProperties.new(0, "HTML CSS求人"))])
    blank_row = Array.new(FreelanceJobs::SheetsClient::SHEET_COLUMN_COUNT) { build_sheet_cell("") }
    grid_spreadsheet = build_grid_spreadsheet([build_sheet_row, blank_row])
    service = GidFakeGoogleSheetsService.new(metadata: metadata, grid_spreadsheet: grid_spreadsheet)
    client_instance = build_client_with_fake_service(spreadsheet_id: "SHEET_ID", sheet_gid: 0, service: service)

    rows = client_instance.read_rows

    assert_equal 1, rows.size, "末尾の全列空の行は落とす想定"
  end

  def test_read_rows_returns_empty_array_when_no_row_data
    metadata = GidFakeMetadata.new([GidFakeSheet.new(GidFakeSheetProperties.new(0, "HTML CSS求人"))])
    grid_spreadsheet = build_grid_spreadsheet([])
    service = GidFakeGoogleSheetsService.new(metadata: metadata, grid_spreadsheet: grid_spreadsheet)
    client_instance = build_client_with_fake_service(spreadsheet_id: "SHEET_ID", sheet_gid: 0, service: service)

    assert_equal [], client_instance.read_rows
  end

  def test_read_rows_raises_fetch_error_when_gid_not_found
    metadata = GidFakeMetadata.new([GidFakeSheet.new(GidFakeSheetProperties.new(0, "HTML CSS求人"))])
    service = GidFakeGoogleSheetsService.new(metadata: metadata)
    client_instance = build_client_with_fake_service(spreadsheet_id: "SHEET_ID", sheet_gid: 999_999, service: service)

    error = assert_raises(FreelanceJobs::FetchError) { client_instance.read_rows }
    assert_equal "Sheet gid not found: 999999", error.message
  end

  def test_sheet_name_resolution_is_memoized_across_multiple_read_rows_calls
    metadata = GidFakeMetadata.new([GidFakeSheet.new(GidFakeSheetProperties.new(0, "HTML CSS求人"))])
    grid_spreadsheet = build_grid_spreadsheet([])
    service = GidFakeGoogleSheetsService.new(metadata: metadata, grid_spreadsheet: grid_spreadsheet)
    client_instance = build_client_with_fake_service(spreadsheet_id: "SHEET_ID", sheet_gid: 0, service: service)

    client_instance.read_rows
    client_instance.read_rows

    metadata_calls = service.get_spreadsheet_calls.reject { |call| call[:ranges] }
    assert_equal 1, metadata_calls.size, "gid解決のためのメタデータ取得は初回の1回だけの想定"
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

  # --- title_link_cell ---

  def test_title_link_cell_sets_link_and_underline_when_url_present
    cell = client.send(:title_link_cell, "https://example.com/job")
    text_format = cell[:user_entered_format][:text_format]

    assert_equal({ uri: "https://example.com/job" }, text_format[:link])
    assert_equal FreelanceJobs::SheetsClient::LINK_FOREGROUND_COLOR, text_format[:foreground_color]
    assert_equal true, text_format[:underline]
  end

  def test_title_link_cell_clears_link_when_url_is_blank
    [nil, "", "   "].each do |blank_url|
      cell = client.send(:title_link_cell, blank_url)
      text_format = cell[:user_entered_format][:text_format]

      assert_nil text_format[:link]
      assert_equal false, text_format[:underline]
    end
  end

  # --- title_link_requests ---
  # 案件名セル(D列)へのハイパーリンク付与は、行位置ごとの張り替えを1リクエストにまとめる。

  def test_title_link_requests_returns_single_request_with_link_for_url_present_rows_and_nil_link_for_blank_url_rows
    rows = [
      full_width_row_model(5 => "https://example.com/a"),
      full_width_row_model(5 => "")
    ]

    requests = client.send(:title_link_requests, 42, rows, 2)

    assert_equal 1, requests.size, "1リクエストにまとめる想定"
    update_cells = requests.first[:update_cells]
    range = update_cells[:range]
    assert_equal 42, range[:sheet_id]
    assert_equal 2, range[:start_row_index]
    assert_equal 4, range[:end_row_index]
    assert_equal FreelanceJobs::SheetsClient::TITLE_COLUMN_INDEX, range[:start_column_index]
    assert_equal FreelanceJobs::SheetsClient::TITLE_COLUMN_INDEX + 1, range[:end_column_index]
    assert_equal "userEnteredFormat.textFormat(link,foregroundColor,underline)", update_cells[:fields]

    first_cell_text_format = update_cells[:rows][0][:values][0][:user_entered_format][:text_format]
    assert_equal({ uri: "https://example.com/a" }, first_cell_text_format[:link])

    second_cell_text_format = update_cells[:rows][1][:values][0][:user_entered_format][:text_format]
    assert_nil second_cell_text_format[:link]
  end

  def test_title_link_requests_covers_leftover_rows_when_previous_row_count_is_larger_than_new_rows
    rows = [full_width_row_model(5 => "https://example.com/a")]

    # previous_row_count=5(バナー+ヘッダー+データ3行相当)に対し新しい行は1件だけなので、
    # 余った2行分もリンク解除セルとして含める想定(link_row_count = max(1, 5-2) = 3)。
    requests = client.send(:title_link_requests, 42, rows, 5)

    update_cells = requests.first[:update_cells]
    assert_equal 3, update_cells[:rows].size
    assert_equal 2, update_cells[:range][:start_row_index]
    assert_equal 5, update_cells[:range][:end_row_index]

    leftover_row_text_formats = update_cells[:rows][1..].map { |row| row[:values][0][:user_entered_format][:text_format] }
    assert leftover_row_text_formats.all? { |text_format| text_format[:link].nil? },
           "新しい行数を超える余剰行はリンクを解除するセルになる想定"
  end

  def test_title_link_requests_returns_empty_array_when_no_rows_and_no_previous_data_rows
    requests = client.send(:title_link_requests, 42, [], 2)

    assert_equal [], requests
  end
end
