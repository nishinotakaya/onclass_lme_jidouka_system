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

  # === AC-03: 行モデルに「追加日」列を足して16列にする ===

  def test_row_column_count_is_16
    assert_equal 16, FreelanceJobs::SheetsClient::ROW_COLUMN_COUNT
  end

  def test_base_sheet_column_count_is_15
    assert_equal 15, FreelanceJobs::SheetsClient::BASE_SHEET_COLUMN_COUNT
  end

  # 従来14列に「追加日」ぶんの90pxが1つ増える。
  def test_column_widths_has_15_entries_including_the_added_on_column
    assert_equal 15, FreelanceJobs::SheetsClient::COLUMN_WIDTHS.size
    assert_equal 90, FreelanceJobs::SheetsClient::COLUMN_WIDTHS.last, "追加日列の幅は取得日時と同じ90px想定"
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

  def test_build_write_values_prepends_banner_row_with_sidekiq_link_visible_count_and_padded_blanks
    header = full_width_row_model(0 => "🌟おすすめ")
    rows = [full_width_row_model]

    values = client.send(:build_write_values, "バナー本文", header, rows)

    expected_banner_row = [FreelanceJobs::SheetsClient::SIDEKIQ_LINK_LABEL,
                           %(=SUBTOTAL(103,$C$3:$C$3)&"件"),
                           "バナー本文"] +
                           Array.new(FreelanceJobs::SheetsClient::BASE_SHEET_COLUMN_COUNT - 3, "")
    assert_equal expected_banner_row, values[0],
                 "A1はSidekiqリンク用ラベル、B1はフィルター後の表示件数、C1がバナー本文の想定"
    assert_equal FreelanceJobs::SheetsClient::BASE_SHEET_COLUMN_COUNT, values[0].size
  end

  # --- visible_count_formula（B1のフィルター後件数） ---

  def test_visible_count_formula_counts_only_visible_rows_from_the_first_data_row
    formula = client.send(:visible_count_formula, 300)

    assert_equal %(=SUBTOTAL(103,$C$3:$C$302)&"件"), formula,
                 "データは3行目から始まり、300件なら302行目までを数える想定"
  end

  def test_visible_count_formula_returns_plain_text_when_there_is_no_data_row
    assert_equal "0件", client.send(:visible_count_formula, 0),
                 "0件のときは範囲が作れないので数式にしない想定"
  end

  def test_build_write_values_keeps_visible_count_formula_unescaped
    header = full_width_row_model(0 => "🌟おすすめ")
    rows = [full_width_row_model]

    values = client.send(:build_write_values, "バナー本文", header, rows)
    visible_count_cell = values[0][FreelanceJobs::SheetsClient::BASE_VISIBLE_COUNT_COLUMN_INDEX]

    assert visible_count_cell.start_with?("=SUBTOTAL("),
           "件数セルは数式として評価させたいので ' を付けない想定: #{visible_count_cell}"
  end

  def test_build_write_values_drops_url_column_from_header_and_rows
    header = full_width_row_model(0 => "🌟おすすめ")
    rows = [full_width_row_model(5 => "https://example.com/job")]

    values = client.send(:build_write_values, "バナー本文", header, rows)

    assert_equal FreelanceJobs::SheetsClient::BASE_SHEET_COLUMN_COUNT, values[1].size, "ヘッダーも14列に落ちる想定"
    assert_equal FreelanceJobs::SheetsClient::BASE_SHEET_COLUMN_COUNT, values[2].size, "データ行も14列に落ちる想定"
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

  # --- replace_sheet の呼び出し順（結合解除 → 値の書き込み） ---
  #
  # 結合されたセル範囲へ値を書くと左上以外のセルへの書き込みは黙って捨てられる。
  # バナーの結合開始列を変えた回に、新しい位置のセルが空のままになるのを防ぐための回帰テスト。

  # 呼ばれたSheets APIの順番だけを記録する最小のフェイク。
  class CallOrderRecordingService
    Properties = Struct.new(:sheet_id, :title, :grid_properties, keyword_init: true)
    GridProperties = Struct.new(:row_count, keyword_init: true)
    SheetStub = Struct.new(:properties, :merges, :basic_filter, :conditional_formats, keyword_init: true)
    MergeRange = Struct.new(:start_row_index, :end_row_index, :start_column_index, :end_column_index,
                             keyword_init: true)

    attr_reader :calls

    def initialize
      @calls = []
    end

    def get_spreadsheet(_spreadsheet_id, **_options)
      @calls << :get_spreadsheet
      banner_merge = MergeRange.new(start_row_index: 0, end_row_index: 1,
                                     start_column_index: 1, end_column_index: 14)
      sheet = SheetStub.new(
        properties: Properties.new(sheet_id: 123, title: "求人",
                                    grid_properties: GridProperties.new(row_count: 10)),
        merges: [banner_merge],
        basic_filter: nil
      )
      Struct.new(:sheets, keyword_init: true).new(sheets: [sheet])
    end

    def batch_update_spreadsheet(_spreadsheet_id, request_body, **_options)
      kinds = request_body.requests.map { |request| request.keys.first }
      @calls << (kinds.include?(:unmerge_cells) ? :unmerge : :batch_update)
    end

    def update_spreadsheet_value(_spreadsheet_id, range, _body, **_options)
      @calls << (range.include?("_backup_") ? :backup_write : :write_values)
    end

    def clear_values(_spreadsheet_id, _range, _request)
      @calls << :clear
    end
  end

  def build_client_with_recording_service
    service = CallOrderRecordingService.new
    sheets_client = FreelanceJobs::SheetsClient.allocate
    sheets_client.instance_variable_set(:@service, service)
    sheets_client.instance_variable_set(:@spreadsheet_id, "sheet-id")
    sheets_client.instance_variable_set(:@sheet_gid, 123)
    [sheets_client, service]
  end

  # --- チェックボックス列（先頭列） ---

  def test_build_write_values_prepends_a_checkbox_cell_to_every_row
    client = build_client(checkbox_column: true)
    rows = [build_row("https://example.com/a"), build_row("https://example.com/b")]

    values = client.send(:build_write_values, "バナー", header_row_model, rows)

    assert_equal FreelanceJobs::SheetsClient::BASE_SHEET_COLUMN_COUNT + 1, values[0].size
    # バナー行のチェックボックス列は空にして、Sidekiqリンクと件数を1列ずつ右へずらす。
    assert_equal "", values[0][0]
    assert_equal FreelanceJobs::SheetsClient::SIDEKIQ_LINK_LABEL, values[0][1]
    assert_match(/\A=SUBTOTAL\(103,\$D\$3:\$D\$4\)/, values[0][2], "件数の集計対象も分類列(D列)へずれる想定")
    assert_equal FreelanceJobs::SheetsClient::CHECKBOX_HEADER_LABEL, values[1][0]
    assert_equal "🌟おすすめ", values[1][1]
    assert_equal [false, false], values[2..].map(&:first)
  end

  # チェックを付けた案件は、次の更新で行の位置が変わってもチェックが残る。
  def test_build_write_values_carries_over_checked_state_by_job_url
    client = build_client(checkbox_column: true)
    client.instance_variable_set(:@checkbox_states_by_url, { "https://example.com/b" => true })
    rows = [build_row("https://example.com/a"), build_row("https://example.com/b")]

    values = client.send(:build_write_values, "バナー", header_row_model, rows)

    assert_equal [false, true], values[2..].map(&:first)
  end

  def test_build_write_values_has_no_checkbox_cell_when_the_sheet_has_no_checkbox_column
    values = build_client.send(:build_write_values, "バナー", header_row_model, [build_row("https://example.com/a")])

    assert_equal FreelanceJobs::SheetsClient::BASE_SHEET_COLUMN_COUNT, values[0].size
    assert_equal FreelanceJobs::SheetsClient::SIDEKIQ_LINK_LABEL, values[0][0]
    assert_equal "🌟おすすめ", values[1][0]
  end

  # 案件が増えればチェックボックスもその行まで伸びる（データ行の範囲ぴったりに張り直す）。
  def test_checkbox_validation_requests_cover_exactly_the_data_rows
    client = build_client(checkbox_column: true)

    requests = client.send(:checkbox_validation_requests, 123, 10, 10)

    assert_equal 1, requests.size
    validation = requests.first.fetch(:set_data_validation)
    assert_equal({ sheet_id: 123, start_row_index: 2, end_row_index: 10, start_column_index: 0, end_column_index: 1 },
                  validation.fetch(:range))
    assert_equal "BOOLEAN", validation.dig(:rule, :condition, :type)
  end

  # 行数が減った回は、余った行の入力規則を外す（ruleを渡さないリクエストが解除になる）。
  def test_checkbox_validation_requests_release_the_rule_on_leftover_rows
    client = build_client(checkbox_column: true)

    requests = client.send(:checkbox_validation_requests, 123, 5, 12)

    assert_equal 2, requests.size
    leftover = requests.last.fetch(:set_data_validation)
    assert_equal 5, leftover.dig(:range, :start_row_index)
    assert_equal 12, leftover.dig(:range, :end_row_index)
    refute leftover.key?(:rule), "ruleを渡さないことが入力規則の解除になる"
  end

  def test_checkbox_validation_requests_are_empty_without_a_checkbox_column
    assert_empty build_client.send(:checkbox_validation_requests, 123, 10, 10)
  end

  # チェックボックス列を足した最初の実行では、まだ旧レイアウトのシートを読む。
  def test_detect_checkbox_offset_reads_the_layout_from_the_header_row
    client = build_client(checkbox_column: true)
    old_layout = [grid_row(["⏰ バナー"]), grid_row(["🌟おすすめ", "No."])]
    new_layout = [grid_row(["", "sidekiq"]), grid_row([FreelanceJobs::SheetsClient::CHECKBOX_HEADER_LABEL, "🌟おすすめ"])]

    assert_equal 0, client.send(:detect_checkbox_offset, old_layout)
    assert_equal 1, client.send(:detect_checkbox_offset, new_layout)
  end

  # チェックボックス列のヘッダーは利用者が付け替える（例:「応募チェック」）。
  # その文言で判定していると、付け替えた瞬間にレイアウトを見失って毎回中断してしまうため、
  # 位置の基準は必ず🌟おすすめ側に置く。
  def test_detect_checkbox_offset_survives_a_renamed_checkbox_header
    client = build_client(checkbox_column: true)
    renamed_layout = [grid_row(["", "sidekiq"]), grid_row(["応募チェック", "🌟おすすめ", "No."])]

    assert_equal 1, client.send(:detect_checkbox_offset, renamed_layout)
  end

  # 読み取れないシート（空・ヘッダー行なし）では、プロファイルの設定どおりのレイアウトで書く。
  def test_detect_checkbox_offset_falls_back_to_the_profile_layout
    assert_equal 1, build_client(checkbox_column: true).send(:detect_checkbox_offset, [])
    assert_equal 0, build_client(checkbox_column: false).send(:detect_checkbox_offset, [])
  end

  # 付け替えられたヘッダー文言は書き戻しでも維持する（勝手に☑へ戻さない）。
  def test_build_write_values_keeps_a_renamed_checkbox_header
    client = build_client(checkbox_column: true)
    renamed_layout = [grid_row(["", "sidekiq"]), grid_row(["応募チェック", "🌟おすすめ"])]
    offset = client.send(:detect_checkbox_offset, renamed_layout)
    client.instance_variable_set(:@checkbox_header_label,
                                  client.send(:detect_checkbox_header_label, renamed_layout, offset))

    values = client.send(:build_write_values, "バナー", header_row_model, [build_row("https://example.com/a")])

    assert_equal "応募チェック", values[1][0]
  end

  def build_row(url)
    row = Array.new(FreelanceJobs::SheetsClient::ROW_COLUMN_COUNT) { |index| "値#{index}" }
    row[FreelanceJobs::SheetsClient::URL_COLUMN_INDEX] = url
    row
  end

  def header_row_model
    FreelanceJobs::RowBuilder::HEADER
  end

  def grid_row(formatted_values)
    Google::Apis::SheetsV4::RowData.new(
      values: formatted_values.map { |value| Google::Apis::SheetsV4::CellData.new(formatted_value: value) }
    )
  end

  # シートの書式はバッチが持ち主なので、条件付き書式は毎回すべて消す。
  # 削除はindexが前に詰まる仕様のため、後ろのルールから消さないと消し漏れる。
  def test_clear_conditional_format_requests_deletes_existing_rules_from_the_last_index
    client = build_client
    main_sheet = sheet_stub(conditional_formats: [Object.new, Object.new, Object.new])

    requests = client.send(:clear_conditional_format_requests, main_sheet)

    deleted_indexes = requests.map { |request| request.dig(:delete_conditional_format_rule, :index) }
    assert_equal [2, 1, 0], deleted_indexes
  end

  def test_clear_conditional_format_requests_returns_nothing_when_there_is_no_rule
    assert_empty build_client.send(:clear_conditional_format_requests, sheet_stub)
  end

  def test_replace_sheet_unmerges_the_banner_row_before_writing_values
    sheets_client, service = build_client_with_recording_service

    sheets_client.replace_sheet(
      banner_text: "バナー本文",
      header: Array.new(FreelanceJobs::SheetsClient::ROW_COLUMN_COUNT) { |index| "見出し#{index}" },
      rows: [Array.new(FreelanceJobs::SheetsClient::ROW_COLUMN_COUNT) { |index| "値#{index}" }],
      starred_row_indexes: [],
      backup_values: [["既存"]]
    )

    unmerge_position = service.calls.index(:unmerge)
    write_position = service.calls.index(:write_values)

    refute_nil unmerge_position, "バナー行の結合解除が呼ばれる想定"
    refute_nil write_position, "本体シートへの値書き込みが呼ばれる想定"
    assert unmerge_position < write_position,
           "結合を解いてから値を書かないと、左上以外のセルへの書き込みが捨てられる: #{service.calls.inspect}"
  end

  # --- merge_banner_request（A1・B1を結合から外す） ---

  def test_merge_banner_request_merges_from_the_banner_text_column_to_the_last_column
    range = client.send(:merge_banner_request, 999).dig(:merge_cells, :range)

    assert_equal 0, range[:start_row_index]
    assert_equal 1, range[:end_row_index]
    assert_equal FreelanceJobs::SheetsClient::BASE_BANNER_TEXT_COLUMN_INDEX, range[:start_column_index]
    assert_equal 2, range[:start_column_index], "A1(Sidekiqリンク)とB1(表示件数)は結合に含めない想定"
    assert_equal FreelanceJobs::SheetsClient::BASE_SHEET_COLUMN_COUNT, range[:end_column_index]
  end

  # --- strip_url_column ---
  # AC-03でROW_COLUMN_COUNTが16になったため、URL列(1列)を落とした後は15列になる。

  def test_strip_url_column_converts_16_column_row_model_to_15_column_sheet_row
    row = full_width_row_model(5 => "https://example.com/job")

    sheet_row = client.send(:strip_url_column, row)

    assert_equal 15, sheet_row.size
    refute_includes sheet_row, "https://example.com/job"
    assert_equal "値4", sheet_row[4]
    assert_equal "値6", sheet_row[5]
    assert_equal "値15", sheet_row.last, "末尾(追加日)もURL列削除ぶん1つ前にずれて残っている想定"
  end

  # --- full_width_range ---

  def test_full_width_range_spans_all_14_sheet_columns
    range = client.send(:full_width_range, 999, 2, 5)

    assert_equal 999, range[:sheet_id]
    assert_equal 2, range[:start_row_index]
    assert_equal 5, range[:end_row_index]
    assert_equal 0, range[:start_column_index]
    assert_equal FreelanceJobs::SheetsClient::BASE_SHEET_COLUMN_COUNT, range[:end_column_index]
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

  # === AC-04: 本日追加の行を強調する ===

  def test_new_today_background_color_constant
    assert_equal({ red: 1.0, green: 0.78, blue: 0.50 }, FreelanceJobs::SheetsClient::NEW_TODAY_BACKGROUND_COLOR)
  end

  # replace_sheetにnew_today_row_indexes:を渡すためのフル機能フェイクサービス。
  # batch_updateで送られたリクエストを全件記録する（CallOrderRecordingServiceは種別しか記録しないため）。
  class FormattingRecordingService
    Properties = Struct.new(:sheet_id, :title, :grid_properties, keyword_init: true)
    GridProperties = Struct.new(:row_count, keyword_init: true)
    SheetStub = Struct.new(:properties, :merges, :basic_filter, :conditional_formats, keyword_init: true)

    attr_reader :batch_update_requests

    def initialize
      @batch_update_requests = []
    end

    def get_spreadsheet(_spreadsheet_id, **_options)
      sheet = SheetStub.new(
        properties: Properties.new(sheet_id: 123, title: "求人", grid_properties: GridProperties.new(row_count: 10)),
        merges: [], basic_filter: nil, conditional_formats: []
      )
      Struct.new(:sheets, keyword_init: true).new(sheets: [sheet])
    end

    def batch_update_spreadsheet(_spreadsheet_id, request_body, **_options)
      @batch_update_requests.concat(request_body.requests)
    end

    def update_spreadsheet_value(_spreadsheet_id, _range, _body, **_options); end

    def clear_values(_spreadsheet_id, _range, _request); end
  end

  def build_client_with_formatting_recording_service
    service = FormattingRecordingService.new
    sheets_client = FreelanceJobs::SheetsClient.allocate
    sheets_client.instance_variable_set(:@service, service)
    sheets_client.instance_variable_set(:@spreadsheet_id, "sheet-id")
    sheets_client.instance_variable_set(:@sheet_gid, 123)
    sheets_client.instance_variable_set(:@checkbox_column, false)
    sheets_client.instance_variable_set(:@checkbox_states_by_url, {})
    sheets_client.instance_variable_set(:@hidden_level_marker, nil)
    [sheets_client, service]
  end

  def repeat_cell_requests_with_background(requests, color)
    requests.select do |request|
      request.dig(:repeat_cell, :cell, :user_entered_format, :background_color) == color
    end
  end

  def build_row_model_for_formatting_test
    Array.new(FreelanceJobs::SheetsClient::ROW_COLUMN_COUNT) { |index| "値#{index}" }
  end

  def test_replace_sheet_paints_new_today_rows_with_the_new_today_background_color
    sheets_client, service = build_client_with_formatting_recording_service

    sheets_client.replace_sheet(
      banner_text: "バナー本文",
      header: build_row_model_for_formatting_test,
      rows: [build_row_model_for_formatting_test, build_row_model_for_formatting_test],
      starred_row_indexes: [],
      new_today_row_indexes: [1],
      backup_values: []
    )

    new_today_requests = repeat_cell_requests_with_background(service.batch_update_requests,
                                                                FreelanceJobs::SheetsClient::NEW_TODAY_BACKGROUND_COLOR)
    assert_equal 1, new_today_requests.size
    range = new_today_requests.first.dig(:repeat_cell, :range)
    assert_equal 3, range[:start_row_index], "data row index 1は既存のstarred_row_requestsと同じ+2オフセットでシート行3になる想定"
    assert_equal 4, range[:end_row_index]
  end

  def test_replace_sheet_defaults_new_today_row_indexes_to_empty_and_paints_nothing
    sheets_client, service = build_client_with_formatting_recording_service

    sheets_client.replace_sheet(
      banner_text: "バナー本文",
      header: build_row_model_for_formatting_test,
      rows: [build_row_model_for_formatting_test],
      starred_row_indexes: [],
      backup_values: []
    )

    new_today_requests = repeat_cell_requests_with_background(service.batch_update_requests,
                                                                FreelanceJobs::SheetsClient::NEW_TODAY_BACKGROUND_COLOR)
    assert_empty new_today_requests, "new_today_row_indexesを渡さない場合は既定の[]で何も塗らない想定"
  end

  # 同じ行が🌟(薄黄色)とnew_today(オレンジ)の両方に該当する場合、バッチリクエストの並びで
  # new_todayが🌟より後に来ることで、実際の描画は後勝ちでオレンジになる。
  def test_replace_sheet_orders_new_today_repaint_after_starred_repaint_for_the_same_row
    sheets_client, service = build_client_with_formatting_recording_service

    sheets_client.replace_sheet(
      banner_text: "バナー本文",
      header: build_row_model_for_formatting_test,
      rows: [build_row_model_for_formatting_test],
      starred_row_indexes: [0],
      new_today_row_indexes: [0],
      backup_values: []
    )

    starred_index = service.batch_update_requests.index do |request|
      request.dig(:repeat_cell, :cell, :user_entered_format, :background_color) ==
        FreelanceJobs::SheetsClient::STARRED_BACKGROUND_COLOR
    end
    new_today_index = service.batch_update_requests.index do |request|
      request.dig(:repeat_cell, :cell, :user_entered_format, :background_color) ==
        FreelanceJobs::SheetsClient::NEW_TODAY_BACKGROUND_COLOR
    end

    refute_nil starred_index, "🌟の塗りリクエストが出る想定"
    refute_nil new_today_index, "本日追加の塗りリクエストが出る想定"
    assert_operator new_today_index, :>, starred_index,
                    "同じ行が両方に該当する場合、本日追加のオレンジが🌟より後（＝後勝ち）で並ぶ想定"
  end

  def test_replace_sheet_still_resets_data_row_background_when_new_today_rows_are_present
    sheets_client, service = build_client_with_formatting_recording_service

    sheets_client.replace_sheet(
      banner_text: "バナー本文",
      header: build_row_model_for_formatting_test,
      rows: [build_row_model_for_formatting_test],
      starred_row_indexes: [],
      new_today_row_indexes: [0],
      backup_values: []
    )

    reset_requests = repeat_cell_requests_with_background(service.batch_update_requests,
                                                            FreelanceJobs::SheetsClient::WHITE_BACKGROUND_COLOR)
    refute_empty reset_requests, "データ行の背景リセットは本日追加の強調があっても毎回出る想定"
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
    Array.new(FreelanceJobs::SheetsClient::BASE_SHEET_COLUMN_COUNT) do |index|
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
    # AC-03でBASE_SHEET_COLUMN_COUNTが15になり(チェックボックス列なし)、最終列はO列になる。
    # ここが古いN列のままだと、本番で追加日列(15列目)が読み取り範囲から外れてしまう。
    assert_equal ["'Ruby TypeScript 求人'!A1:O2000"], grid_call[:ranges],
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
    # このテストもcheckbox_columnを設定しない(=チェックボックス列なし)ため、最終列はO列になる。
    assert_equal ["'HTML CSS求人'!A1:O2000"], grid_call[:ranges],
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
    blank_row = Array.new(FreelanceJobs::SheetsClient::BASE_SHEET_COLUMN_COUNT) { build_sheet_cell("") }
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

  # === AC-03(a): 旧レイアウト（追加日列がまだ無い14列のシート本体）を読む ===
  # チェックボックス列なし・ありの両方で、行モデルは16列になり、追加日(index15)は""になる想定。

  def test_read_rows_returns_empty_added_on_when_the_sheet_body_still_has_the_old_14_column_layout_without_checkbox
    metadata = GidFakeMetadata.new([GidFakeSheet.new(GidFakeSheetProperties.new(0, "HTML CSS求人"))])
    old_layout_cells = Array.new(14) { |index| build_sheet_cell("値#{index}") }
    old_layout_cells[FreelanceJobs::SheetsClient::TITLE_COLUMN_INDEX] =
      build_sheet_cell("案件", hyperlink: "https://example.com/old-layout")
    grid_spreadsheet = build_grid_spreadsheet([old_layout_cells])
    service = GidFakeGoogleSheetsService.new(metadata: metadata, grid_spreadsheet: grid_spreadsheet)
    client_instance = build_client_with_fake_service(spreadsheet_id: "SHEET_ID", sheet_gid: 0, service: service)

    rows = client_instance.read_rows

    assert_equal 1, rows.size
    assert_equal 16, rows.first.size, "追加日列が無い旧レイアウトを読んでも16列の行モデルになる想定"
    assert_equal "", rows.first[15], "追加日が無いセルは空文字で埋まる想定"
    assert_equal "https://example.com/old-layout", rows.first[FreelanceJobs::SheetsClient::URL_COLUMN_INDEX]
  end

  def test_read_rows_returns_empty_added_on_when_the_sheet_body_still_has_the_old_14_column_layout_with_checkbox
    metadata = GidFakeMetadata.new([GidFakeSheet.new(GidFakeSheetProperties.new(0, "HTML CSS求人"))])
    banner_cells = [build_sheet_cell("")]
    header_cells = [build_sheet_cell(FreelanceJobs::SheetsClient::CHECKBOX_HEADER_LABEL), build_sheet_cell("🌟おすすめ")]
    data_cells = [build_sheet_cell(false)] + Array.new(14) { |index| build_sheet_cell("値#{index}") }
    data_cells[1 + FreelanceJobs::SheetsClient::TITLE_COLUMN_INDEX] =
      build_sheet_cell("案件", hyperlink: "https://example.com/old-layout-checkbox")
    grid_spreadsheet = build_grid_spreadsheet([banner_cells, header_cells, data_cells])
    service = GidFakeGoogleSheetsService.new(metadata: metadata, grid_spreadsheet: grid_spreadsheet)
    client_instance = build_client_with_fake_service(spreadsheet_id: "SHEET_ID", sheet_gid: 0, service: service)

    rows = client_instance.read_rows
    data_row_model = rows.last

    assert_equal 16, data_row_model.size
    assert_equal "", data_row_model[15]
    assert_equal "https://example.com/old-layout-checkbox", data_row_model[FreelanceJobs::SheetsClient::URL_COLUMN_INDEX]
  end

  # === AC-03(b): 新レイアウト（追加日ありの15列）を読む ===

  def test_read_rows_preserves_the_added_on_value_when_the_sheet_body_already_has_the_new_15_column_layout
    metadata = GidFakeMetadata.new([GidFakeSheet.new(GidFakeSheetProperties.new(0, "HTML CSS求人"))])
    new_layout_cells = Array.new(15) { |index| build_sheet_cell("値#{index}") }
    new_layout_cells[FreelanceJobs::SheetsClient::TITLE_COLUMN_INDEX] =
      build_sheet_cell("案件", hyperlink: "https://example.com/new-layout")
    new_layout_cells[14] = build_sheet_cell("2026-09-10") # シート本体側の追加日は最終列(index14)
    grid_spreadsheet = build_grid_spreadsheet([new_layout_cells])
    service = GidFakeGoogleSheetsService.new(metadata: metadata, grid_spreadsheet: grid_spreadsheet)
    client_instance = build_client_with_fake_service(spreadsheet_id: "SHEET_ID", sheet_gid: 0, service: service)

    rows = client_instance.read_rows

    assert_equal 16, rows.first.size
    assert_equal "2026-09-10", rows.first[15], "既存の追加日の値をそのまま行モデルに差し戻す想定"
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

  # === AC-03(c): バックアップ隠しシートの列拡張 ===
  # バックアップ用隠しシートの column_count が16未満なら、replace_sheetの内部でupdate_sheet_propertiesを
  # 発行してgrid_properties.column_countを16に広げる想定。既に16以上なら発行しない。

  BackupSheetProperties = Struct.new(:sheet_id, :title, :grid_properties, keyword_init: true)
  BackupGridProperties = Struct.new(:column_count, keyword_init: true)
  BackupSheetStub = Struct.new(:properties, keyword_init: true)
  BackupMetadataStub = Struct.new(:sheets, keyword_init: true)

  class BatchUpdateRecordingService
    attr_reader :batch_update_requests

    def initialize
      @batch_update_requests = []
    end

    def batch_update_spreadsheet(_spreadsheet_id, request_body, **_options)
      @batch_update_requests.concat(request_body.requests)
    end
  end

  def build_backup_sheet_metadata(column_count:)
    backup_sheet = BackupSheetStub.new(
      properties: BackupSheetProperties.new(sheet_id: 55, title: "_backup_gid0",
                                             grid_properties: BackupGridProperties.new(column_count: column_count))
    )
    BackupMetadataStub.new(sheets: [backup_sheet])
  end

  def build_client_for_backup_sheet_test(service)
    client_instance = FreelanceJobs::SheetsClient.allocate
    client_instance.instance_variable_set(:@spreadsheet_id, "SHEET_ID")
    client_instance.instance_variable_set(:@sheet_gid, 0)
    client_instance.instance_variable_set(:@service, service)
    client_instance
  end

  def test_ensure_backup_sheet_exists_expands_column_count_when_it_is_below_16
    service = BatchUpdateRecordingService.new
    client_instance = build_client_for_backup_sheet_test(service)
    metadata = build_backup_sheet_metadata(column_count: 15)

    client_instance.send(:ensure_backup_sheet_exists!, metadata)

    expansion_request = service.batch_update_requests.find { |request| request.key?(:update_sheet_properties) }
    refute_nil expansion_request, "バックアップシートの列数が16未満なら拡張リクエストを発行する想定"
    properties = expansion_request[:update_sheet_properties][:properties]
    assert_equal 55, properties[:sheet_id]
    assert_equal 16, properties.dig(:grid_properties, :column_count)
  end

  def test_ensure_backup_sheet_exists_does_not_expand_when_column_count_is_already_16
    service = BatchUpdateRecordingService.new
    client_instance = build_client_for_backup_sheet_test(service)
    metadata = build_backup_sheet_metadata(column_count: 16)

    client_instance.send(:ensure_backup_sheet_exists!, metadata)

    assert_empty service.batch_update_requests, "既に16列以上なら拡張リクエストは発行しない想定"
  end

  def test_ensure_backup_sheet_exists_does_not_expand_when_column_count_is_already_larger_than_16
    service = BatchUpdateRecordingService.new
    client_instance = build_client_for_backup_sheet_test(service)
    metadata = build_backup_sheet_metadata(column_count: 26)

    client_instance.send(:ensure_backup_sheet_exists!, metadata)

    assert_empty service.batch_update_requests, "既に16列より広ければ拡張リクエストは発行しない想定"
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

  # リクエストの組み立てだけを見たいので、認証せずにインスタンスだけ作る。
  # checkbox_column: true にするとチェックボックス列ぶん全体が1列ずれる。
  def build_client(checkbox_column: false, hidden_level_marker: nil)
    client_instance = FreelanceJobs::SheetsClient.allocate
    client_instance.instance_variable_set(:@checkbox_column, checkbox_column)
    client_instance.instance_variable_set(:@checkbox_states_by_url, {})
    client_instance.instance_variable_set(:@hidden_level_marker, hidden_level_marker)
    client_instance
  end

  # --- 既定フィルター（レベル） ---

  # 上級まで並ぶと応募できる案件が埋もれるので、開いた直後は中級以下だけが見える状態にする。
  def test_basic_filter_hides_the_levels_marked_as_out_of_range
    client = build_client(checkbox_column: true, hidden_level_marker: "★★★")

    filter = client.send(:basic_filter_request, 42, 300)[:set_basic_filter][:filter]

    # チェックボックス列が1つ入るので、レベル列はG列(index 6)になる。
    criteria = filter[:criteria]["6"]
    assert_equal "TEXT_NOT_CONTAINS", criteria.condition.type
    assert_equal "★★★", criteria.condition.values.first.user_entered_value
  end

  # チェックボックス列が無いシートでは1つ手前(F列)を見る。
  def test_basic_filter_targets_the_level_column_without_a_checkbox_column
    client = build_client(checkbox_column: false, hidden_level_marker: "★★★")

    filter = client.send(:basic_filter_request, 42, 300)[:set_basic_filter][:filter]

    assert_equal ["5"], filter[:criteria].keys
  end

  # 未経験向けシートは全レベルを見せたいので、条件は付けない（素のフィルターのまま）。
  def test_basic_filter_has_no_criteria_without_a_hidden_level_marker
    client = build_client(checkbox_column: false)

    filter = client.send(:basic_filter_request, 42, 300)[:set_basic_filter][:filter]

    refute filter.key?(:criteria)
  end

  # 書式リクエストが読むのは sheet_id と conditional_formats だけ。
  def sheet_stub(conditional_formats: [])
    CallOrderRecordingService::SheetStub.new(
      properties: CallOrderRecordingService::Properties.new(
        sheet_id: 123, title: "求人",
        grid_properties: CallOrderRecordingService::GridProperties.new(row_count: 10)
      ),
      merges: [],
      basic_filter: nil,
      conditional_formats: conditional_formats
    )
  end
end
