# frozen_string_literal: true

module FreelanceJobs
  # 「エンジニア申込サイト一覧」タブの A 列（案件数）を毎朝のバッチ結果で更新する。
  # 案件一覧シートとは別タブで、B 列のサイト名の行ごとに「掲載N件／取得M件」を書く。
  # 通信するのはこのクラスだけで、文言の組み立ては SiteListCounts（純粋ロジック）に任せる。
  class SiteListSheet
    HEADER_LABEL = "サイト名"
    SITE_NAME_COLUMN_INDEX = 1
    READ_RANGE = "A1:B200"

    def initialize(spreadsheet_id:, sheet_gid:)
      @spreadsheet_id = spreadsheet_id
      @sheet_gid = sheet_gid
      @service = FreelanceJobs::SheetsServiceFactory.build(application_name: "FreelanceJobs Site List Update")
    end

    # 更新した行数（ヘッダー行を含む）を返す。
    def update(fetched_counts:, sheet_rows:, failed_sites:, excluded_sites:)
      sheet_name = resolve_sheet_name
      values = @service.get_spreadsheet_values(@spreadsheet_id, "'#{sheet_name}'!#{READ_RANGE}").values || []
      header_index = header_row_index(values)
      counts_by_index = FreelanceJobs::SiteListCounts.build(
        site_list_names: site_list_names(values, header_index),
        fetched_counts: fetched_counts,
        sheet_rows: sheet_rows,
        failed_sites: failed_sites,
        excluded_sites: excluded_sites
      )
      column_values = build_column_values(values, header_index, counts_by_index)

      # 先頭が「=」「+」等の文言を数式として解釈させないため RAW で書く。
      first_row_number = header_index + 1
      last_row_number = first_row_number + column_values.size - 1
      value_range = Google::Apis::SheetsV4::ValueRange.new(values: column_values)
      @service.update_spreadsheet_value(
        @spreadsheet_id, "'#{sheet_name}'!A#{first_row_number}:A#{last_row_number}", value_range,
        value_input_option: "RAW"
      )
      column_values.size
    end

    private

    def resolve_sheet_name
      metadata = @service.get_spreadsheet(@spreadsheet_id, fields: "sheets.properties")
      sheet = metadata.sheets&.find { |candidate| candidate.properties&.sheet_id == @sheet_gid }
      raise FreelanceJobs::FetchError, "Sheet gid not found: #{@sheet_gid}" unless sheet

      sheet.properties.title
    end

    # B 列が「サイト名」の最初の行（0始まり）。末尾の空セルは API が省くため短い行を許容する。
    def header_row_index(values)
      index = values.index { |row| row[SITE_NAME_COLUMN_INDEX].to_s.strip == HEADER_LABEL }
      raise FreelanceJobs::FetchError, "申込サイト一覧のヘッダー行（B列=#{HEADER_LABEL}）が見つかりません" unless index

      index
    end

    def site_list_names(values, header_row_index)
      (values[(header_row_index + 1)..] || []).map { |row| row[SITE_NAME_COLUMN_INDEX] }
    end

    # ヘッダーA セル + 以降の各行。一致しない行は手書きのメモを消さないよう既存値を書き戻す。
    def build_column_values(values, header_row_index, counts_by_index)
      body_rows = values[(header_row_index + 1)..] || []
      # 読み取りは表示値（FORMATTED_VALUE）、書き込みは RAW のため、一致しない行の A 列が数式だった場合は
      # 計算結果の文字列で上書きされ数式は残らない。このタブの A 列は手書きメモ（数式なし）が前提で、
      # 数式を誤って評価させないことを優先して RAW にしている。
      body = body_rows.each_with_index.map do |row, index|
        [counts_by_index[index] || row[0].to_s]
      end
      [[FreelanceJobs::SiteListCounts::HEADER_TEXT]] + body
    end
  end
end
