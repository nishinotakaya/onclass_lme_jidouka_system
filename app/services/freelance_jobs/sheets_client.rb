# frozen_string_literal: true

require "google/apis/sheets_v4"
require "googleauth"
require "json"

module FreelanceJobs
  # 案件一覧シートの読み取り・書き込みを担当する。
  # 認証は Youtube::CompetitorWorker#build_sheets_service と同じ流儀
  # （サービスアカウントJSON鍵、ENV["GOOGLE_APPLICATION_CREDENTIALS"]）。
  #
  # 行モデル（RowBuilder / SheetMerger が扱う配列）は ROW_COLUMN_COUNT 列で、
  # index URL_COLUMN_INDEX に案件URLを持つ。一方シート上は案件URL列を置かず、
  # 案件名セルのハイパーリンク（textFormat.link）としてURLを保持する。
  # 読み取り時にリンクからURLを復元して行モデルへ差し戻すため、マージ側は
  # 案件URL列の有無を意識しなくてよい。
  class SheetsClient
    # 行モデルの列数（案件URLを含む）。バックアップシートにはこの形のまま退避する。
    ROW_COLUMN_COUNT = 15
    # シート上の列数（案件URLを除く A〜N）。
    SHEET_COLUMN_COUNT = 14
    SHEET_LAST_COLUMN = "N"
    URL_COLUMN_INDEX = 5
    TITLE_COLUMN_INDEX = 3
    MAX_READ_ROW_COUNT = 2000

    # 案件URL列(230px)を除いた14列分。
    COLUMN_WIDTHS = [90, 45, 130, 300, 95, 130, 400, 260, 130, 80, 130, 120, 330, 120].freeze

    BANNER_BACKGROUND_COLOR = { red: 0.93, green: 0.93, blue: 0.93 }.freeze
    HEADER_BACKGROUND_COLOR = { red: 0.16, green: 0.40, blue: 0.75 }.freeze
    STARRED_BACKGROUND_COLOR = { red: 1.0, green: 0.97, blue: 0.80 }.freeze
    WHITE_BACKGROUND_COLOR = { red: 1.0, green: 1.0, blue: 1.0 }.freeze
    FORMULA_TRIGGER_CHARS = ["=", "+", "-", "@"].freeze
    LINK_FOREGROUND_COLOR = { red: 0.05, green: 0.35, blue: 0.75 }.freeze
    # バナー行の左上(A1)に置く Sidekiq Web UI へのリンク。ラベルと遷移先。
    SIDEKIQ_LINK_LABEL = "sidekiq"
    DEFAULT_SIDEKIQ_WEB_URL = "https://onclass-lme-jidouka-app-857ffde75fc4.herokuapp.com/sidekiq"

    def self.sidekiq_web_url
      ENV.fetch("SIDEKIQ_WEB_URL", DEFAULT_SIDEKIQ_WEB_URL)
    end

    def initialize(spreadsheet_id:, sheet_gid:)
      @spreadsheet_id = spreadsheet_id
      @sheet_gid = sheet_gid
      @service = build_service
    end

    # シートを行モデル（ROW_COLUMN_COUNT列）として読む。
    # 案件名セルのハイパーリンクを URL_COLUMN_INDEX に差し込んで返すため、
    # 値だけを返す get_spreadsheet_values ではなくグリッドデータを取得する。
    def read_rows(max_row_count: MAX_READ_ROW_COUNT)
      range = "'#{sheet_name}'!A1:#{SHEET_LAST_COLUMN}#{max_row_count}"
      spreadsheet = @service.get_spreadsheet(
        @spreadsheet_id,
        ranges: [range],
        include_grid_data: true,
        fields: "sheets.data.rowData.values(formattedValue,hyperlink,userEnteredFormat.textFormat.link)"
      )
      row_data = spreadsheet.sheets&.first&.data&.first&.row_data || []
      rows = row_data.map { |grid_row| build_row_model(grid_row) }
      rows.pop while rows.last && rows.last.all? { |value| value.to_s.empty? }
      rows
    end

    # 書き込み手順（空になる瞬間を作らない）:
    # 1. 隠しシート（backup_sheet_name）へ現在値(backup_values)を案件URL列込みで退避
    # 2. A1:N(n)を上書き（案件URL列は書かない。数式化しうる文字列は'を付けてガード）
    # 3. 余った行だけclear
    # 4. 書式（既存merge/basic_filterを取得してから安全に張り替え）＋案件名セルへリンク付与
    def replace_sheet(banner_text:, header:, rows:, starred_row_indexes:, backup_values:, column_widths: COLUMN_WIDTHS)
      metadata = fetch_metadata
      main_sheet = resolve_main_sheet!(metadata)
      @sheet_name = main_sheet.properties.title

      backup_existing_values!(metadata, backup_values)

      previous_row_count = main_sheet.properties.grid_properties.row_count.to_i
      values = build_write_values(banner_text, header, rows)
      write_values!(values)
      clear_leftover_rows!(previous_row_count, values.size)
      apply_formatting!(main_sheet, values.size, starred_row_indexes, column_widths, rows, previous_row_count)
    end

    # バックアップ用の隠しシート名。gidごとに一意にする（例: "_backup_gid0"）。
    def backup_sheet_name
      "_backup_gid#{@sheet_gid}"
    end

    private

    # グリッドデータ1行 → 案件URLを差し込んだ ROW_COLUMN_COUNT 列の行モデル。
    def build_row_model(grid_row)
      cells = grid_row.values || []
      sheet_values = Array.new(SHEET_COLUMN_COUNT) { |index| cells[index]&.formatted_value.to_s }
      sheet_values.insert(URL_COLUMN_INDEX, cell_link_uri(cells[TITLE_COLUMN_INDEX]))
    end

    def cell_link_uri(cell)
      return "" unless cell

      (cell.hyperlink || cell.user_entered_format&.text_format&.link&.uri).to_s
    end

    def build_service
      service = Google::Apis::SheetsV4::SheetsService.new
      service.client_options.application_name = "FreelanceJobs Research Batch"
      scope = [Google::Apis::SheetsV4::AUTH_SPREADSHEETS]
      keyfile = ENV["GOOGLE_APPLICATION_CREDENTIALS"]

      raise FreelanceJobs::FetchError, "ENV GOOGLE_APPLICATION_CREDENTIALS is not set." if keyfile.nil? || keyfile.strip.empty?
      raise FreelanceJobs::FetchError, "Service account key not found: #{keyfile}" unless File.exist?(keyfile)

      json = begin
        JSON.parse(File.read(keyfile))
      rescue JSON::ParserError
        nil
      end
      unless json && json["type"] == "service_account" && json["private_key"] && json["client_email"]
        raise FreelanceJobs::FetchError, "Invalid service account JSON: missing private_key/client_email/type=service_account"
      end

      authorizer = Google::Auth::ServiceAccountCredentials.make_creds(json_key_io: File.open(keyfile), scope: scope)
      authorizer.fetch_access_token!
      service.authorization = authorizer
      service
    end

    def fetch_metadata
      @service.get_spreadsheet(@spreadsheet_id, include_grid_data: false)
    end

    # gidからシート名を解決する（遅延・メモ化）。replace_sheetは自前で取得済みのmetadataから
    # resolve_main_sheet!を直接呼ぶため二重にAPIを叩かない。read_values単独呼び出し用の経路。
    def sheet_name
      @sheet_name ||= resolve_main_sheet!(fetch_metadata).properties.title
    end

    def resolve_main_sheet!(metadata)
      main_sheet = find_sheet_by_gid(metadata, @sheet_gid)
      raise FreelanceJobs::FetchError, "Sheet gid not found: #{@sheet_gid}" unless main_sheet

      main_sheet
    end

    def find_sheet_by_gid(metadata, sheet_gid)
      metadata.sheets&.find { |sheet| sheet.properties&.sheet_id == sheet_gid }
    end

    def find_sheet(metadata, title)
      metadata.sheets&.find { |sheet| sheet.properties&.title == title }
    end

    def backup_existing_values!(metadata, backup_values)
      ensure_backup_sheet_exists!(metadata)
      clear_range!("'#{backup_sheet_name}'!A1:Z2000")
      return if backup_values.nil? || backup_values.empty?

      write_range!("'#{backup_sheet_name}'!A1", backup_values, value_input_option: "RAW")
    end

    def ensure_backup_sheet_exists!(metadata)
      return if find_sheet(metadata, backup_sheet_name)

      batch_update!([{
        add_sheet: {
          properties: {
            title: backup_sheet_name,
            hidden: true,
            grid_properties: { row_count: MAX_READ_ROW_COUNT, column_count: ROW_COLUMN_COUNT }
          }
        }
      }])
    end

    def build_write_values(banner_text, header, rows)
      # A1は Sidekiq Web へのリンク、B1以降(結合セル)がバナー本文。
      banner_row = [SIDEKIQ_LINK_LABEL, banner_text] + Array.new(SHEET_COLUMN_COUNT - 2, "")
      [banner_row, *[header, *rows].map { |row| strip_url_column(row) }]
        .map { |row| row.map { |value| escape_formula(value) } }
    end

    # 行モデルから案件URL列を落として、シート上の14列にする。
    def strip_url_column(row)
      sheet_row = Array.new(ROW_COLUMN_COUNT) { |index| row[index] }
      sheet_row.delete_at(URL_COLUMN_INDEX)
      sheet_row
    end

    # 先頭が = + - @ の文字列は数式と解釈されうるため ' を付けてテキスト扱いにする。
    def escape_formula(value)
      return value unless value.is_a?(String)
      return value if value.empty? || !FORMULA_TRIGGER_CHARS.include?(value[0])

      "'#{value}"
    end

    def write_values!(values)
      write_range!("'#{@sheet_name}'!A1:#{SHEET_LAST_COLUMN}#{values.size}", values,
                    value_input_option: "USER_ENTERED")
    end

    def clear_leftover_rows!(previous_row_count, written_row_count)
      return if previous_row_count <= written_row_count

      clear_range!("'#{@sheet_name}'!A#{written_row_count + 1}:#{SHEET_LAST_COLUMN}#{previous_row_count}")
    end

    def apply_formatting!(main_sheet, total_row_count, starred_row_indexes, column_widths, rows, previous_row_count)
      sheet_id = main_sheet.properties.sheet_id
      requests = []

      requests.concat(unmerge_row_zero_requests(sheet_id, main_sheet.merges))
      requests << { clear_basic_filter: { sheet_id: sheet_id } } if main_sheet.basic_filter
      requests << merge_banner_request(sheet_id)
      requests << banner_format_request(sheet_id)
      requests << banner_row_height_request(sheet_id)
      requests << header_format_request(sheet_id)
      requests << all_cell_alignment_request(sheet_id, total_row_count)

      if total_row_count > 2
        requests << data_background_reset_request(sheet_id, total_row_count)
        requests << recommend_column_font_request(sheet_id, total_row_count)
      end

      requests.concat(starred_row_requests(sheet_id, starred_row_indexes))
      requests << frozen_row_count_request(sheet_id)
      requests.concat(column_width_requests(sheet_id, column_widths))
      requests << sidekiq_link_request(sheet_id)
      requests.concat(title_link_requests(sheet_id, rows, previous_row_count))
      requests << basic_filter_request(sheet_id, total_row_count)

      batch_update!(requests)
    end

    # 行0（バナー行）にかかる既存mergeだけを、その正確な範囲でunmergeする。
    def unmerge_row_zero_requests(sheet_id, merges)
      (merges || []).select { |range| range.start_row_index.to_i <= 0 && range.end_row_index.to_i > 0 }
                    .map do |range|
        {
          unmerge_cells: {
            range: {
              sheet_id: sheet_id,
              start_row_index: range.start_row_index,
              end_row_index: range.end_row_index,
              start_column_index: range.start_column_index,
              end_column_index: range.end_column_index
            }
          }
        }
      end
    end

    # A1はSidekiqリンク用に独立させ、B1:N1だけをバナー本文として結合する。
    def merge_banner_request(sheet_id)
      {
        merge_cells: {
          range: { sheet_id: sheet_id, start_row_index: 0, end_row_index: 1,
                    start_column_index: 1, end_column_index: SHEET_COLUMN_COUNT },
          merge_type: "MERGE_ALL"
        }
      }
    end

    def banner_format_request(sheet_id)
      {
        repeat_cell: {
          range: full_width_range(sheet_id, 0, 1),
          cell: {
            user_entered_format: {
              background_color: BANNER_BACKGROUND_COLOR,
              text_format: { bold: true, font_size: 11 },
              horizontal_alignment: "CENTER",
              vertical_alignment: "MIDDLE",
              wrap_strategy: "WRAP"
            }
          },
          fields: "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)"
        }
      }
    end

    def banner_row_height_request(sheet_id)
      {
        update_dimension_properties: {
          range: { sheet_id: sheet_id, dimension: "ROWS", start_index: 0, end_index: 1 },
          properties: { pixel_size: 36 },
          fields: "pixelSize"
        }
      }
    end

    def header_format_request(sheet_id)
      {
        repeat_cell: {
          range: full_width_range(sheet_id, 1, 2),
          cell: {
            user_entered_format: {
              background_color: HEADER_BACKGROUND_COLOR,
              text_format: { bold: true, foreground_color: { red: 1, green: 1, blue: 1 } },
              horizontal_alignment: "CENTER",
              vertical_alignment: "MIDDLE",
              wrap_strategy: "WRAP"
            }
          },
          fields: "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)"
        }
      }
    end

    # ユーザー要望: ヘッダーと同じく全セルを上下左右中央寄せ・折り返しにする。
    def all_cell_alignment_request(sheet_id, total_row_count)
      {
        repeat_cell: {
          range: full_width_range(sheet_id, 0, total_row_count),
          cell: {
            user_entered_format: {
              horizontal_alignment: "CENTER",
              vertical_alignment: "MIDDLE",
              wrap_strategy: "WRAP"
            }
          },
          fields: "userEnteredFormat(horizontalAlignment,verticalAlignment,wrapStrategy)"
        }
      }
    end

    def data_background_reset_request(sheet_id, total_row_count)
      {
        repeat_cell: {
          range: full_width_range(sheet_id, 2, total_row_count),
          cell: { user_entered_format: { background_color: WHITE_BACKGROUND_COLOR } },
          fields: "userEnteredFormat.backgroundColor"
        }
      }
    end

    # A列(🌟)はwrite_sheet.rbに合わせてフォントを少し大きくする。
    def recommend_column_font_request(sheet_id, total_row_count)
      {
        repeat_cell: {
          range: { sheet_id: sheet_id, start_row_index: 2, end_row_index: total_row_count, start_column_index: 0, end_column_index: 1 },
          cell: { user_entered_format: { text_format: { font_size: 14 } } },
          fields: "userEnteredFormat.textFormat.fontSize"
        }
      }
    end

    def starred_row_requests(sheet_id, starred_row_indexes)
      starred_row_indexes.map do |data_row_index|
        sheet_row_index = data_row_index + 2
        {
          repeat_cell: {
            range: full_width_range(sheet_id, sheet_row_index, sheet_row_index + 1),
            cell: { user_entered_format: { background_color: STARRED_BACKGROUND_COLOR } },
            fields: "userEnteredFormat.backgroundColor"
          }
        }
      end
    end

    def frozen_row_count_request(sheet_id)
      {
        update_sheet_properties: {
          properties: { sheet_id: sheet_id, grid_properties: { frozen_row_count: 2 } },
          fields: "gridProperties.frozenRowCount"
        }
      }
    end

    def column_width_requests(sheet_id, column_widths)
      column_widths.each_with_index.map do |width, index|
        {
          update_dimension_properties: {
            range: { sheet_id: sheet_id, dimension: "COLUMNS", start_index: index, end_index: index + 1 },
            properties: { pixel_size: width },
            fields: "pixelSize"
          }
        }
      end
    end

    # バナー行の左上(A1)に Sidekiq Web UI へのリンクを張る。
    # banner_format_request が textFormat を丸ごと上書きするため、必ずその後に適用する。
    def sidekiq_link_request(sheet_id)
      {
        update_cells: {
          range: { sheet_id: sheet_id, start_row_index: 0, end_row_index: 1,
                    start_column_index: 0, end_column_index: 1 },
          rows: [{ values: [link_cell(self.class.sidekiq_web_url, bold: true)] }],
          fields: "userEnteredFormat.textFormat(link,foregroundColor,underline,bold)"
        }
      }
    end

    # 案件名セル(D列)へ案件URLをハイパーリンクとして張る。値は書き換えず書式だけを更新する。
    # 全行を毎回書き直す（order_rowsで並びが変わるため、行位置に残った古いリンクは必ず上書きする）。
    # 前回より行数が減った場合は、余った行のリンクも消す。
    def title_link_requests(sheet_id, rows, previous_row_count)
      link_row_count = [rows.size, previous_row_count - 2].max
      return [] if link_row_count <= 0

      link_cells = Array.new(link_row_count) { |index| title_link_cell(rows.dig(index, URL_COLUMN_INDEX)) }
      [{
        update_cells: {
          range: {
            sheet_id: sheet_id, start_row_index: 2, end_row_index: 2 + link_row_count,
            start_column_index: TITLE_COLUMN_INDEX, end_column_index: TITLE_COLUMN_INDEX + 1
          },
          rows: link_cells.map { |cell| { values: [cell] } },
          fields: "userEnteredFormat.textFormat(link,foregroundColor,underline)"
        }
      }]
    end

    def title_link_cell(url)
      link_cell(url)
    end

    # URLが空ならリンクを解除するセル、あればリンク付きセルを返す。
    def link_cell(url, bold: nil)
      text_format = { link: nil, underline: false }
      unless url.to_s.strip.empty?
        text_format = { link: { uri: url.to_s.strip }, foreground_color: LINK_FOREGROUND_COLOR, underline: true }
      end
      text_format[:bold] = bold unless bold.nil?

      { user_entered_format: { text_format: text_format } }
    end

    # データ0件でも範囲が壊れないよう、end_row_indexは最低3を確保する。
    def basic_filter_request(sheet_id, total_row_count)
      {
        set_basic_filter: {
          filter: {
            range: {
              sheet_id: sheet_id,
              start_row_index: 1,
              end_row_index: [total_row_count, 3].max,
              start_column_index: 0,
              end_column_index: SHEET_COLUMN_COUNT
            }
          }
        }
      }
    end

    def full_width_range(sheet_id, start_row_index, end_row_index)
      { sheet_id: sheet_id, start_row_index: start_row_index, end_row_index: end_row_index,
        start_column_index: 0, end_column_index: SHEET_COLUMN_COUNT }
    end

    def write_range!(range, values, value_input_option:)
      body = Google::Apis::SheetsV4::ValueRange.new(range: range, values: values)
      @service.update_spreadsheet_value(@spreadsheet_id, range, body, value_input_option: value_input_option)
    end

    def clear_range!(range)
      @service.clear_values(@spreadsheet_id, range, Google::Apis::SheetsV4::ClearValuesRequest.new)
    end

    def batch_update!(requests)
      return if requests.empty?

      request_body = Google::Apis::SheetsV4::BatchUpdateSpreadsheetRequest.new(requests: requests)
      @service.batch_update_spreadsheet(@spreadsheet_id, request_body)
    end
  end
end
