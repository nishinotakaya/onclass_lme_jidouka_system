# frozen_string_literal: true

require "google/apis/sheets_v4"
require "googleauth"
require "json"

module FreelanceJobs
  # 案件一覧シートの読み取り・書き込みを担当する。
  # 認証は Youtube::CompetitorWorker#build_sheets_service と同じ流儀
  # （サービスアカウントJSON鍵、ENV["GOOGLE_APPLICATION_CREDENTIALS"]）。
  class SheetsClient
    COLUMN_COUNT = 15
    BACKUP_SHEET_NAME = "_backup_シート1"

    COLUMN_WIDTHS = [90, 45, 130, 300, 95, 230, 130, 400, 260, 130, 80, 130, 120, 330, 120].freeze

    BANNER_BACKGROUND_COLOR = { red: 0.93, green: 0.93, blue: 0.93 }.freeze
    HEADER_BACKGROUND_COLOR = { red: 0.16, green: 0.40, blue: 0.75 }.freeze
    STARRED_BACKGROUND_COLOR = { red: 1.0, green: 0.97, blue: 0.80 }.freeze
    WHITE_BACKGROUND_COLOR = { red: 1.0, green: 1.0, blue: 1.0 }.freeze
    FORMULA_TRIGGER_CHARS = ["=", "+", "-", "@"].freeze

    def initialize(spreadsheet_id:, sheet_name:)
      @spreadsheet_id = spreadsheet_id
      @sheet_name = sheet_name
      @service = build_service
    end

    def read_values(range)
      response = @service.get_spreadsheet_values(@spreadsheet_id, "'#{@sheet_name}'!#{range}")
      response.values || []
    end

    # 書き込み手順（空になる瞬間を作らない）:
    # 1. 隠しシート_backup_シート1へ現在値(backup_values)を退避
    # 2. A1:O(n)を上書き（数式化しうる文字列は'を付けてガード）
    # 3. 余った行だけclear
    # 4. 書式（既存merge/basic_filterを取得してから安全に張り替え）
    def replace_sheet(banner_text:, header:, rows:, starred_row_indexes:, backup_values:, column_widths: COLUMN_WIDTHS)
      metadata = fetch_metadata
      main_sheet = find_sheet(metadata, @sheet_name)
      raise FreelanceJobs::FetchError, "Sheet not found: #{@sheet_name}" unless main_sheet

      backup_existing_values!(metadata, backup_values)

      values = build_write_values(banner_text, header, rows)
      write_values!(values)
      clear_leftover_rows!(main_sheet, values.size)
      apply_formatting!(main_sheet, values.size, starred_row_indexes, column_widths)
    end

    private

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

    def find_sheet(metadata, title)
      metadata.sheets&.find { |sheet| sheet.properties&.title == title }
    end

    def backup_existing_values!(metadata, backup_values)
      ensure_backup_sheet_exists!(metadata)
      clear_range!("'#{BACKUP_SHEET_NAME}'!A1:Z2000")
      return if backup_values.nil? || backup_values.empty?

      write_range!("'#{BACKUP_SHEET_NAME}'!A1", backup_values, value_input_option: "RAW")
    end

    def ensure_backup_sheet_exists!(metadata)
      return if find_sheet(metadata, BACKUP_SHEET_NAME)

      batch_update!([{
        add_sheet: {
          properties: {
            title: BACKUP_SHEET_NAME,
            hidden: true,
            grid_properties: { row_count: 2000, column_count: COLUMN_COUNT }
          }
        }
      }])
    end

    def build_write_values(banner_text, header, rows)
      banner_row = [banner_text] + Array.new(header.size - 1, "")
      [banner_row, header, *rows].map { |row| row.map { |value| escape_formula(value) } }
    end

    # 先頭が = + - @ の文字列は数式と解釈されうるため ' を付けてテキスト扱いにする。
    def escape_formula(value)
      return value unless value.is_a?(String)
      return value if value.empty? || !FORMULA_TRIGGER_CHARS.include?(value[0])

      "'#{value}"
    end

    def write_values!(values)
      write_range!("'#{@sheet_name}'!A1:O#{values.size}", values, value_input_option: "USER_ENTERED")
    end

    def clear_leftover_rows!(main_sheet, written_row_count)
      previous_row_count = main_sheet.properties.grid_properties.row_count.to_i
      return if previous_row_count <= written_row_count

      clear_range!("'#{@sheet_name}'!A#{written_row_count + 1}:O#{previous_row_count}")
    end

    def apply_formatting!(main_sheet, total_row_count, starred_row_indexes, column_widths)
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

    def merge_banner_request(sheet_id)
      {
        merge_cells: {
          range: full_width_range(sheet_id, 0, 1),
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
              end_column_index: COLUMN_COUNT
            }
          }
        }
      }
    end

    def full_width_range(sheet_id, start_row_index, end_row_index)
      { sheet_id: sheet_id, start_row_index: start_row_index, end_row_index: end_row_index,
        start_column_index: 0, end_column_index: COLUMN_COUNT }
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
