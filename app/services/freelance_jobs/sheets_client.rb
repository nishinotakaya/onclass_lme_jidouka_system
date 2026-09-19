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
    # 案件URL列を除いた、シート本体の列数（🌟おすすめ 〜 取得日時）。
    # チェックボックス列を持つシートは、これに先頭1列が足されて15列になる。
    BASE_SHEET_COLUMN_COUNT = 14
    URL_COLUMN_INDEX = 5
    TITLE_COLUMN_INDEX = 3
    MAX_READ_ROW_COUNT = 2000

    # チェックボックス列（先頭列）。checkbox_column: true のシートにだけ付く。
    # チェック状態は行位置ではなく案件URLをキーに引き継ぐ（並べ替えで行が動くため）。
    CHECKBOX_HEADER_LABEL = "☑"
    CHECKBOX_COLUMN_WIDTH = 40

    # 行モデル1列目のヘッダー文言。シート上でこのラベルが何列目にあるかが、そのまま
    # 行モデルの開始位置（＝チェックボックス列の有無）になる。
    # チェックボックス列のヘッダーは利用者が自由に付け替えられる（例:「応募チェック」）ので、
    # そちらの文言でレイアウトを判定してはいけない。
    STAR_HEADER_LABEL = FreelanceJobs::RowBuilder::HEADER.first

    # 案件URL列(230px)を除いた14列分。チェックボックス列の幅は先頭に足す。
    # 表示件数("300件")を置く列はNo.用の45pxだと狭いので60pxにしている。
    COLUMN_WIDTHS = [90, 60, 130, 300, 95, 130, 400, 260, 130, 80, 130, 120, 330, 120].freeze

    BANNER_BACKGROUND_COLOR = { red: 0.93, green: 0.93, blue: 0.93 }.freeze
    HEADER_BACKGROUND_COLOR = { red: 0.16, green: 0.40, blue: 0.75 }.freeze
    STARRED_BACKGROUND_COLOR = { red: 1.0, green: 0.97, blue: 0.80 }.freeze
    WHITE_BACKGROUND_COLOR = { red: 1.0, green: 1.0, blue: 1.0 }.freeze
    FORMULA_TRIGGER_CHARS = ["=", "+", "-", "@"].freeze
    LINK_FOREGROUND_COLOR = { red: 0.05, green: 0.35, blue: 0.75 }.freeze
    # バナー行の左上(A1)に置く Sidekiq Web UI へのリンク。ラベルと遷移先。
    SIDEKIQ_LINK_LABEL = "sidekiq"
    # バナー行の構成（チェックボックス列がある場合は全体が1列ずれる）:
    # Sidekiqリンク / フィルター後の表示件数 / 以降は結合してバナー本文。
    BASE_SIDEKIQ_LINK_COLUMN_INDEX = 0
    BASE_VISIBLE_COUNT_COLUMN_INDEX = 1
    BASE_BANNER_TEXT_COLUMN_INDEX = 2
    # 表示件数の集計対象列。分類列は全行必ず埋まるのでCOUNTAの母数にできる。
    BASE_VISIBLE_COUNT_TARGET_COLUMN_INDEX = 2
    # データ開始行（1行目=バナー、2行目=ヘッダー）。
    FIRST_DATA_ROW_NUMBER = 3

    # レベル列の位置。行モデルでは index 6 だが、案件URL列(index 5)はシートに書かないため
    # シート本体では1つ手前にずれる。既定フィルターの対象列として使う。
    LEVEL_ROW_COLUMN_INDEX = 6
    BASE_LEVEL_COLUMN_INDEX = LEVEL_ROW_COLUMN_INDEX - 1
    DEFAULT_SIDEKIQ_WEB_URL = "https://onclass-lme-jidouka-app-857ffde75fc4.herokuapp.com/sidekiq"

    def self.sidekiq_web_url
      ENV.fetch("SIDEKIQ_WEB_URL", DEFAULT_SIDEKIQ_WEB_URL)
    end

    # checkbox_column: 先頭にチェックボックス列を持たせるか（プロファイルごとに決まる）。
    # hidden_level_marker: 既定のフィルターで畳むレベル表記（部分一致）。nilならフィルターは素のまま。
    def initialize(spreadsheet_id:, sheet_gid:, checkbox_column: false, hidden_level_marker: nil)
      @spreadsheet_id = spreadsheet_id
      @sheet_gid = sheet_gid
      @checkbox_column = checkbox_column
      @hidden_level_marker = hidden_level_marker
      @checkbox_states_by_url = {}
      # シート上のチェックボックス列ヘッダー（付け替えられていたらそれを引き継ぐ）
      @checkbox_header_label = nil
      @service = build_service
    end

    # シートを行モデル（ROW_COLUMN_COUNT列）として読む。
    # 案件名セルのハイパーリンクを URL_COLUMN_INDEX に差し込んで返すため、
    # 値だけを返す get_spreadsheet_values ではなくグリッドデータを取得する。
    def read_rows(max_row_count: MAX_READ_ROW_COUNT)
      range = "'#{sheet_name}'!A1:#{sheet_last_column}#{max_row_count}"
      spreadsheet = @service.get_spreadsheet(
        @spreadsheet_id,
        ranges: [range],
        include_grid_data: true,
        fields: "sheets.data.rowData.values(formattedValue,hyperlink,userEnteredFormat.textFormat.link)"
      )
      row_data = spreadsheet.sheets&.first&.data&.first&.row_data || []
      # チェックボックス列を足した最初の実行では、まだ旧レイアウト（先頭列が🌟おすすめ）の
      # シートを読むことになるため、列のずれは設定ではなく実データから判定する。
      offset = detect_checkbox_offset(row_data)
      @checkbox_states_by_url = collect_checkbox_states(row_data, offset)
      @checkbox_header_label = detect_checkbox_header_label(row_data, offset)
      rows = row_data.map { |grid_row| build_row_model(grid_row, offset) }
      rows.pop while rows.last && rows.last.all? { |value| value.to_s.empty? }
      rows
    end

    # 書き込み手順（空になる瞬間を作らない）:
    # 1. 隠しシート（backup_sheet_name）へ現在値(backup_values)を案件URL列込みで退避
    # 2. バナー行の既存の結合を解く（★値を書く前に必ず行う。理由は unmerge_banner_row! を参照）
    # 3. A1:最終列(n)を上書き（案件URL列は書かない。数式化しうる文字列は'を付けてガード）
    # 4. 余った行だけclear
    # 5. 書式（basic_filterを取得してから安全に張り替え）＋案件名セルへリンク付与
    #    ＋チェックボックスのデータ入力規則
    def replace_sheet(banner_text:, header:, rows:, starred_row_indexes:, backup_values:, column_widths: COLUMN_WIDTHS)
      metadata = fetch_metadata
      main_sheet = resolve_main_sheet!(metadata)
      @sheet_name = main_sheet.properties.title

      backup_existing_values!(metadata, backup_values)

      previous_row_count = main_sheet.properties.grid_properties.row_count.to_i
      unmerge_banner_row!(main_sheet)
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

    # チェックボックス列を持つシートでは、本体の列が1つ右にずれる。
    def checkbox_offset
      @checkbox_column ? 1 : 0
    end

    def sheet_column_count
      BASE_SHEET_COLUMN_COUNT + checkbox_offset
    end

    def sheet_last_column
      column_letter(sheet_column_count - 1)
    end

    def column_letter(column_index)
      ("A".."Z").to_a.fetch(column_index)
    end

    def sidekiq_link_column_index
      checkbox_offset + BASE_SIDEKIQ_LINK_COLUMN_INDEX
    end

    def visible_count_column_index
      checkbox_offset + BASE_VISIBLE_COUNT_COLUMN_INDEX
    end

    def banner_text_column_index
      checkbox_offset + BASE_BANNER_TEXT_COLUMN_INDEX
    end

    def sheet_title_column_index
      checkbox_offset + TITLE_COLUMN_INDEX
    end

    # シート上にチェックボックス列があるかを実データから判定する。ヘッダー行(2行目)の
    # 先頭セルがチェックボックス列の見出しなら、本体は1列右にずれている。
    def detect_checkbox_offset(row_data)
      header_cells = row_data[1]&.values || []
      star_column_index = header_cells.find_index do |cell|
        cell&.formatted_value.to_s == STAR_HEADER_LABEL
      end
      # 空シートや読み取り失敗時は、プロファイルの設定どおりのレイアウトで書く。
      star_column_index || checkbox_offset
    end

    # チェックボックス列のヘッダー文言をシートから引き継ぐ。
    # 利用者が「応募チェック」等に付け替えていても、書き戻しで元に戻さないため。
    def detect_checkbox_header_label(row_data, offset)
      return nil if offset.zero?

      label = (row_data[1]&.values || []).first&.formatted_value.to_s
      label.empty? ? nil : label
    end

    # 案件URL → チェック状態。次の書き込みでそのまま書き戻すために保持する。
    def collect_checkbox_states(row_data, offset)
      return {} if offset.zero?

      row_data.each_with_object({}) do |grid_row, states|
        cells = grid_row.values || []
        url = FreelanceJobs::JobPosting.normalize_url(cell_link_uri(cells[offset + TITLE_COLUMN_INDEX]))
        next if url.empty?

        states[url] = cells.first&.formatted_value.to_s.casecmp?("true")
      end
    end

    # グリッドデータ1行 → 案件URLを差し込んだ ROW_COLUMN_COUNT 列の行モデル。
    # 行モデルにチェックボックス列は含めない（マージ側は列の増減を意識しなくてよい）。
    def build_row_model(grid_row, offset)
      cells = grid_row.values || []
      sheet_values = Array.new(BASE_SHEET_COLUMN_COUNT) { |index| cells[offset + index]&.formatted_value.to_s }
      sheet_values.insert(URL_COLUMN_INDEX, cell_link_uri(cells[offset + TITLE_COLUMN_INDEX]))
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

    # バナー行 / ヘッダー行 / データ行を、シート上の列数ぶんの配列にして返す。
    # チェックボックス列がある場合は、各行の先頭に1セル足す。
    def build_write_values(banner_text, header, rows)
      # Sidekiqリンク、フィルター後の表示件数（数式）、以降(結合セル)=バナー本文。
      # 件数セルだけは数式として評価させたいので escape_formula を通さない。
      banner_row = blank_checkbox_cell +
                    [SIDEKIQ_LINK_LABEL, visible_count_formula(rows.size), escape_formula(banner_text)] +
                    Array.new(BASE_SHEET_COLUMN_COUNT - 3, "")
      header_row = checkbox_header_cell + sheet_row_values(header)
      data_rows = rows.each_with_index.map { |row, index| checkbox_cell(row) + sheet_row_values(row) }

      [banner_row, header_row, *data_rows]
    end

    def sheet_row_values(row)
      strip_url_column(row).map { |value| escape_formula(value) }
    end

    def blank_checkbox_cell
      Array.new(checkbox_offset, "")
    end

    def checkbox_header_cell
      Array.new(checkbox_offset, @checkbox_header_label || CHECKBOX_HEADER_LABEL)
    end

    # 既存シートのチェック状態を案件URLで引き継ぐ。並べ替えで行位置が変わっても
    # 同じ案件のチェックが残り、シートに無かった新規案件だけが未チェックになる。
    def checkbox_cell(row)
      return [] if checkbox_offset.zero?

      url = FreelanceJobs::JobPosting.normalize_url(row[URL_COLUMN_INDEX])
      [@checkbox_states_by_url.fetch(url, false)]
    end

    # フィルターで絞り込んだあとに実際に見えている行数を出す。
    # SUBTOTAL(103, ...) は COUNTA と同じ数え方をしつつ、フィルターで隠れた行を数えない。
    # データ0件のときは範囲が作れないので固定文字列にする。
    def visible_count_formula(data_row_count)
      return "0件" if data_row_count.zero?

      last_row_number = FIRST_DATA_ROW_NUMBER + data_row_count - 1
      target_column = column_letter(checkbox_offset + BASE_VISIBLE_COUNT_TARGET_COLUMN_INDEX)
      range = "$#{target_column}$#{FIRST_DATA_ROW_NUMBER}:$#{target_column}$#{last_row_number}"
      %(=SUBTOTAL(103,#{range})&"件")
    end

    # 行モデルから案件URL列を落として、シート本体の14列にする。
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
      write_range!("'#{@sheet_name}'!A1:#{sheet_last_column}#{values.size}", values,
                    value_input_option: "USER_ENTERED")
    end

    def clear_leftover_rows!(previous_row_count, written_row_count)
      return if previous_row_count <= written_row_count

      clear_range!("'#{@sheet_name}'!A#{written_row_count + 1}:#{sheet_last_column}#{previous_row_count}")
    end

    def apply_formatting!(main_sheet, total_row_count, starred_row_indexes, column_widths, rows, previous_row_count)
      sheet_id = main_sheet.properties.sheet_id
      requests = []

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
      requests.concat(column_width_requests(sheet_id, blank_checkbox_cell.empty? ? column_widths : [CHECKBOX_COLUMN_WIDTH, *column_widths]))
      requests << sidekiq_link_request(sheet_id)
      requests.concat(title_link_requests(sheet_id, rows, previous_row_count))
      requests.concat(clear_conditional_format_requests(main_sheet))
      requests.concat(checkbox_validation_requests(sheet_id, total_row_count, previous_row_count))
      requests << basic_filter_request(sheet_id, total_row_count)

      batch_update!(requests)
    end

    # 値の書き込みより先にバナー行の結合を解く。
    # 結合されたセル範囲へ値を書くと、左上のセル以外への書き込みは黙って捨てられる。
    # そのためバナーのレイアウト（結合の開始列）を変えた回は、先に解いておかないと
    # 新しい位置のセルが空のままになる（B1:N1結合のままC1へバナー本文を書いて消えた実例あり）。
    def unmerge_banner_row!(main_sheet)
      requests = unmerge_row_zero_requests(main_sheet.properties.sheet_id, main_sheet.merges)
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

    # A1(Sidekiqリンク)とB1(表示件数)は独立させ、C1:N1だけをバナー本文として結合する。
    def merge_banner_request(sheet_id)
      {
        merge_cells: {
          range: { sheet_id: sheet_id, start_row_index: 0, end_row_index: 1,
                    start_column_index: banner_text_column_index, end_column_index: sheet_column_count },
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

    # 🌟おすすめ列はwrite_sheet.rbに合わせてフォントを少し大きくする。
    def recommend_column_font_request(sheet_id, total_row_count)
      {
        repeat_cell: {
          range: { sheet_id: sheet_id, start_row_index: 2, end_row_index: total_row_count,
                    start_column_index: checkbox_offset, end_column_index: checkbox_offset + 1 },
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

    # このシートの書式はバッチが持ち主なので、手で追加された条件付き書式も含めて毎回消す。
    # 残っていると行が入れ替わった後も同じ行位置が塗られたままになり、意味のない色になる。
    # 削除はindexが前に詰まる仕様なので、必ず後ろのルールから消す（降順）。
    def clear_conditional_format_requests(main_sheet)
      sheet_id = main_sheet.properties.sheet_id
      ((main_sheet.conditional_formats || []).size - 1).downto(0).map do |index|
        { delete_conditional_format_rule: { sheet_id: sheet_id, index: index } }
      end
    end

    # チェックボックス列にデータ入力規則(BOOLEAN)を張る。データ行の範囲ぴったりに張り直すので、
    # 案件が増えればチェックボックスもその行まで自動で伸びる。
    # 前回より行数が減ったときは、余った行の入力規則を消す（ruleを渡さないと解除になる）。
    def checkbox_validation_requests(sheet_id, total_row_count, previous_row_count)
      return [] if checkbox_offset.zero?

      requests = []
      if total_row_count > 2
        requests << {
          set_data_validation: {
            range: checkbox_column_range(sheet_id, 2, total_row_count),
            rule: { condition: { type: "BOOLEAN" }, strict: true, show_custom_ui: true }
          }
        }
      end
      if previous_row_count > total_row_count
        requests << { set_data_validation: { range: checkbox_column_range(sheet_id, total_row_count, previous_row_count) } }
      end
      requests
    end

    def checkbox_column_range(sheet_id, start_row_index, end_row_index)
      { sheet_id: sheet_id, start_row_index: start_row_index, end_row_index: end_row_index,
        start_column_index: 0, end_column_index: 1 }
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

    # バナー行の Sidekiq セルに Sidekiq Web UI へのリンクを張る。
    # banner_format_request が textFormat を丸ごと上書きするため、必ずその後に適用する。
    def sidekiq_link_request(sheet_id)
      {
        update_cells: {
          range: { sheet_id: sheet_id, start_row_index: 0, end_row_index: 1,
                    start_column_index: sidekiq_link_column_index,
                    end_column_index: sidekiq_link_column_index + 1 },
          rows: [{ values: [link_cell(self.class.sidekiq_web_url, bold: true)] }],
          fields: "userEnteredFormat.textFormat(link,foregroundColor,underline,bold)"
        }
      }
    end

    # 案件名セルへ案件URLをハイパーリンクとして張る。値は書き換えず書式だけを更新する。
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
            start_column_index: sheet_title_column_index, end_column_index: sheet_title_column_index + 1
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
      filter = {
        range: {
          sheet_id: sheet_id,
          start_row_index: 1,
          end_row_index: [total_row_count, 3].max,
          start_column_index: 0,
          end_column_index: sheet_column_count
        }
      }
      criteria = default_filter_criteria
      filter[:criteria] = criteria if criteria

      { set_basic_filter: { filter: filter } }
    end

    # 既定でレベル列に掛けておくフィルター条件。
    # 「★★★を含まない行だけ表示」にしているので、上級の表記が
    # （★★★ 上級（リード・設計／7年以上）のように）増えても追従できる。
    def default_filter_criteria
      return nil if @hidden_level_marker.to_s.empty?

      column_index = checkbox_offset + BASE_LEVEL_COLUMN_INDEX
      {
        column_index.to_s => Google::Apis::SheetsV4::FilterCriteria.new(
          condition: Google::Apis::SheetsV4::BooleanCondition.new(
            type: "TEXT_NOT_CONTAINS",
            values: [Google::Apis::SheetsV4::ConditionValue.new(user_entered_value: @hidden_level_marker)]
          )
        )
      }
    end

    def full_width_range(sheet_id, start_row_index, end_row_index)
      { sheet_id: sheet_id, start_row_index: start_row_index, end_row_index: end_row_index,
        start_column_index: 0, end_column_index: sheet_column_count }
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
