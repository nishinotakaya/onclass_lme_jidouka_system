# frozen_string_literal: true

require "date"
require "time"

module FreelanceJobs
  # 6サイトの取得→分類→整形→既存シートとのマージ→書き込みを束ねるオーケストレーション。
  class ResearchService
    DEFAULT_SPREADSHEET_ID = "1kuMNWceZzUHHn6zi9fcW8dUczHcquZlvjr3BEPcNRxM"
    DEFAULT_SHEET_NAME = "シート1"

    SOURCES = [
      FreelanceJobs::Sources::Crowdworks,
      FreelanceJobs::Sources::Lancers,
      FreelanceJobs::Sources::Coconala,
      FreelanceJobs::Sources::Shufti,
      FreelanceJobs::Sources::Mamaworks,
      FreelanceJobs::Sources::Craudia
    ].freeze

    HEADER_LABEL = "🌟おすすめ"

    def initialize(run_window_label:,
                    spreadsheet_id: ENV.fetch("FREELANCE_JOBS_SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID),
                    sheet_name: ENV.fetch("FREELANCE_JOBS_SHEET_NAME", DEFAULT_SHEET_NAME),
                    fetcher: nil,
                    sheets_client: nil,
                    now: Time.now.getlocal("+09:00"))
      @run_window_label = run_window_label
      @spreadsheet_id = spreadsheet_id
      @sheet_name = sheet_name
      @shared_fetcher = fetcher
      @sheets_client = sheets_client
      @now = now
      @today = now.to_date
    end

    def call
      fetched_counts = {}
      succeeded_sites = []
      failures = []
      postings = []

      SOURCES.each do |source_class|
        begin
          source_postings = fetch_from(source_class)
          fetched_counts[source_class::SITE_NAME] = source_postings.size
          succeeded_sites << source_class::SITE_NAME
          postings.concat(source_postings)
        rescue StandardError => error
          message = "#{source_class::SITE_NAME}（#{error.class}: #{error.message[0, 60]}）"
          failures << message
          FreelanceJobs.logger.error("[FreelanceJobs::ResearchService] #{message}")
        end
      end

      rows = build_rows(postings)
      starred_candidates = count_starred(rows)

      if succeeded_sites.empty?
        return aborted_summary("取得に成功したサイトがありません", fetched_counts, succeeded_sites, failures, rows.size,
                                starred_candidates)
      end
      if rows.empty?
        return aborted_summary("分類済み候補が0件です", fetched_counts, succeeded_sites, failures, rows.size, starred_candidates)
      end

      raw_values = sheets_client.read_values("A1:O2000")
      existing_rows, header_missing = extract_existing_rows(raw_values)

      if header_missing
        return aborted_summary("既存シートにヘッダー行(#{HEADER_LABEL})が見つかりません", fetched_counts, succeeded_sites, failures,
                                rows.size, starred_candidates)
      end

      # C2: 新規行(既存シートにURLが無いもの)は🌟付きだけをマージ対象にする。
      # 既存行にURLが一致する行はJ/K/L/M/O更新のため🌟の有無に関わらず対象にする。
      rows_for_merge = filter_new_rows_for_merge(rows, existing_rows)

      merge_result = FreelanceJobs::SheetMerger.merge(
        existing_rows: existing_rows,
        new_rows: rows_for_merge,
        succeeded_sites: succeeded_sites,
        today: @today
      )

      starred_row_indexes = merge_result.rows.each_index.select { |index| merge_result.rows[index][0].to_s.include?("🌟") }

      sheets_client.replace_sheet(
        banner_text: build_banner_text(merge_result, failures),
        header: FreelanceJobs::RowBuilder::HEADER,
        rows: merge_result.rows,
        starred_row_indexes: starred_row_indexes,
        backup_values: raw_values
      )

      {
        aborted: false,
        reason: nil,
        fetched: fetched_counts,
        candidates: rows.size,
        starred_candidates: starred_candidates,
        added: merge_result.added,
        updated: merge_result.updated,
        removed: merge_result.removed,
        total: merge_result.total,
        failures: failures,
        succeeded_sites: succeeded_sites,
        backup_sheet: FreelanceJobs::SheetsClient::BACKUP_SHEET_NAME
      }
    end

    private

    def fetch_from(source_class)
      fetcher = @shared_fetcher || FreelanceJobs::HttpFetcher.new(interval: source_class::REQUEST_INTERVAL,
                                                                    logger: FreelanceJobs.logger)
      source_class.new(fetcher: fetcher, today: @today).fetch
    end

    # 分類 → 対象外(category nil)を除外 → 行に整形 → URLで重複除去。
    def build_rows(postings)
      rows_by_url = {}

      postings.each do |posting|
        classification = FreelanceJobs::Classifier.classify(posting, today: @today)
        next if classification.category.nil?

        row = FreelanceJobs::RowBuilder.build(posting, classification, now: @now)
        rows_by_url[row[5]] ||= row
      end

      rows_by_url.values
    end

    def count_starred(rows)
      rows.count { |row| row[0].to_s.include?("🌟") }
    end

    # C2: 既存シートにURLが無い(=真に新規の)行は🌟付きだけを残す。既存行にURLが一致する行は
    # J/K/L/M/O更新のためSheetMerger側でのマッチングが必要なので、🌟の有無に関わらず残す。
    def filter_new_rows_for_merge(rows, existing_rows)
      existing_urls = existing_rows.each_with_object({}) do |row, memo|
        url = FreelanceJobs::JobPosting.normalize_url(row[FreelanceJobs::SheetMerger::URL_COLUMN_INDEX])
        memo[url] = true unless url.empty?
      end

      rows.select do |row|
        url = FreelanceJobs::JobPosting.normalize_url(row[FreelanceJobs::SheetMerger::URL_COLUMN_INDEX])
        existing_urls[url] || row[0].to_s.include?("🌟")
      end
    end

    # A5: 読み取った値が1セルでもあるのにヘッダー行が見つからない場合は中止扱いにする。
    # 完全に空のシートだけ「新規」として扱う。戻り値は [existing_rows, header_missing]。
    def extract_existing_rows(raw_values)
      return [[], false] if raw_values.nil? || raw_values.empty?

      header_index = raw_values.find_index { |row| row[0].to_s == HEADER_LABEL }
      return [[], true] if header_index.nil?

      [raw_values[(header_index + 1)..] || [], false]
    end

    def sheets_client
      @sheets_client ||= FreelanceJobs::SheetsClient.new(spreadsheet_id: @spreadsheet_id, sheet_name: @sheet_name)
    end

    def build_banner_text(merge_result, failures)
      success_count = SOURCES.size - failures.size
      text = "⏰ 自動更新バッチ：#{@run_window_label} に自動実行（Sidekiq）｜" \
             "最終実行 #{@now.strftime("%Y-%m-%d %H:%M")}｜" \
             "取得元 #{SOURCES.size}サイト（成功 #{success_count}/#{SOURCES.size}）｜" \
             "掲載 #{merge_result.total}件（新規 +#{merge_result.added}／期限切れ削除 −#{merge_result.removed}）"
      text += "｜⚠ 取得失敗: #{failures.join("、")}" unless failures.empty?
      text
    end

    # A1/A5: シートに一切書かずに中止する場合の summary（書き込み・バックアップは行わない）。
    def aborted_summary(reason, fetched_counts, succeeded_sites, failures, candidates, starred_candidates)
      FreelanceJobs.logger.error("[FreelanceJobs::ResearchService] aborted: #{reason}")
      {
        aborted: true,
        reason: reason,
        fetched: fetched_counts,
        candidates: candidates,
        starred_candidates: starred_candidates,
        added: 0,
        updated: 0,
        removed: 0,
        total: 0,
        failures: failures,
        succeeded_sites: succeeded_sites,
        backup_sheet: nil
      }
    end
  end
end
