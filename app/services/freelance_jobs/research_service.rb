# frozen_string_literal: true

require "date"
require "time"

module FreelanceJobs
  # 各サイトの取得→分類→整形→既存シートとのマージ→書き込みを束ねるオーケストレーション。
  class ResearchService
    DEFAULT_SPREADSHEET_ID = "1kuMNWceZzUHHn6zi9fcW8dUczHcquZlvjr3BEPcNRxM"

    HEADER_LABEL = "🌟おすすめ"

    # 環境変数FREELANCE_JOBS_EXCLUDED_SITES（サイト表示名を「,」または「、」区切り）を
    # 取得対象外サイト名の配列に変換する。前後の空白は除き、空要素は取り除く。
    def self.excluded_sites_from_env(value = ENV.fetch("FREELANCE_JOBS_EXCLUDED_SITES", ""))
      value.split(/[,、]/).map(&:strip).reject(&:empty?)
    end

    def initialize(profile:,
                    run_window_label:,
                    spreadsheet_id: ENV.fetch("FREELANCE_JOBS_SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID),
                    excluded_sites: self.class.excluded_sites_from_env,
                    fetcher: nil,
                    sheets_client: nil,
                    now: Time.now.getlocal("+09:00"))
      @profile = profile
      @run_window_label = run_window_label
      @spreadsheet_id = spreadsheet_id
      # 実在するサイト名だけを対象外として扱う（typoはバナーに出さず、callの冒頭でログ警告する）。
      known_site_names = @profile.source_specs.map { |source_class, _options| source_class::SITE_NAME }
      @excluded_sites = excluded_sites.uniq & known_site_names
      @unknown_excluded_sites = excluded_sites.uniq - known_site_names
      @sources = @profile.source_specs.reject { |source_class, _options| @excluded_sites.include?(source_class::SITE_NAME) }
      @shared_fetcher = fetcher
      @sheets_client = sheets_client
      @now = now
      @today = now.to_date
    end

    def call
      warn_about_unknown_excluded_sites

      fetched_counts = {}
      succeeded_sites = []
      # failuresは原因つき（summary・ログ用）、failed_sitesはサイト名だけ（バナー用）。
      # バナーはURLやHTTPステータスまで出すと読めなくなるので、原因はログ側だけに残す。
      failures = []
      failed_sites = []
      postings = []

      @sources.each do |source_class, options|
        site_name = source_class::SITE_NAME
        begin
          source_postings = fetch_from(source_class, options)
          fetched_counts[site_name] = source_postings.size
          succeeded_sites << site_name
          postings.concat(source_postings)
        rescue StandardError => error
          failures << "#{site_name}（#{error.message[0, 60]}）"
          failed_sites << site_name
          FreelanceJobs.logger.error("[FreelanceJobs::ResearchService] #{site_name} #{error.class}: #{error.message}")
        end
      end

      # closedなpostingは分類器に通さない（build_rowsの対象外）。分類器に通すとcategoryが
      # nilになって黙って消えることがあり、募集終了として既存行を消すための情報（URL）が
      # 失われてしまうため、ここで先に分けてclosed_urlsとして扱う。
      closed_postings, open_postings = postings.partition(&:closed?)
      closed_urls = closed_postings.map(&:url)

      rows = build_rows(open_postings)
      starred_candidates = count_starred(rows)

      if succeeded_sites.empty?
        return aborted_summary("取得に成功したサイトがありません", fetched_counts, succeeded_sites, failures, rows.size,
                                starred_candidates)
      end
      if rows.empty?
        return aborted_summary("分類済み候補が0件です", fetched_counts, succeeded_sites, failures, rows.size, starred_candidates)
      end

      # 行モデル（案件URLをindex5に差し込んだ15列）で読む。シート上に案件URL列は無く、
      # 案件名セルのハイパーリンクからURLが復元される。
      raw_values = sheets_client.read_rows
      existing_rows, header_missing = extract_existing_rows(raw_values)

      if header_missing
        return aborted_summary("既存シートにヘッダー行(#{HEADER_LABEL})が見つかりません", fetched_counts, succeeded_sites, failures,
                                rows.size, starred_candidates)
      end

      # C2: 新規行(既存シートにURLが無いもの)は🌟付きだけをマージ対象にする（profile.new_rows_require_starがtrueの時だけ）。
      # 既存行にURLが一致する行はJ/K/L/M/O更新のため🌟の有無に関わらず対象にする。
      rows_for_merge = @profile.new_rows_require_star ? filter_new_rows_for_merge(rows, existing_rows) : rows

      # 同じURLが別サイトからopenとしても来た場合はclosedを優先する（closed_urlsに入っていれば行は削除される）。
      merge_result = FreelanceJobs::SheetMerger.merge(
        existing_rows: existing_rows,
        new_rows: rows_for_merge,
        succeeded_sites: succeeded_sites,
        excluded_sites: @excluded_sites,
        category_order: @profile.category_order,
        today: @today,
        closed_urls: closed_urls
      )

      starred_row_indexes = merge_result.rows.each_index.select { |index| merge_result.rows[index][0].to_s.include?("🌟") }
      # AC-04: 追加日(index15)が今日の行だけを強調表示の対象にする。既存行は追加日が保持されるため
      # ここには含まれず、今回新たにシートへ載った行だけが塗られる。
      today_added_on_text = @today.strftime("%Y-%m-%d")
      new_today_row_indexes = merge_result.rows.each_index.select do |index|
        merge_result.rows[index][FreelanceJobs::SheetMerger::ADDED_ON_COLUMN_INDEX] == today_added_on_text
      end

      sheets_client.replace_sheet(
        banner_text: build_banner_text(merge_result, failed_sites, succeeded_sites),
        header: @profile.header,
        rows: merge_result.rows,
        starred_row_indexes: starred_row_indexes,
        new_today_row_indexes: new_today_row_indexes,
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
        excluded_sites: @excluded_sites,
        profile: @profile.key,
        sheet_gid: @profile.sheet_gid,
        backup_sheet: sheets_client.backup_sheet_name
      }
    end

    private

    # excluded_sites指定のtypo検知。source_specsのSITE_NAMEに一つも一致しない指定はログ警告する。
    def warn_about_unknown_excluded_sites
      return if @unknown_excluded_sites.empty?

      FreelanceJobs.logger.warn(
        "[FreelanceJobs::ResearchService] FREELANCE_JOBS_EXCLUDED_SITESに未知のサイト名があります: " \
        "#{@unknown_excluded_sites.join("、")}"
      )
    end

    def fetch_from(source_class, options)
      fetcher = @shared_fetcher || FreelanceJobs::HttpFetcher.new(interval: source_class::REQUEST_INTERVAL,
                                                                    logger: FreelanceJobs.logger)
      source_class.new(fetcher: fetcher, today: @today, **options).fetch
    end

    # 分類 → 対象外(category nil)を除外 → 行に整形 → URLで重複除去。
    def build_rows(postings)
      rows_by_url = {}

      postings.each do |posting|
        classification = @profile.classifier.classify(posting, today: @today)
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
      @sheets_client ||= FreelanceJobs::SheetsClient.new(spreadsheet_id: @spreadsheet_id, sheet_gid: @profile.sheet_gid,
                                                          checkbox_column: @profile.checkbox_column,
                                                          hidden_level_marker: @profile.hidden_level_marker)
    end

    # バナーは一目で読み切れる長さに絞る。失敗の原因（HTTPステータス・URL）や実行間隔の但し書きは
    # 載せず、A1のSidekiqリンクからログを見てもらう。掲載件数はB1のSUBTOTALが出すので、
    # ここでは「フィルター前の全件数」と前回からの増減だけを持たせる。
    def build_banner_text(merge_result, failed_sites, succeeded_sites)
      parts = [
        "⏰ #{@run_window_label}更新",
        @now.strftime("%m/%d %H:%M"),
        "#{succeeded_sites.size}/#{@sources.size}サイト",
        "全#{merge_result.total}件#{row_difference_text(merge_result)}"
      ]
      parts << "⚠失敗 #{failed_sites.join("、")}" unless failed_sites.empty?
      parts << "除外 #{@excluded_sites.join("、")}" unless @excluded_sites.empty?
      parts.join("｜")
    end

    # 増減があるときだけ " +3 −1" のように付ける（0のときは何も出さない）。
    def row_difference_text(merge_result)
      difference = []
      difference << "+#{merge_result.added}" if merge_result.added.positive?
      difference << "−#{merge_result.removed}" if merge_result.removed.positive?
      difference.empty? ? "" : " #{difference.join(" ")}"
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
        excluded_sites: @excluded_sites,
        profile: @profile.key,
        sheet_gid: @profile.sheet_gid,
        backup_sheet: nil
      }
    end
  end
end
