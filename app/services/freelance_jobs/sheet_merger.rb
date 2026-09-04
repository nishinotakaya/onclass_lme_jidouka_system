# frozen_string_literal: true

require "date"

module FreelanceJobs
  # 既存のスプレッドシート行と、今回取得した新規行をマージする純粋関数。
  # 15列の配列（[🌟, No., 分類, 案件名, 掲載サイト, 案件URL, 難易度, 内容, 必要スキル,
  #             報酬, 形式, 応募状況, 締切, 一言メモ, 取得日時]）を対象にする。
  class SheetMerger
    COLUMN_COUNT = 15
    URL_COLUMN_INDEX = 5
    SITE_COLUMN_INDEX = 4
    CATEGORY_COLUMN_INDEX = 2
    DEADLINE_TEXT_COLUMN_INDEX = 12
    FETCHED_ON_COLUMN_INDEX = 14
    NUMBER_COLUMN_INDEX = 1

    # 既存行のうち自動更新して良い列（報酬・形式・応募状況・締切・取得日時）。
    # A・C・D・G・H・I・N（🌟/分類/案件名/難易度/内容/必要スキル/メモ）は手入力を保持する。
    AUTO_UPDATE_COLUMN_INDEXES = [9, 10, 11, 12, 14].freeze

    CATEGORY_ORDER = ["HTML/CSS", "Excel・スプレッドシート"].freeze
    MAX_TOTAL_ROWS = 300
    # 1回の実行で追加する新規行の上限（ラウンド2 C3）。超過分は今回は見送り、
    # 次回以降の実行で再度候補になれば追加され得る。
    MAX_NEW_ROWS_PER_RUN = 80
    REMOVE_UNKNOWN_DEADLINE_AFTER_DAYS = 60
    FAR_FUTURE_SENTINEL = Date.new(9999, 12, 31)

    Result = Struct.new(:rows, :added, :updated, :removed, :total, keyword_init: true)

    # succeeded_sites: 今回取得に成功したサイト表示名の配列。
    # 失敗したサイトの既存行は、更新も期限切れ削除もせずそのまま残す（A1）。
    # 取得日時（O列）はRowBuilderが構築時点で書き込み済みのため、ここでは受け取らない。
    def self.merge(existing_rows:, new_rows:, succeeded_sites:, today:)
      normalized_existing_rows = existing_rows.map { |row| normalize_row(row) }
      deduped_new_rows = dedupe_by_url(new_rows)
      new_rows_by_url = index_by_url(deduped_new_rows)

      existing_url_seen = {}
      surviving_existing_rows = []
      updated = 0
      removed = 0

      normalized_existing_rows.each do |row|
        url = FreelanceJobs::JobPosting.normalize_url(row[URL_COLUMN_INDEX])

        if url.empty?
          surviving_existing_rows << row # F列が空：キーにせずそのまま保持
          next
        end

        if existing_url_seen[url]
          surviving_existing_rows << row # 重複URLの2件目以降：変更しない
          next
        end
        existing_url_seen[url] = true

        matched_new_row = new_rows_by_url[url]
        if matched_new_row
          apply_update!(row, matched_new_row)
          updated += 1
          surviving_existing_rows << row
          next
        end

        unless succeeded_sites.include?(row[SITE_COLUMN_INDEX])
          surviving_existing_rows << row # 取得失敗サイトの行は触らない
          next
        end

        if should_remove?(row, today)
          removed += 1
          next
        end

        surviving_existing_rows << row
      end

      brand_new_rows_all = deduped_new_rows.reject do |row|
        existing_url_seen[FreelanceJobs::JobPosting.normalize_url(row[URL_COLUMN_INDEX])]
      end
      brand_new_rows = cap_new_rows_per_run(brand_new_rows_all)
      brand_new_object_ids = brand_new_rows.each_with_object({}) { |row, memo| memo[row.object_id] = true }

      ordered_rows = order_rows(brand_new_rows, surviving_existing_rows)
      capped_rows = cap_rows(ordered_rows, brand_new_rows)
      renumbered_rows = renumber(capped_rows)

      # addedは各種上限適用後に実際にシートへ残った新規行数にする（ラウンド2 C3）。
      added = capped_rows.count { |row| brand_new_object_ids[row.object_id] }

      Result.new(rows: renumbered_rows, added: added, updated: updated, removed: removed,
                 total: renumbered_rows.size)
    end

    def self.normalize_row(row)
      Array.new(COLUMN_COUNT) { |index| row[index].nil? ? "" : row[index] }
    end

    def self.dedupe_by_url(rows)
      seen = {}
      rows.each_with_object([]) do |row, result|
        url = FreelanceJobs::JobPosting.normalize_url(row[URL_COLUMN_INDEX])
        next if seen[url]

        seen[url] = true
        result << row
      end
    end

    def self.index_by_url(rows)
      rows.each_with_object({}) do |row, index|
        index[FreelanceJobs::JobPosting.normalize_url(row[URL_COLUMN_INDEX])] ||= row
      end
    end

    def self.apply_update!(existing_row, new_row)
      AUTO_UPDATE_COLUMN_INDEXES.each { |index| existing_row[index] = new_row[index] }
    end

    # 締切が過ぎている（当日は残す）、または締切不明で取得日時が60日より古い行を削除対象にする。
    def self.should_remove?(row, today)
      deadline_on = parse_deadline_on(row[DEADLINE_TEXT_COLUMN_INDEX], row[FETCHED_ON_COLUMN_INDEX])
      return deadline_on < today if deadline_on

      fetched_on = parse_fetched_on(row[FETCHED_ON_COLUMN_INDEX])
      return false unless fetched_on

      fetched_on < today - REMOVE_UNKNOWN_DEADLINE_AFTER_DAYS
    end

    def self.parse_deadline_on(deadline_text, fetched_on_text)
      text = deadline_text.to_s
      if (match = text.match(/(\d{4})[-\/年](\d{1,2})[-\/月](\d{1,2})/))
        year, month, day = match.captures.map(&:to_i)
        return safe_date(year, month, day)
      end

      if (match = text.match(/あと\s*(\d+)\s*日/))
        base_date = parse_fetched_on(fetched_on_text)
        return base_date ? base_date + match[1].to_i : nil
      end

      nil
    end

    def self.parse_fetched_on(fetched_on_text)
      match = fetched_on_text.to_s.match(/(\d{4})-(\d{2})-(\d{2})/)
      return nil unless match

      year, month, day = match.captures.map(&:to_i)
      safe_date(year, month, day)
    end

    def self.safe_date(year, month, day)
      Date.new(year, month, day)
    rescue ArgumentError
      nil
    end

    # 分類(C列)ごとにグルーピングし、分類内は新規行・既存行を区別せず統一キーで並べ直す
    # （毎回全行を並べ直す。ラウンド2 C10。優先順位はrow_priority_keyを参照）。
    def self.order_rows(brand_new_rows, surviving_existing_rows)
      all_rows = brand_new_rows + surviving_existing_rows
      categories = all_rows.map { |row| row[CATEGORY_COLUMN_INDEX] }.each_with_index.uniq { |category, _| category }
      categories = categories.sort_by { |category, index| [category_rank(category), index] }.map(&:first)

      new_row_object_ids = brand_new_rows.each_with_object({}) { |row, memo| memo[row.object_id] = true }
      existing_original_index = surviving_existing_rows.each_with_index.each_with_object({}) do |(row, index), memo|
        memo[row.object_id] = index
      end

      categories.flat_map do |category|
        all_rows.select { |row| row[CATEGORY_COLUMN_INDEX] == category }
                .sort_by { |row| row_priority_key(row, new_row_object_ids, existing_original_index) }
      end
    end

    def self.category_rank(category)
      index = CATEGORY_ORDER.index(category)
      index.nil? ? CATEGORY_ORDER.size : index
    end

    def self.star_count(recommend_value)
      recommend_value.to_s.count("🌟")
    end

    def self.deadline_sort_key(deadline_text)
      match = deadline_text.to_s.match(/(\d{4})-(\d{2})-(\d{2})/)
      return FAR_FUTURE_SENTINEL unless match

      year, month, day = match.captures.map(&:to_i)
      safe_date(year, month, day) || FAR_FUTURE_SENTINEL
    end

    # 全行(新規+既存)共通の並び優先度キー（昇順ソートで上位＝残す/先頭に出す対象になる）。
    # 優先順位: 🌟が多い順 → 新規行が既存行より先 → 締切が遠い順(不明は最後) →
    # 既存行同士は元の順（ラウンド2 C10）。80件/300件の上限で残す行の選定にも同じキーを使う。
    # new_row_object_ids: brand_new_rowsのobject_id集合。existing_original_index: 既存行のobject_id => 元の並び順index。
    def self.row_priority_key(row, new_row_object_ids, existing_original_index)
      deadline = deadline_sort_key(row[DEADLINE_TEXT_COLUMN_INDEX])
      deadline_rank = deadline == FAR_FUTURE_SENTINEL ? Float::INFINITY : -deadline.jd
      existing_rank = new_row_object_ids[row.object_id] ? 0 : 1
      original_index = existing_original_index[row.object_id] || 0
      [-star_count(row[0]), existing_rank, deadline_rank, original_index]
    end

    # 1回の実行で追加する新規行はMAX_NEW_ROWS_PER_RUN件まで（ラウンド2 C3）。
    # 超過分は優先順位（row_priority_key）が低いものから見送る。
    def self.cap_new_rows_per_run(brand_new_rows)
      return brand_new_rows if brand_new_rows.size <= MAX_NEW_ROWS_PER_RUN

      new_row_object_ids = brand_new_rows.each_with_object({}) { |row, memo| memo[row.object_id] = true }
      brand_new_rows.sort_by { |row| row_priority_key(row, new_row_object_ids, {}) }.first(MAX_NEW_ROWS_PER_RUN)
    end

    # 300行上限は新規行にのみ適用する（既存行は上限で落とさない）。
    # 超過分は新規行の中で優先度（row_priority_key）が低いものから落とす。
    def self.cap_rows(ordered_rows, brand_new_rows)
      return ordered_rows if ordered_rows.size <= MAX_TOTAL_ROWS

      overflow = ordered_rows.size - MAX_TOTAL_ROWS
      new_row_object_ids = brand_new_rows.each_with_object({}) { |row, memo| memo[row.object_id] = true }
      priority_sorted = brand_new_rows.sort_by { |row| row_priority_key(row, new_row_object_ids, {}) }
      rows_to_drop = priority_sorted.reverse.first(overflow)
      drop_object_ids = rows_to_drop.each_with_object({}) { |row, memo| memo[row.object_id] = true }

      ordered_rows.reject { |row| drop_object_ids[row.object_id] }
    end

    def self.renumber(rows)
      rows.each_with_index.map do |row, index|
        renumbered_row = row.dup
        renumbered_row[NUMBER_COLUMN_INDEX] = index + 1
        renumbered_row
      end
    end
  end
end
