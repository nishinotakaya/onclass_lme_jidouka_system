# frozen_string_literal: true

require "date"

module FreelanceJobs
  # 既存のスプレッドシート行と、今回取得した新規行をマージする純粋関数。
  # 16列の配列（[🌟, No., 分類, 案件名, 掲載サイト, 案件URL, 難易度, 内容, 必要スキル,
  #             報酬, 形式, 応募状況, 締切, 一言メモ, 取得日時, 追加日]）を対象にする。
  class SheetMerger
    COLUMN_COUNT = 16
    URL_COLUMN_INDEX = 5
    SITE_COLUMN_INDEX = 4
    CATEGORY_COLUMN_INDEX = 2
    DEADLINE_TEXT_COLUMN_INDEX = 12
    FETCHED_ON_COLUMN_INDEX = 14
    # AC-03: 追加日。既存行がいつシートに追加されたかを表す列で、AUTO_UPDATE_COLUMN_INDEXESには
    # 含めない（含めると既存行の追加日が今回の取得日で上書きされてしまうため）。
    ADDED_ON_COLUMN_INDEX = 15
    NUMBER_COLUMN_INDEX = 1

    # 既存行のうち自動更新して良い列（案件URL表記・報酬・形式・応募状況・締切・取得日時）。
    # A・C・D・G・H・I・N（🌟/分類/案件名/難易度/内容/必要スキル/メモ）は手入力を保持する。
    # AC-10: URL表記(F列)も対象に含める。マッチング自体は両側normalize_urlで行うため
    # キーの一致は崩れず、表記だけを今回取得した値（末尾スラッシュ付きの正しいURL等）へ揃えられる。
    AUTO_UPDATE_COLUMN_INDEXES = [URL_COLUMN_INDEX, 9, 10, 11, 12, 14].freeze

    CATEGORY_ORDER = ["HTML/CSS", "Excel・スプレッドシート"].freeze
    # AC-05: 300行→500行に引き上げ。
    MAX_TOTAL_ROWS = 500
    # 1回の実行で追加する新規行の合計上限（ラウンド2 C3）。サイト別の保証枠・品質枠への配分は
    # select_new_rowsで行う（AC-21）。超過分は今回は見送り、次回以降の実行で再度候補になれば追加され得る。
    MAX_NEW_ROWS_PER_RUN = 80
    # AC-21: 1サイトが持てる合計行数のソフト上限。新規行の受け入れ判定にだけ使う
    # （既存行はこの上限では落とさない。落とすと手入力で保持している情報が消えてしまうため）。
    # 稼働サイトは約27で500÷27≒18なので、40は「1サイトがシートを埋め尽くさない」ための
    # 余裕を持たせたソフト上限。レバテック91件のような既存の偏りは60日ルール・募集終了判定で
    # 自然に減っていく想定なので、既存行側で強制的に削る必要はない。
    # （稼働サイト数・レバテックの件数は2026-09時点の参考値。増減した場合は見直しを検討する）
    MAX_ROWS_PER_SITE = 40
    # AC-21: 1回の実行で各サイトに保証する新規行数。🌟の少ないサイトでも毎回この件数だけは
    # 必ずシートに載るようにし、🌟の多い1〜2サイトがMAX_NEW_ROWS_PER_RUNの枠を独占して
    # 他のサイトが何回実行しても1行も載らない、という状態を防ぐ。
    NEW_ROWS_FLOOR_PER_SITE = 3
    REMOVE_UNKNOWN_DEADLINE_AFTER_DAYS = 60
    FAR_FUTURE_SENTINEL = Date.new(9999, 12, 31)

    Result = Struct.new(:rows, :added, :updated, :removed, :total, keyword_init: true)

    # succeeded_sites: 今回取得に成功したサイト表示名の配列。
    # excluded_sites: 環境変数等で取得対象外にしたサイト表示名の配列（新規行は来ない）。
    # 取得に失敗したサイトの既存行は、更新も期限切れ削除もせずそのまま残す（A1）。
    # 対象外サイトの既存行は更新されない（新規行が来ないため）が、期限切れ削除（should_remove?）は適用する。
    # 取得日時（O列）はRowBuilderが構築時点で書き込み済みのため、ここでは受け取らない。
    # closed_urls: 今回募集終了と判明したURLの集合（省略時は[]で現行の挙動を変えない）。
    # 該当する既存行は更新も期限切れ削除も待たず最優先で削除し、新規行としても追加しない。
    def self.merge(existing_rows:, new_rows:, succeeded_sites:, today:, excluded_sites: [], category_order: CATEGORY_ORDER,
                    closed_urls: [])
      normalized_existing_rows = existing_rows.map { |row| normalize_row(row) }
      deduped_new_rows = dedupe_by_url(new_rows)
      new_rows_by_url = index_by_url(deduped_new_rows)
      normalized_closed_urls = closed_urls.each_with_object({}) do |url, memo|
        memo[FreelanceJobs::JobPosting.normalize_url(url)] = true
      end

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

        # closed_urls判定を最優先にする（重複URLの2件目以降判定・更新・取得失敗サイト保護・締切ルールより先）。
        # 同じURLの行がシート上に複数残っていても、募集終了と分かった行は全て削除する必要があるため、
        # 重複判定でスキップされてしまう前にここで判定する。
        # 募集終了と分かった行を新規行の値で更新してしまうと「募集終了」の情報が消えるため。
        if normalized_closed_urls[url]
          removed += 1
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

        site_name = row[SITE_COLUMN_INDEX]
        unless succeeded_sites.include?(site_name) || excluded_sites.include?(site_name)
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
        url = FreelanceJobs::JobPosting.normalize_url(row[URL_COLUMN_INDEX])
        existing_url_seen[url] || normalized_closed_urls[url]
      end
      brand_new_rows = select_new_rows(brand_new_rows_all, surviving_existing_rows)
      brand_new_object_ids = brand_new_rows.each_with_object({}) { |row, memo| memo[row.object_id] = true }

      ordered_rows = order_rows(brand_new_rows, surviving_existing_rows, category_order)
      renumbered_rows = renumber(ordered_rows)

      # addedは実際にシートへ残った新規行数にする（ラウンド2 C3）。500行上限は
      # select_new_rows内の予算計算で既に担保されているため、ここでの追加のcapは不要（AC-23）。
      added = ordered_rows.count { |row| brand_new_object_ids[row.object_id] }

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
    def self.order_rows(brand_new_rows, surviving_existing_rows, category_order)
      all_rows = brand_new_rows + surviving_existing_rows
      categories = all_rows.map { |row| row[CATEGORY_COLUMN_INDEX] }.each_with_index.uniq { |category, _| category }
      categories = categories.sort_by { |category, index| [category_rank(category, category_order), index] }.map(&:first)

      new_row_object_ids = brand_new_rows.each_with_object({}) { |row, memo| memo[row.object_id] = true }
      existing_original_index = surviving_existing_rows.each_with_index.each_with_object({}) do |(row, index), memo|
        memo[row.object_id] = index
      end

      categories.flat_map do |category|
        all_rows.select { |row| row[CATEGORY_COLUMN_INDEX] == category }
                .sort_by { |row| row_priority_key(row, new_row_object_ids, existing_original_index) }
      end
    end

    def self.category_rank(category, category_order)
      index = category_order.index(category)
      index.nil? ? category_order.size : index
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
    # 既存行同士は元の順（ラウンド2 C10） → 最後にURLで完全に順序を決める（同着の行が残っても
    # sort_byは安定ソートではないため、URLで一意化しないと並び・選定結果が実行ごとにブレるため）。
    # 新規行のサイト別グループ化・保証枠・品質枠の選定（select_new_rows, AC-21）にも同じキーを使う。
    # new_row_object_ids: brand_new_rowsのobject_id集合。existing_original_index: 既存行のobject_id => 元の並び順index。
    def self.row_priority_key(row, new_row_object_ids, existing_original_index)
      deadline = deadline_sort_key(row[DEADLINE_TEXT_COLUMN_INDEX])
      deadline_rank = deadline == FAR_FUTURE_SENTINEL ? Float::INFINITY : -deadline.jd
      existing_rank = new_row_object_ids[row.object_id] ? 0 : 1
      original_index = existing_original_index[row.object_id] || 0
      [-star_count(row[0]), existing_rank, deadline_rank, original_index, row[URL_COLUMN_INDEX].to_s]
    end

    # AC-21〜AC-23: 新規行をサイト別の保証枠(NEW_ROWS_FLOOR_PER_SITE)＋品質枠の2段階で選ぶ。
    # なぜ保証枠を既存行の少ないサイトから配るのか: シートに載っている行が少ない（＝これまで
    # 独占されてきた）サイトを先に処理することで、そのサイトが確実に最低件数だけ載るようにするため。
    # 既存行の多いサイトを先に処理すると、予算が保証枠の途中で尽きたときに後回しのサイトが
    # また0件になってしまう。
    # なぜMAX_ROWS_PER_SITE(40)は新規行にだけ効くのか: 既存行を上限で機械的に落とすと、
    # 手入力で保持している情報（🌟評価・分類・案件名など）が消えてしまうため。
    # 500行上限（MAX_TOTAL_ROWS）は予算計算（budget）だけで担保し、既存行は一切落とさない。
    def self.select_new_rows(brand_new_rows_all, surviving_existing_rows)
      budget = new_row_budget(surviving_existing_rows.size)
      return [] if budget <= 0

      new_row_object_ids = brand_new_rows_all.each_with_object({}) { |row, memo| memo[row.object_id] = true }
      surviving_existing_count_by_site = count_rows_by_site(surviving_existing_rows)
      candidate_rows_by_site = group_new_candidates_within_site_capacity(
        brand_new_rows_all, new_row_object_ids, surviving_existing_count_by_site
      )

      guaranteed_floor_rows, taken_count_by_site, remaining_budget = select_guaranteed_floor_rows(
        candidate_rows_by_site, surviving_existing_count_by_site, new_row_object_ids, budget
      )
      quality_fill_rows = select_quality_fill_rows(
        candidate_rows_by_site, taken_count_by_site, new_row_object_ids, remaining_budget
      )

      guaranteed_floor_rows + quality_fill_rows
    end

    # AC-21: 1回の実行で追加できる新規行数の上限。全体上限(MAX_TOTAL_ROWS)からはみ出さないよう
    # 生存既存行数を差し引く（既存行だけで500件を超えている場合は負になり、新規行は0件になる。
    # 既存行は絶対に落とさない方針のため）。
    def self.new_row_budget(surviving_existing_row_count)
      [MAX_NEW_ROWS_PER_RUN, MAX_TOTAL_ROWS - surviving_existing_row_count].min
    end

    def self.count_rows_by_site(rows)
      rows.each_with_object(Hash.new(0)) { |row, memo| memo[row[SITE_COLUMN_INDEX]] += 1 }
    end

    # AC-22: サイトごとに新規候補をrow_priority_key昇順（優先度が高い順）に並べ、
    # MAX_ROWS_PER_SITEから生存既存行数を引いた受け入れ可能数で切り詰める。
    def self.group_new_candidates_within_site_capacity(brand_new_rows_all, new_row_object_ids,
                                                         surviving_existing_count_by_site)
      grouped_by_site = brand_new_rows_all.group_by { |row| row[SITE_COLUMN_INDEX] }

      grouped_by_site.each_with_object({}) do |(site_name, candidate_rows), candidate_rows_by_site|
        sorted_candidate_rows = candidate_rows.sort_by { |row| row_priority_key(row, new_row_object_ids, {}) }
        acceptable_count = [MAX_ROWS_PER_SITE - surviving_existing_count_by_site[site_name], 0].max
        candidate_rows_by_site[site_name] = sorted_candidate_rows.first(acceptable_count)
      end
    end

    # AC-21: サイトを生存既存行数の少ない順に処理し、各サイトの上位候補から最大
    # NEW_ROWS_FLOOR_PER_SITE件を、予算が尽きるまで確保する。
    # 生存既存行数が同数のサイト同士は、そのサイトの先頭候補（最も優先度が高い候補）の
    # row_priority_keyが小さい方（＝優先度が高い方。row_priority_keyは昇順ソートで上位＝優先扱い）を
    # 先にする（sort_byの安定性に依存せず順序を決定的にするため）。
    def self.select_guaranteed_floor_rows(candidate_rows_by_site, surviving_existing_count_by_site,
                                           new_row_object_ids, budget)
      site_names_with_candidates = candidate_rows_by_site.keys.select { |site_name| candidate_rows_by_site[site_name].any? }
      ordered_site_names = site_names_with_candidates.sort_by do |site_name|
        top_candidate_priority_key = row_priority_key(candidate_rows_by_site[site_name].first, new_row_object_ids, {})
        [surviving_existing_count_by_site[site_name], top_candidate_priority_key, site_name]
      end

      remaining_budget = budget
      guaranteed_floor_rows = []
      taken_count_by_site = Hash.new(0)

      ordered_site_names.each do |site_name|
        break if remaining_budget <= 0

        candidate_rows = candidate_rows_by_site[site_name]
        rows_to_take = [NEW_ROWS_FLOOR_PER_SITE, candidate_rows.size, remaining_budget].min
        guaranteed_floor_rows.concat(candidate_rows.first(rows_to_take))
        taken_count_by_site[site_name] = rows_to_take
        remaining_budget -= rows_to_take
      end

      [guaranteed_floor_rows, taken_count_by_site, remaining_budget]
    end

    # AC-21・AC-27: 保証枠で取らなかった残り候補を全サイト混ぜてquality_fill_sort_key昇順に並べ直し、
    # 残予算ぶんを品質順（🌟が多い順→締切が遠い順）に取る。🌟数・締切が同着のときは
    # quality_fill_sort_key内のサイト内順位で各サイトから交互に取られる（AC-27）。
    def self.select_quality_fill_rows(candidate_rows_by_site, taken_count_by_site, new_row_object_ids, remaining_budget)
      return [] if remaining_budget <= 0

      leftover_candidates_with_rank = candidate_rows_by_site.flat_map do |site_name, candidate_rows|
        candidate_rows.drop(taken_count_by_site[site_name])
                      .each_with_index.map { |row, within_site_rank| [row, within_site_rank, site_name] }
      end

      leftover_candidates_with_rank
        .sort_by { |row, within_site_rank, site_name| quality_fill_sort_key(row, new_row_object_ids, within_site_rank, site_name) }
        .first(remaining_budget)
        .map { |row, _within_site_rank, _site_name| row }
    end

    # AC-27: 品質枠専用の並び優先度キー。row_priority_keyの先頭3要素（🌟数・新規既存・締切。
    # 末尾のURLタイブレークは含めない）までは通常通り優先度で決め、そこが完全に同着の場合だけ
    # within_site_rank（そのサイトの残り候補内での順位。0が最優先）で比較する。
    # 同着ならまずwithin_site_rank=0同士が並び、次いでsite_name順に0→1→…と巡回するため、
    # 結果として複数サイトの同着候補から1件ずつ交互に選ばれる（1サイトが残枠を独占するのを防ぐ）。
    # 末尾のURL（row[URL_COLUMN_INDEX]）は、within_site_rankがeach_with_indexでサイト内一意・
    # site_nameがgroup_byでグループごとに一意なため、(within_site_rank, site_name)の組だけで
    # 既に全候補が一意に区別でき、比較上は到達しない。それでも残しているのは、将来サイト内順位の
    # 付け方が変わってwithin_site_rankの一意性が崩れた場合でも、sort_byの不安定性（同着の並びが
    # 実行ごとにブレる問題）が表に出ないようにするための保険。
    def self.quality_fill_sort_key(row, new_row_object_ids, within_site_rank, site_name)
      row_priority_key(row, new_row_object_ids, {})[0..2] + [within_site_rank, site_name, row[URL_COLUMN_INDEX].to_s]
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
