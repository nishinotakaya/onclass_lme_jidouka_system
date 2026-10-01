# frozen_string_literal: true

module FreelanceJobs
  # 「申込サイト一覧」タブの A 列に書く案件数の文言を組み立てる（通信なし）。
  # タブの B 列のサイト名は「ココナラ（公開依頼）」のように括弧の補足が付くことがあり、
  # バッチ側の SITE_NAME と表記が揃わないため、括弧部分を除いた名前で突き合わせる。
  module SiteListCounts
    HEADER_TEXT = "案件数（実測）\n掲載=シート行数／取得=一覧取得数\n（毎朝バッチが自動更新）"

    # 全角「（…）」と半角「(…)」の括弧部分を除去して前後の空白を落とす。
    def self.normalize_site_name(text)
      text.to_s.gsub(/（[^）]*）|\([^)]*\)/, "").strip
    end

    # 戻り値は { site_list_names の配列index => 文言 }（一致した行だけ、index昇順）。
    def self.build(site_list_names:, fetched_counts:, sheet_rows:, failed_sites:, excluded_sites:)
      fetched_count_by_name = fetched_counts.transform_keys { |site_name| normalize_site_name(site_name) }
      failed_names = failed_sites.map { |site_name| normalize_site_name(site_name) }
      excluded_names = excluded_sites.map { |site_name| normalize_site_name(site_name) }
      batch_site_names = fetched_count_by_name.keys | failed_names | excluded_names
      posted_count_by_name = sheet_rows.each_with_object(Hash.new(0)) do |row, memo|
        memo[normalize_site_name(row[FreelanceJobs::SheetMerger::SITE_COLUMN_INDEX])] += 1
      end

      site_list_names.each_with_index.each_with_object({}) do |(raw_name, index), texts_by_index|
        site_name = normalize_site_name(raw_name)
        next if site_name.empty? || !batch_site_names.include?(site_name)

        posted_count = posted_count_by_name[site_name]
        texts_by_index[index] =
          if excluded_names.include?(site_name)
            "除外中（掲載#{posted_count}件）"
          elsif failed_names.include?(site_name)
            "取得失敗（掲載#{posted_count}件）"
          else
            "掲載#{posted_count}件／取得#{fetched_count_by_name.fetch(site_name, 0)}件"
          end
      end
    end
  end
end
