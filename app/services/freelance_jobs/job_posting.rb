# frozen_string_literal: true

module FreelanceJobs
  # 各サイトから取得した1案件を表す値オブジェクト。
  JobPosting = Struct.new(
    :site,               # 表示名: "CrowdWorks" / "ランサーズ" / "ココナラ（公開依頼）" / "シュフティ" / "ママワークス" / "クラウディア"
    :url,                # 正規化済み案件URL
    :title,
    :description,        # 本文の要約元（normalize_description 済み）
    :category_hint,      # "HTML/CSS" / "Excel・スプレッドシート" / nil
    :reward,             # 表示用文字列 例 "10,000〜30,000円" / "時給 1,500〜2,000円" / "要相談"
    :work_format,        # "固定報酬制" / "時間単価制" / "タスク" / "プロジェクト" / "コンペ" / "公開依頼" / "業務委託（求人）" など
    :application_status, # 表示用 例 "応募 27件 / 契約 0/2人" / "提案 3件" / "応募者 2人" / "閲覧 220" / "-"
    :deadline_text,      # 表示用 例 "2026-09-17" / "あと7日（2026-09-11）"
    :deadline_on,        # Date or nil
    :skills,             # Array<String>
    :client,             # 発注者名 or ""
    :tags,               # Array<String> 例 ["初心者歓迎","マニュアルあり","継続発注あり","PR"]
    :posted_on,          # Date or nil
    keyword_init: true
  ) do
    # マージキー用のURL正規化。
    # scheme/hostを小文字化してhttpsに統一し、クエリ・フラグメント・末尾スラッシュを除去する。
    def self.normalize_url(url)
      text = url.to_s.strip
      return "" if text.empty?

      text = text.sub(%r{\Ahttps?://}i, "https://")
      text = text.split("?", 2).first
      text = text.split("#", 2).first
      text = text.sub(%r{/\z}, "")

      match = text.match(%r{\Ahttps://([^/]+)(.*)\z})
      return text unless match

      host, rest = match.captures
      "https://#{host.downcase}#{rest}"
    end

    DESCRIPTION_URL_RE = %r{https?://[^\s]+}i.freeze

    # 本文要約の共通整形。改行・連続空白を1スペースに畳み、URLを"[url]"に置換する。
    def self.normalize_description(text)
      return "" if text.nil?

      text.to_s.gsub(DESCRIPTION_URL_RE, "[url]").gsub(/[[:space:]]+/, " ").strip
    end
  end
end
