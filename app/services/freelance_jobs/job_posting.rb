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
  )

  # Struct.new(...) do ... end のブロックは定義位置（module FreelanceJobs）が
  # 定数の所属先になり、JobPosting::CLOSED_STATUSにはならない。
  # そのためStruct生成後にclassを開き直し、定数がJobPostingに属するようにする。
  class JobPosting
    # 応募状況(application_status)が募集終了を示す表示用文字列。
    CLOSED_STATUS = "募集終了"

    # application_statusが募集終了を示すかどうか（完全一致。部分一致・nilはfalse）。
    def closed?
      application_status == CLOSED_STATUS
    end

    # クエリでしか求人を区別できない掲載元のホストと、残す識別用クエリのキー。
    # 求人ボックス経由の転載求人（2026-10-08実測）でビズリーチ・マイナビエージェント・AIdea Careerの
    # 登録フォームURLは全求人が同じパスで、クエリを落とすと別求人が1行に潰れてしまうため、この3ホストだけ残す。
    IDENTIFYING_QUERY_KEY_BY_HOST = {
      "www.bizreach.jp" => "job_id",
      "mynavi-agent.jp" => "jno",
      "aidea-career.co.jp" => "rid"
    }.freeze

    # マージキー用のURL正規化。
    # scheme/hostを小文字化してhttpsに統一し、クエリ・フラグメント・末尾スラッシュを除去する。
    # ただしIDENTIFYING_QUERY_KEY_BY_HOSTのホストだけは、識別用クエリ1つを "?<key>=<value>" として残す。
    def self.normalize_url(url)
      text = url.to_s.strip
      return "" if text.empty?

      text = text.sub(%r{\Ahttps?://}i, "https://")
      text, query = text.split("?", 2)
      text = text.split("#", 2).first
      text = text.sub(%r{/\z}, "")

      match = text.match(%r{\Ahttps://([^/]+)(.*)\z})
      return text unless match

      host, rest = match.captures
      host = host.downcase
      "https://#{host}#{rest}#{identifying_query(host, query)}"
    end

    # 識別用クエリ（"?job_id=123"）。対象ホストでない・該当キーが無い場合は空文字。
    def self.identifying_query(host, query)
      key = IDENTIFYING_QUERY_KEY_BY_HOST[host]
      return "" unless key && query

      value = query.split("#", 2).first.split("&").filter_map do |pair|
        pair_key, pair_value = pair.split("=", 2)
        pair_value if pair_key == key
      end.first
      value.nil? || value.empty? ? "" : "?#{key}=#{value}"
    end
    private_class_method :identifying_query

    DESCRIPTION_URL_RE = %r{https?://[^\s]+}i.freeze

    # 本文要約の共通整形。改行・連続空白を1スペースに畳み、URLを"[url]"に置換する。
    def self.normalize_description(text)
      return "" if text.nil?

      text.to_s.gsub(DESCRIPTION_URL_RE, "[url]").gsub(/[[:space:]]+/, " ").strip
    end
  end
end
