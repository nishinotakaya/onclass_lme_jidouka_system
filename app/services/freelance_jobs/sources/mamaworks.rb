# frozen_string_literal: true

require "nokogiri"

module FreelanceJobs
  module Sources
    # ママワークス: robots.txtが `/jobs?` (クエリ付き検索) をDisallowしているため、
    # クエリ無しのカテゴリ一覧ページを取得し、タイトル・説明文をキーワードで絞り込む。
    class Mamaworks
      SITE_NAME = "ママワークス"
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://mamaworks.jp"

      CATEGORY_PATHS = ["/jobs/data-entry", "/jobs/engineering", "/jobs/creative", "/jobs/office-admin"].freeze

      KEYWORD_FILTER_RE = /HTML|CSS|コーディング|Web|ホームページ|Excel|エクセル|スプレッドシート|データ入力|事務|入力/i

      def initialize(fetcher:, today:)
        @fetcher = fetcher
        @today = today
      end

      # 通信あり。カテゴリ一覧を巡回する（各カテゴリ50件程度）。
      def fetch
        postings = {}

        CATEGORY_PATHS.each do |path|
          body = @fetcher.get("#{BASE_URL}#{path}")
          self.class.parse(body, today: @today).each { |posting| postings[posting.url] ||= posting }
        end

        postings.values
      end

      # 通信なし（テスト用）。カテゴリ一覧1ページ分のHTML本文から求人一覧を作る。
      # タイトル＋説明がKEYWORD_FILTER_REに一致するものだけ残す。
      def self.parse(body, today:, source_url: nil)
        document = Nokogiri::HTML(body)
        postings = {}

        document.css("li.p-recruit-index__result-box").each do |card|
          posting = build_posting(card)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      def self.build_posting(card)
        href = job_href(card)
        return nil unless href

        title = card.at_css("h2.p-recruit-index__result-ttl")&.text&.gsub(/\s+/, " ")&.strip || ""
        description_raw = card.at_css(".p-recruit-index__result-description")&.text
        return nil unless "#{title} #{description_raw}" =~ KEYWORD_FILTER_RE

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url(absolute_url(href)),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(description_raw),
          category_hint: nil,
          reward: extract_reward(card),
          work_format: "業務委託（求人）",
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: [],
          client: card.at_css(".p-recruit-index__result-name")&.text&.strip || "",
          tags: [],
          posted_on: nil
        )
      end

      def self.job_href(card)
        card.css("a").map { |link| link["href"] }.compact
            .find { |href| href.include?("mamaworks.jp/job/") || href.start_with?("/job/") }
      end

      def self.absolute_url(href)
        href.start_with?("/") ? "#{BASE_URL}#{href}" : href
      end

      # 報酬欄には専用クラスが無いため、詳細ボックス内で「報酬」を含む<p>を探す。
      def self.extract_reward(card)
        detail_box = card.at_css("section.p-recruit-index__result-detail-box")
        reward_paragraph = detail_box&.css("p")&.find { |paragraph| paragraph.text.include?("報酬") }
        return "求人ページ参照" unless reward_paragraph

        reward_paragraph.text.gsub(/[[:space:]]+/, " ").strip.sub(/\A報酬\s*[:：]\s*/, "")
      end
    end
  end
end
