# frozen_string_literal: true

require "nokogiri"
require "date"

module FreelanceJobs
  module Sources
    # クラウディア: 募集中一覧（s_accepting=1）を1回だけ取得する。
    # キーワード絞り込みパラメータは効かないため使わない。
    class Craudia
      SITE_NAME = "クラウディア"
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://www.craudia.com"
      LIST_URL = "#{BASE_URL}/work_list?s_accepting=1"

      def initialize(fetcher:, today:)
        @fetcher = fetcher
        @today = today
      end

      # 通信あり。募集中一覧を1回取得する。
      def fetch
        body = @fetcher.get(LIST_URL)
        self.class.parse(body, today: @today)
      end

      # 通信なし（テスト用）。一覧HTML本文から案件一覧を作る。
      def self.parse(body, today:, source_url: nil)
        document = Nokogiri::HTML(body)
        postings = {}

        document.css(".work-list__item-inner").each do |item|
          posting = build_posting(item, today)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      def self.build_posting(item, today)
        title_link = item.at_css("a.work-list__title")
        return nil unless title_link

        href = title_link["href"].to_s.strip
        return nil if href.empty?

        status_texts = item.css(".work-list__status > div").map { |node| node.text.gsub(/\s+/, " ").strip }
        deadline_status = status_texts.find { |text| text.include?("募集期間") }
        deadline_on, deadline_text = deadline_from_status(deadline_status, today)

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}#{href}"),
          title: title_link.text.gsub(/\s+/, " ").strip,
          description: "",
          category_hint: nil,
          reward: normalize_reward(item.at_css(".work-list__reward")&.text),
          work_format: item.at_css(".work-list__work-type")&.text&.strip || "-",
          application_status: application_status_text(status_texts),
          deadline_text: deadline_text,
          deadline_on: deadline_on,
          skills: [],
          client: "",
          tags: [],
          posted_on: nil
        )
      end

      def self.normalize_reward(text)
        return "要相談" if text.nil? || text.strip.empty?

        text.gsub(/[[:space:]]+/, "")
      end

      def self.application_status_text(status_texts)
        found = status_texts.find { |text| text.include?("参加申請数") || text.include?("作業数") }
        found ? found.gsub(/\s+/, "") : "-"
      end

      # 「募集期間 あと N 日」を日付化する。「募集終了」等の非数値は日付化せず原文を残す。
      def self.deadline_from_status(text, today)
        return [nil, "-"] if text.nil?

        if (match = text.match(/あと\s*(\d+)\s*日/))
          days = match[1].to_i
          deadline_on = today + days
          [deadline_on, "あと#{days}日（#{deadline_on.strftime("%Y-%m-%d")}）"]
        else
          [nil, text]
        end
      end
    end
  end
end
