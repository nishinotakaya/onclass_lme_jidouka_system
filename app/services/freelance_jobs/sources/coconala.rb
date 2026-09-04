# frozen_string_literal: true

require "nokogiri"
require "cgi"
require "date"

module FreelanceJobs
  module Sources
    # ココナラ 公開依頼（HTML・Nuxtでサーバーサイドレンダリングされたもの）をパースする。
    class Coconala
      SITE_NAME = "ココナラ（公開依頼）"
      REQUEST_INTERVAL = 5.0 # robots.txt配慮でリクエスト間隔を広めに取る
      BASE_URL = "https://coconala.com"

      KEYWORDS = ["HTML", "CSS", "コーディング", "Excel", "エクセル", "スプレッドシート", "データ入力"].freeze

      def initialize(fetcher:, today:)
        @fetcher = fetcher
        @today = today
      end

      # 通信あり。キーワードごとに1ページ目のみ取得する。
      def fetch
        postings = {}

        KEYWORDS.each do |keyword|
          url = "#{BASE_URL}/requests?keyword=#{CGI.escape(keyword)}&page=1"
          body = @fetcher.get(url)
          self.class.parse(body, today: @today).each { |posting| postings[posting.url] ||= posting }
        end

        postings.values
      end

      # 通信なし（テスト用）。検索結果1ページ分のHTML本文から公開依頼一覧を作る。
      # 「募集終了」のタイル、募集期限が today より前のものは除外する。
      def self.parse(body, today:, source_url: nil)
        document = Nokogiri::HTML(body)
        postings = {}

        document.css("div.c-searchItemWrapper").each do |tile|
          posting = build_posting(tile, today)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      def self.build_posting(tile, today)
        link = tile.at_css("a.c-searchItem_detailLink")
        return nil unless link

        request_id = link["href"].to_s[%r{\A/requests/(\d+)\z}, 1]
        return nil unless request_id

        tile_contents = extract_tile_contents(tile)
        remaining_text = tile_contents["募集期限"].to_s
        return nil if remaining_text.include?("募集終了")

        deadline_on, deadline_text = deadline_from_remaining_text(remaining_text, today)
        return nil if deadline_on && deadline_on < today

        description_raw = tile.at_css(".c-itemInfo_description")&.text

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}/requests/#{request_id}"),
          title: tile.at_css(".c-itemInfo_title")&.text&.gsub(/\s+/, " ")&.strip || "",
          description: FreelanceJobs::JobPosting.normalize_description(description_raw),
          category_hint: nil,
          reward: normalize_reward(tile_contents["予算"]),
          work_format: "公開依頼（提案制）",
          application_status: application_status_text(tile_contents["応募者数"]),
          deadline_text: deadline_text,
          deadline_on: deadline_on,
          skills: [],
          client: tile.at_css(".c-itemInfoUser_name")&.text&.strip || "",
          tags: [],
          posted_on: parse_posted_on(tile.at_css(".c-itemInfoUser_created span[title]")&.[]("title"))
        )
      end

      # .c-itemTile_tile を「予算」「応募者数」「募集期限」のラベルをキーにしたHashへ変換する。
      def self.extract_tile_contents(tile)
        tile.css(".c-itemTile_tile").each_with_object({}) do |sub_tile, tile_contents|
          caption = sub_tile.at_css(".c-itemTile_caption")&.text&.strip
          content = sub_tile.at_css(".c-itemTileContent")&.text&.gsub(/\s+/, " ")&.strip
          tile_contents[caption] = content if caption
        end
      end

      def self.deadline_from_remaining_text(text, today)
        if text.include?("本日終了")
          [today, "本日終了（#{today.strftime("%Y-%m-%d")}）"]
        elsif (match = text.match(/あと\s*(\d+)\s*日/))
          days = match[1].to_i
          deadline_on = today + days
          [deadline_on, "あと#{days}日（#{deadline_on.strftime("%Y-%m-%d")}）"]
        else
          [nil, text.strip.empty? ? "-" : text]
        end
      end

      def self.normalize_reward(text)
        return "要相談" if text.nil? || text.strip.empty?

        text.gsub(/[[:space:]]+/, "")
      end

      def self.application_status_text(count_text)
        return "-" if count_text.nil? || count_text.strip.empty?

        "応募者 #{count_text.strip}人"
      end

      def self.parse_posted_on(title_text)
        return nil if title_text.nil?

        match = title_text.match(/(\d{4})年(\d{1,2})月(\d{1,2})日/)
        return nil unless match

        year, month, day = match.captures.map(&:to_i)
        Date.new(year, month, day)
      end
    end
  end
end
