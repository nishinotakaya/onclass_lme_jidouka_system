# frozen_string_literal: true

require "nokogiri"
require "cgi"
require "date"

module FreelanceJobs
  module Sources
    # ランサーズ: 検索結果HTML（SSR）をパースする。
    class Lancers
      SITE_NAME = "ランサーズ"
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://www.lancers.jp"

      FIXED_PATHS = [
        "/work/search/task/input?open=1&sort=started",
        "/work/search/task?open=1&sort=started"
      ].freeze

      # robots.txt配慮でキーワードは最小限に絞る（2026-09-04時点）
      KEYWORDS = ["Excel", "スプレッドシート", "HTML", "コーディング"].freeze

      def initialize(fetcher:, today:, fixed_paths: FIXED_PATHS, keywords: KEYWORDS)
        @fetcher = fetcher
        @today = today
        @fixed_paths = fixed_paths
        @keywords = keywords
      end

      # 通信あり。固定URL＋キーワード検索を巡回する。
      def fetch
        postings = {}

        (@fixed_paths.map { |path| "#{BASE_URL}#{path}" } + keyword_urls).each do |url|
          body = @fetcher.get(url)
          self.class.parse(body, today: @today).each { |posting| postings[posting.url] ||= posting }
        end

        postings.values
      end

      # 通信なし（テスト用）。検索結果1ページ分のHTML本文から案件一覧を作る。
      def self.parse(body, today:, source_url: nil)
        document = Nokogiri::HTML(body)
        postings = {}

        document.css("div.p-search-job-media").each do |card|
          posting = build_posting(card, today)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      def self.build_posting(card, today)
        title_link = card.at_css("a.p-search-job-media__title")
        return nil unless title_link

        job_id = title_link["href"].to_s[%r{/work/detail/(\d+)}, 1]
        return nil unless job_id

        work_format = normalize_work_format(card.at_css("span.c-badge__text")&.text&.strip)
        deadline_raw = extract_deadline_raw(card)
        deadline_on, deadline_text = deadline_from_text(deadline_raw, today)

        title_node = title_link.dup
        title_node.css("ul, li, span").remove
        description_raw = card.at_css(".js-job-show-description")&.text.to_s.slice(0, 600)
        client_name = card.at_css("a[href^='/client/']")&.text&.strip || ""

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}/work/detail/#{job_id}"),
          title: title_node.text.strip,
          description: FreelanceJobs::JobPosting.normalize_description(description_raw),
          category_hint: nil,
          reward: normalize_reward(card.at_css(".p-search-job-media__price")&.text),
          work_format: work_format,
          application_status: application_status_text(work_format, card),
          deadline_text: deadline_text,
          deadline_on: deadline_on,
          skills: [],
          client: client_name,
          tags: card.css("li.p-search-job-media__tag-list").map { |node| node.text.strip }.reject(&:empty?),
          posted_on: nil
        )
      end

      def self.extract_deadline_raw(card)
        card_text = card.text.gsub(/\s+/, " ").strip
        card_text[/(あと\s*\d+\s*日|残り\s*\d+\s*日|本日締切|締切[^ ]{0,12}|\d{4}年\d{1,2}月\d{1,2}日)/, 1]
      end

      def self.deadline_from_text(raw_text, today)
        return [nil, "-"] if raw_text.nil?

        if (match = raw_text.match(/(?:あと|残り)\s*(\d+)\s*日/))
          days = match[1].to_i
          deadline_on = today + days
          [deadline_on, "あと#{days}日（#{deadline_on.strftime("%Y-%m-%d")}）"]
        elsif raw_text == "本日締切"
          # 絶対日付をテキストに埋めない場合、翌日以降にSheetMerger.parse_deadline_onが
          # deadline_on を復元できずREMOVE_UNKNOWN_DEADLINE_AFTER_DAYS(60日)残ってしまう。
          # ココナラ・シュフティの「本日終了（YYYY-MM-DD）」に合わせて絶対日付を併記する（ラウンド2 C7）。
          [today, "本日締切（#{today.strftime("%Y-%m-%d")}）"]
        elsif (match = raw_text.match(/(\d{4})年(\d{1,2})月(\d{1,2})日/))
          year, month, day = match.captures.map(&:to_i)
          deadline_on = Date.new(year, month, day)
          [deadline_on, deadline_on.strftime("%Y-%m-%d")]
        else
          [nil, raw_text]
        end
      end

      # 案件価格欄の表示テキストを整形する（半角スペースを除去し ~ / を全角に揃える）。
      def self.normalize_reward(raw_text)
        text = raw_text.to_s.gsub(/[[:space:]]+/, "")
        return "要相談" if text.empty?

        text.tr("~", "〜").tr("/", "／")
      end

      # 形式バッジの表記をCrowdWorks側の語彙（固定報酬制／時間単価制）に揃える（ラウンド2 C6）。
      WORK_FORMAT_NORMALIZATION = {
        "固定報酬" => "固定報酬制",
        "時間単価" => "時間単価制"
      }.freeze

      def self.normalize_work_format(raw_text)
        text = raw_text.to_s.strip
        return "-" if text.empty?

        WORK_FORMAT_NORMALIZATION[text] || text
      end

      # 提案・当選者数欄はワークタイプごとに表記が違うため数値だけ拾って組み立てる。
      def self.application_status_text(work_format, card)
        proposals_text = card.css(".p-search-job-media__proposals").text.gsub(/\s+/, " ").strip
        numbers = proposals_text.scan(/\d+/)

        case work_format
        when "コンペ"
          numbers[0] ? "提案 #{numbers[0]}件" : "-"
        when "プロジェクト"
          numbers[1] ? "#{numbers[0]}/#{numbers[1]}人" : "-"
        when "タスク"
          numbers[1] ? "#{numbers[0]}/#{numbers[1]}件" : "-"
        else
          proposals_text.empty? ? "-" : proposals_text
        end
      end

      def keyword_urls
        @keywords.map { |keyword| "#{BASE_URL}/work/search?keyword=#{CGI.escape(keyword)}&open=1&sort=started" }
      end
      private :keyword_urls
    end
  end
end
