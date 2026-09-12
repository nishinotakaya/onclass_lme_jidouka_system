# frozen_string_literal: true

require "nokogiri"
require "cgi"

module FreelanceJobs
  module Sources
    # レバテックフリーランス: キーワード検索結果ページ（article.projectCard）をHTMLパースする。
    # JSON埋め込みが無いため、単価・勤務地・契約形態はテキスト内容で判定して取り出す。
    class Levtech
      SITE_NAME = "レバテックフリーランス"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://freelance.levtech.jp"

      DEFAULT_SEARCH_TARGETS = [
        { keyword: "Ruby", hint: "Ruby" },
        { keyword: "TypeScript", hint: "TypeScript" },
        { keyword: "React", hint: "React" }
      ].freeze

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
      end

      # 通信あり。キーワードごとに検索結果ページを取得する。
      def fetch
        postings = {}

        @search_targets.each do |target|
          body = @fetcher.get(search_url(target[:keyword]))
          self.class.parse(body, today: @today, category_hint: target[:hint]).each do |posting|
            postings[posting.url] ||= posting
          end
        end

        postings.values
      end

      # 通信なし（テスト用）。検索結果1ページ分のHTML本文から案件一覧を作る。
      def self.parse(body, today:, category_hint: nil)
        document = Nokogiri::HTML(body)
        postings = {}

        document.css("article.projectCard").each do |card|
          posting = build_posting(card, category_hint)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      def self.build_posting(card, category_hint)
        href = job_href(card)
        return nil unless href

        title = card.at_css("h3.nameGroup span.name")&.text&.gsub(/\s+/, " ")&.strip || ""
        summary_items = card.css("ul.summaryList li.item").map { |item| item.text.strip }
        reward_text = summary_items.find { |item| item.include?("／時") || item.include?("／月") } || ""
        work_location = summary_items.find { |item| !item.include?("／時") && !item.include?("／月") && !item.include?("業務委託") } || ""

        definition_table = table_items(card)

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}#{href}"),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(build_description(definition_table, work_location)),
          category_hint: category_hint,
          reward: reward_text.empty? ? "要確認" : reward_text,
          work_format: work_format(reward_text),
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: split_skills(definition_table["開発環境"]),
          client: "",
          tags: build_tags(card),
          posted_on: nil
        )
      end

      def self.job_href(card)
        card.css("a").map { |link| link["href"] }.compact
            .find { |href| href.match?(%r{/project/detail/\d+}) }
      end

      # dl.tableItem を「dt.title => dd.data」のHashにする（開発環境／求めるスキル／募集職種）。
      def self.table_items(card)
        card.css("dl.tableItem").each_with_object({}) do |definition_list, table_items|
          label = definition_list.at_css("dt.title")&.text&.strip
          value = definition_list.at_css("dd.data")&.text&.gsub(/\s+/, " ")&.strip
          table_items[label] = value if label && value && !value.empty?
        end
      end

      def self.build_description(definition_table, work_location)
        parts = []
        parts << "募集職種: #{definition_table["募集職種"]}" if definition_table["募集職種"]
        parts << "開発環境: #{definition_table["開発環境"]}" if definition_table["開発環境"]
        parts << "求めるスキル: #{definition_table["求めるスキル"]}" if definition_table["求めるスキル"]
        parts << work_location unless work_location.empty?
        parts.join(" / ")
      end

      def self.split_skills(development_environment)
        return [] unless development_environment

        development_environment.split(%r{\s*/\s*}).reject(&:empty?)
      end

      def self.build_tags(card)
        tags = card.css("ul.featureList li.featureLabel").map { |label| label.text.strip }
        status_label = card.at_css("p.statusLabel")&.text&.strip
        status_label && !status_label.empty? ? [status_label, *tags] : tags
      end

      def self.work_format(reward_text)
        if reward_text.include?("／時")
          "時間単価制"
        elsif reward_text.include?("／月")
          "月額制（業務委託）"
        else
          "業務委託（フリーランス）"
        end
      end

      def search_url(keyword)
        "#{BASE_URL}/project/search/?keyword=#{CGI.escape(keyword)}"
      end
      private :search_url
    end
  end
end
