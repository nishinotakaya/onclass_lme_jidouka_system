# frozen_string_literal: true

require "date"
require "json"
require "nokogiri"

module FreelanceJobs
  module Sources
    # アットエンジニア: サイト自身が SSR（__NEXT_DATA__）で呼んでいる公開 JSON API から案件一覧を取る。
    # 認証不要で、一覧の1件だけで本文・必須/優遇スキル・報酬・働き方・スキルまで揃うため、詳細ページは取得しない。
    #
    # robots.txt について: at-engineer.jp は `Allow: /` のみ。API ホスト（api.at-engineer.jp）は
    # robots.txt が無い（404）。
    #
    # ページ送りは `?page=N`（10件/頁）。新着差分取りには十分なため最新 DEFAULT_MAX_PAGES 頁（50件）だけ取る。
    # projects が空のレスポンスで終端とみなす。JSON として解釈できない応答は JSON::ParserError を
    # そのまま上げ、ResearchService 側でサイト単位の失敗として扱わせる。
    class AtEngineer
      SITE_NAME = "アットエンジニア"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      API_BASE_URL = "https://api.at-engineer.jp"
      SITE_BASE_URL = "https://at-engineer.jp"

      DEFAULT_MAX_PAGES = 5

      JSON_HEADERS = { "Accept" => "application/json" }.freeze
      REMOTE_MARK = "リモート"

      def initialize(fetcher:, today:, max_pages: DEFAULT_MAX_PAGES, **_options)
        @fetcher = fetcher
        @today = today
        @max_pages = max_pages
      end

      # 通信あり。ページ順に取得し、URLキーで重複排除する（ページ送り中に新着が入ると同じ案件が再登場し得る）。
      def fetch
        postings_by_url = {}

        (1..@max_pages).each do |page_number|
          body = @fetcher.get(list_url(page_number), headers: JSON_HEADERS)
          page_postings = self.class.parse(body, today: @today)
          break if page_postings.empty?

          page_postings.each { |posting| postings_by_url[posting.url] ||= posting }
        end

        postings_by_url.values
      end

      # 通信なし（テスト用）。API の JSON 本文から案件一覧を作る。審査中（is_in_review）の案件は除く。
      def self.parse(body, today:, category_hint: nil)
        payload = JSON.parse(body)
        projects = payload.is_a?(Hash) ? Array(payload["projects"]) : []
        postings = {}

        projects.each do |project|
          next unless project.is_a?(Hash)
          next if project["is_in_review"] == true

          posting = build_posting(project, category_hint)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      # 案件1件をJobPostingに組み立てる。IDまたはタイトルが無いものは nil を返して除外する。
      def self.build_posting(project, category_hint)
        project_id = project["id"]
        title = normalize_text(project["title"])
        return nil if project_id.nil? || title.empty?

        characteristic_names = names_of(project["characteristics"])

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{SITE_BASE_URL}/projects/#{project_id}"),
          title: title,
          description: build_description(project),
          # 分類は EngineerClassifier が skills / description から行う。
          category_hint: category_hint,
          reward: name_or_default(project["reward"]),
          work_format: characteristic_names.find { |name| name.include?(REMOTE_MARK) } || "要確認",
          application_status: project["is_closed"] == true ? FreelanceJobs::JobPosting::CLOSED_STATUS : "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: (names_of(project["skills"]) + names_of(project["frameworks"])).uniq,
          client: nil,
          tags: build_tags(project, characteristic_names),
          posted_on: parse_date(project["created_at"])
        )
      end

      # 本文 + 必須 + 優遇。nil・空は除外する。
      def self.build_description(project)
        parts = [project["contents"], project["required"], project["preferred"]]
                .map { |part| part.to_s.strip }
                .reject(&:empty?)
        FreelanceJobs::JobPosting.normalize_description(parts.join("\n"))
      end

      # tags: 職種 + 契約期間 + 勤務地 + リモート以外の特徴。
      def self.build_tags(project, characteristic_names)
        other_characteristics = characteristic_names.reject { |name| name.include?(REMOTE_MARK) }
        tags = names_of(project["positions"]) +
               [name_of(project["term"]), name_of(project["location"])] +
               other_characteristics
        tags.reject(&:empty?).uniq
      end

      def self.names_of(list)
        Array(list).filter_map { |entry| entry["name"].to_s.strip if entry.is_a?(Hash) }.reject(&:empty?)
      end

      def self.name_of(entry)
        entry.is_a?(Hash) ? entry["name"].to_s.strip : ""
      end

      def self.name_or_default(entry)
        name = name_of(entry)
        name.empty? ? "要確認" : name
      end

      def self.parse_date(text)
        Date.iso8601(text.to_s)
      rescue ArgumentError
        nil
      end

      def self.normalize_text(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      private_class_method :build_posting, :build_description, :build_tags, :names_of, :name_of,
                           :name_or_default, :parse_date, :normalize_text

      def list_url(page_number)
        "#{API_BASE_URL}/projects/?page=#{page_number}"
      end
      private :list_url
    end
  end
end
