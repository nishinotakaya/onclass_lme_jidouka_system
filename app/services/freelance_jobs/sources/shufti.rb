# frozen_string_literal: true

require "json"
require "date"

module FreelanceJobs
  module Sources
    # シュフティ: ゲスト向けJSON API（ログイン不要）をパースする。
    class Shufti
      SITE_NAME = "シュフティ"
      REQUEST_INTERVAL = 1.5
      API_URL = "https://internal.shufti.jp/api/v1/guest/jobs"
      VIEW_BASE_URL = "https://app.shufti.jp/jobs/view"

      REQUEST_HEADERS = {
        "Accept" => "application/json",
        "Origin" => "https://app.shufti.jp",
        "Referer" => "https://app.shufti.jp/"
      }.freeze

      # 初心者歓迎/スキル不要/マニュアルあり/データ入力
      TAG_IDS = [1, 2, 5, 9].freeze
      MAX_PAGE = 2 # 20件/ページ

      def initialize(fetcher:, today:)
        @fetcher = fetcher
        @today = today
      end

      # 通信あり。タグごとに最大2ページ取得する。
      def fetch
        postings = {}

        TAG_IDS.each do |tag_id|
          (1..MAX_PAGE).each do |page|
            body = @fetcher.get(tag_page_url(tag_id, page), headers: REQUEST_HEADERS)
            self.class.parse(body, today: @today).each { |posting| postings[posting.url] ||= posting }
          end
        end

        postings.values
      end

      # 通信なし（テスト用）。APIレスポンス本文（JSON文字列）から案件一覧を作る。
      def self.parse(body, today:, source_url: nil)
        data = JSON.parse(body)
        postings = {}

        (data["data"] || []).each do |job|
          posting = build_posting(job, today)
          postings[posting.url] ||= posting
        end

        postings.values
      end

      def self.build_posting(job, today)
        attributes = job["attributes"] || {}
        deadline_on, deadline_text = deadline_from_period(attributes["period"], today)

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{VIEW_BASE_URL}/#{job["id"]}"),
          title: attributes["name"].to_s.strip,
          description: FreelanceJobs::JobPosting.normalize_description(attributes["name"]),
          category_hint: nil,
          reward: reward_text(attributes),
          work_format: work_format_for(attributes["type"]),
          application_status: "閲覧 #{attributes["view_count"]}",
          deadline_text: deadline_text,
          deadline_on: deadline_on,
          skills: [],
          client: attributes["client_name"].to_s,
          tags: tags_for(attributes),
          posted_on: nil
        )
      end

      def self.reward_text(attributes)
        price = FreelanceJobs.format_number(attributes["price"])
        unit_price = FreelanceJobs.format_number(attributes["unit_price"])
        hourly_wage = FreelanceJobs.format_number(attributes["job_hourly_wage"])
        "#{price}円（単価#{unit_price}円／想定時給#{hourly_wage}円）"
      end

      def self.work_format_for(type)
        case type
        when 1 then "プロジェクト"
        when 2 then "タスク"
        else "形式#{type}"
        end
      end

      def self.tags_for(attributes)
        tags = (attributes["job_tags"] || []).map { |tag| tag["name"] }
        tags += ["継続発注あり"] if attributes["continuous_order"]
        tags += ["PR"] if attributes["name"].to_s.start_with?("【PR】")
        tags
      end

      def self.deadline_from_period(period, today)
        return [nil, "-"] if period.nil? || period.strip.empty?

        if (match = period.match(/あと\s*(\d+)\s*日/))
          days = match[1].to_i
          deadline_on = today + days
          [deadline_on, "あと#{days}日（#{deadline_on.strftime("%Y-%m-%d")}）"]
        elsif period.match(/あと\s*\d+\s*時間/)
          [today, "#{period}（#{today.strftime("%Y-%m-%d")}）"]
        else
          [nil, period]
        end
      end

      def tag_page_url(tag_id, page)
        "#{API_URL}?page=#{page}&sort=start_date%7Cdesc&recruiting=all&continuous_order=all&job_tag_id=#{tag_id}"
      end
      private :tag_page_url
    end
  end
end
