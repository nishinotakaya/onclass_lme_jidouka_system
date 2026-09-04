# frozen_string_literal: true

require "nokogiri"
require "json"
require "date"
require "time"

module FreelanceJobs
  module Sources
    # CrowdWorks: 一覧HTMLの #vue-container[data] に埋め込まれた検索結果JSONを読む。
    # HTTPのみでブラウザ不要。
    class Crowdworks
      SITE_NAME = "CrowdWorks"
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://crowdworks.jp"

      # category_id => { hint:, max_page: }
      CATEGORIES = {
        16  => { hint: "HTML/CSS", max_page: 2 },              # HTML・CSSコーディング
        17  => { hint: "HTML/CSS", max_page: 2 },              # LP
        14  => { hint: "HTML/CSS", max_page: 2 },              # ホームページ作成
        249 => { hint: "Excel・スプレッドシート", max_page: 3 }, # データ作成・入力
        52  => { hint: "Excel・スプレッドシート", max_page: 3 }, # データ入力
        146 => { hint: "Excel・スプレッドシート", max_page: 1 }, # 資料作成・マニュアル作成
        101 => { hint: "Excel・スプレッドシート", max_page: 1 }  # 文書作成
      }.freeze

      def initialize(fetcher:, today:)
        @fetcher = fetcher
        @today = today
      end

      # 通信あり。カテゴリごとに total_page を見ながら上限ページまで取得する。
      def fetch
        postings = {}

        CATEGORIES.each_key do |category_id|
          max_page = CATEGORIES[category_id][:max_page]
          total_page = 1
          page = 1

          while page <= max_page && page <= total_page
            body = @fetcher.get(category_page_url(category_id, page))
            search_result = self.class.extract_search_result(body)
            total_page = search_result.dig("page", "total_page") || 1

            self.class.job_offer_entries(search_result).each do |entry, is_pr|
              posting = self.class.build_posting(entry, is_pr)
              postings[posting.url] ||= posting
            end

            page += 1
          end
        end

        postings.values
      end

      # 通信なし（テスト用）。1ページ分のHTML本文から案件一覧を作る。
      def self.parse(body, today:, source_url: nil)
        search_result = extract_search_result(body)
        postings = {}

        job_offer_entries(search_result).each do |entry, is_pr|
          posting = build_posting(entry, is_pr)
          postings[posting.url] ||= posting
        end

        postings.values
      end

      def self.extract_search_result(body)
        document = Nokogiri::HTML(body)
        node = document.at_css("#vue-container")
        raise FreelanceJobs::FetchError, "CrowdWorks: #vue-container not found" unless node

        data = JSON.parse(node["data"].to_s)
        data["searchResult"] || {}
      end

      # job_offers に加え pr_gold/pr_platinum/pr_diamond も同じ形の配列として扱う。
      # 戻り値は [entry, is_pr] の配列。
      def self.job_offer_entries(search_result)
        entries = (search_result["job_offers"] || []).map { |entry| [entry, false] }
        %w[pr_gold pr_platinum pr_diamond].each do |pr_key|
          entries += (search_result[pr_key] || []).map { |entry| [entry, true] }
        end
        entries
      end

      def self.build_posting(entry, is_pr)
        job_offer = entry["job_offer"] || {}
        client = entry["client"] || {}
        reward, work_format = reward_and_work_format(entry["payment"] || {})

        posted_on = job_offer["last_released_at"] ? Time.parse(job_offer["last_released_at"]).to_date : nil
        deadline_on = job_offer["expired_on"] ? Date.strptime(job_offer["expired_on"], "%Y-%m-%d") : nil

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}/public/jobs/#{job_offer["id"]}"),
          title: job_offer["title"].to_s.strip,
          description: FreelanceJobs::JobPosting.normalize_description(job_offer["description_digest"]),
          category_hint: CATEGORIES.dig(job_offer["category_id"], :hint),
          reward: reward,
          work_format: work_format,
          application_status: application_status_text(entry["entry"] || {}),
          deadline_text: deadline_on ? deadline_on.strftime("%Y-%m-%d") : "-",
          deadline_on: deadline_on,
          skills: (job_offer["skills"] || []).map { |skill| skill["name"] },
          client: client["username"].to_s,
          tags: is_pr ? ["PR"] : [],
          posted_on: posted_on
        )
      end

      # payment キー => 形式表示名。生のキー名をそのままシートに出さないための対応表（ラウンド2 C6）。
      PAYMENT_WORK_FORMATS = {
        "fixed_price_payment" => "固定報酬制",
        "hourly_payment" => "時間単価制",
        "competition_payment" => "コンペ",
        "task_payment" => "タスク"
      }.freeze

      def self.reward_and_work_format(payment)
        if payment.key?("fixed_price_payment")
          budget = payment["fixed_price_payment"] || {}
          [format_range_reward(budget["min_budget"], budget["max_budget"]), PAYMENT_WORK_FORMATS["fixed_price_payment"]]
        elsif payment.key?("hourly_payment")
          wage = payment["hourly_payment"] || {}
          range_text = format_range_reward(wage["min_hourly_wage"], wage["max_hourly_wage"])
          reward = range_text == "要相談" ? range_text : "時給 #{range_text}"
          [reward, PAYMENT_WORK_FORMATS["hourly_payment"]]
        else
          other_key = payment.keys.first
          ["要相談", PAYMENT_WORK_FORMATS[other_key] || "その他"]
        end
      end

      def self.format_range_reward(min_value, max_value)
        min_text = FreelanceJobs.format_number(min_value)
        max_text = FreelanceJobs.format_number(max_value)

        if min_text.nil? && max_text.nil?
          "要相談"
        elsif min_text.nil?
          "〜#{max_text}円"
        elsif max_text.nil?
          "#{min_text}円〜"
        else
          "#{min_text}〜#{max_text}円"
        end
      end

      def self.application_status_text(application)
        if application.key?("project_entry")
          project_entry = application["project_entry"] || {}
          "応募 #{project_entry["num_application_conditions"]}件 / " \
            "契約 #{project_entry["num_contracts"]}/#{project_entry["project_contract_hope_number"]}人"
        else
          other_key = application.keys.first
          other_value = application[other_key]
          if other_value.is_a?(Hash)
            other_value.map { |key, value| "#{key} #{value}" }.join(" / ")
          else
            other_value.nil? ? "-" : other_value.to_s
          end
        end
      end

      def category_page_url(category_id, page)
        "#{BASE_URL}/public/jobs/category/#{category_id}?order=new&hide_expired=true&page=#{page}"
      end
      private :category_page_url
    end
  end
end
