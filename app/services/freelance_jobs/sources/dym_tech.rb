# frozen_string_literal: true

require "nokogiri"
require "json"
require "date"

module FreelanceJobs
  module Sources
    # DYMテック: 案件アーカイブ（/archives/project、2ページ目以降は /archives/project/page/N）を
    # HTMLパースし、カードごとに詳細ページを取得して補完する。WordPress製で、一覧に検索は無く最新順の
    # アーカイブだけがある。robots.txt は `Disallow:`（空）で全許可。1ページ10件・3ページ（最新30件）まで。
    #
    # 一覧カード（div.c-card-recruit）には月単価・勤務地・年代バッジしか無く、技術名が乏しい。
    # 分類器（EngineerClassifier）は skills / description の技術名を見るため、詳細ページから
    # スキルタグ・募集背景/求めるスキル/歓迎スキル・本文・稼働日数・掲載日を補う。
    #
    # 詳細取得は1件ごとに rescue し、失敗した案件は一覧情報だけの JobPosting にする。
    # 1件の通信失敗で残り29件ぶんの案件を捨てないため。ただしアクセス制限（AccessBlockedError）は
    # 取り直しても解消せず続行すると状況を悪化させるので、握りつぶさず送出する。
    class DymTech
      SITE_NAME = "DYMテック"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://dym-tech.jp"
      DEFAULT_MAX_PAGES = 3

      CARD_SELECTOR = "div.c-card-recruit"
      LINK_SELECTOR = "a.c-card-recruit__inner"
      TITLE_SELECTOR = "h2.c-card-recruit__title"
      SALARY_SELECTOR = ".c-card-recruit__salary span"
      AREA_SELECTOR = ".c-card-recruit__area"
      STATION_SELECTOR = ".c-card-recruit__station"
      BADGE_SELECTOR = ".c-card-recruit__badges .c-card-recruit__badge"

      SKILL_TAG_SELECTOR = ".p-project__tags .p-project__tag"
      SECTION_TITLE_SELECTOR = ".p-project__content-item-list-item-title"
      EDITOR_SELECTOR = ".p-project__editor"
      DATA_ITEM_SELECTOR = ".p-project__datas .p-project__data--item p"
      JSON_LD_SELECTOR = 'script[type="application/ld+json"]'
      JOB_POSTING_TYPE = "JobPosting"

      def initialize(fetcher:, today:, max_pages: DEFAULT_MAX_PAGES, **_options)
        @fetcher = fetcher
        @today = today
        @max_pages = max_pages
      end

      # 通信あり。一覧を最大 max_pages ページ取得してURLで重複排除し、案件ごとに詳細を1回だけ取得する。
      # カードが0件のページは終端とみなして打ち切る。
      def fetch
        list_postings_by_url = {}

        (1..@max_pages).each do |page_number|
          page_postings = self.class.parse(@fetcher.get(list_url(page_number)), today: @today)
          break if page_postings.empty?

          page_postings.each { |posting| list_postings_by_url[posting.url] ||= posting }
        end

        list_postings_by_url.values.map { |list_posting| enrich_with_detail(list_posting) }
      end

      # 通信なし（テスト用）。一覧1ページ分のHTMLから案件一覧を作る。
      # todayは全取得元共通のインターフェースで、締切の概念が無いサイトなので参照しない。
      def self.parse(body, today:, category_hint: nil)
        postings = {}

        Nokogiri::HTML(body).css(CARD_SELECTOR).each do |card|
          posting = build_posting(card, category_hint)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      # 通信なし。一覧由来の JobPosting に詳細HTMLの skills・本文・掲載日を足した新しい JobPosting を返す。
      # 詳細が想定外のHTMLでも、一覧の情報は失わない（補完は任意扱い）。
      def self.apply_detail(list_posting, detail_body)
        document = Nokogiri::HTML(detail_body)
        skills = document.css(SKILL_TAG_SELECTOR).map { |tag| squish(tag.text) }.reject(&:empty?)
        description_parts = [list_posting.description] + detail_description_parts(document)

        list_posting.dup.tap do |detailed_posting|
          detailed_posting.skills = skills
          detailed_posting.description = FreelanceJobs::JobPosting.normalize_description(description_parts.join(" / "))
          detailed_posting.posted_on = posted_on_from_json_ld(document)
        end
      end

      # リンクかタイトルが取れないカードは一覧カードではない（または構造が変わった）ので nil を返す。
      def self.build_posting(card, category_hint)
        href = card.at_css(LINK_SELECTOR)&.[]("href").to_s.strip
        return nil if href.empty?

        title = squish(card.at_css(TITLE_SELECTOR)&.text)
        return nil if title.empty?

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url(absolute_url(href)),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(location_text(card)),
          category_hint: category_hint,
          reward: reward(card),
          work_format: "業務委託（フリーランス）",
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: [],
          client: "",
          tags: card.css(BADGE_SELECTOR).map { |badge| squish(badge.text) }.reject(&:empty?),
          posted_on: nil
        )
      end

      def self.absolute_url(href)
        href.start_with?("http") ? href : "#{BASE_URL}#{href}"
      end

      # span は「90」のように万円単位の数字だけが入る。空なら金額不明として「要確認」にする。
      def self.reward(card)
        monthly_amount = squish(card.at_css(SALARY_SELECTOR)&.text)
        monthly_amount.empty? ? "要確認" : "#{monthly_amount}万円／月"
      end

      # 勤務地（都道府県＋最寄駅）。駅が空のカードもあるので、あるものだけ並べる。
      def self.location_text(card)
        area = squish(card.at_css(AREA_SELECTOR)&.text)
        station = squish(card.at_css(STATION_SELECTOR)&.text)
        location = [area, station].reject(&:empty?).join(" ")
        location.empty? ? "" : "勤務地: #{location}"
      end

      # 詳細ページの本文要素を「見出し: 本文」「本文」「稼働日数：週5日」の順に並べる。
      def self.detail_description_parts(document)
        section_parts = document.css(SECTION_TITLE_SELECTOR).map do |heading|
          content = squish(heading.next_element&.text)
          content.empty? ? nil : "#{squish(heading.text)}: #{content}"
        end
        editor_text = squish(document.at_css(EDITOR_SELECTOR)&.text)
        # 「職種： プロジェクトマネージャー」のように全角コロン後に空白が入るので詰める。
        data_items = document.css(DATA_ITEM_SELECTOR).map { |item| squish(item.text).sub(/：\s+/, "：") }

        (section_parts + [editor_text] + data_items).compact.reject(&:empty?)
      end

      # yoast の @graph 内 JobPosting の datePosted（"2026-09-09T14:23:43+09:00"）の日付部分。
      # WebPage の datePublished は UTC 換算で日付がずれるため使わない。
      # 壊れたJSON・想定外の形・日付形式違いはすべて nil（掲載日は任意扱い）。
      def self.posted_on_from_json_ld(document)
        document.css(JSON_LD_SELECTOR).each do |script_node|
          date_posted = job_posting_date(JSON.parse(script_node.text))
          return Date.strptime(date_posted[0, 10], "%Y-%m-%d") if date_posted
        rescue JSON::ParserError, ArgumentError
          next
        end
        nil
      end

      def self.job_posting_date(json_ld)
        nodes = json_ld.is_a?(Hash) && json_ld["@graph"].is_a?(Array) ? json_ld["@graph"] : [json_ld]
        job_posting = nodes.find { |node| node.is_a?(Hash) && node["@type"] == JOB_POSTING_TYPE }
        job_posting && job_posting["datePosted"].is_a?(String) ? job_posting["datePosted"] : nil
      end

      def self.squish(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      private

      # 詳細取得に失敗しても一覧情報だけの posting を返す。rescue は @fetcher.get の1回だけに絞り、
      # パース側のバグを覆い隠さない。
      def enrich_with_detail(list_posting)
        detail_body = fetch_detail_body(list_posting.url)
        detail_body ? self.class.apply_detail(list_posting, detail_body) : list_posting
      end

      def fetch_detail_body(url)
        @fetcher.get(url)
      rescue FreelanceJobs::AccessBlockedError
        raise
      rescue StandardError => error
        FreelanceJobs.logger.warn(
          "[FreelanceJobs::Sources::DymTech] #{error.message} 詳細を取得できないため一覧情報だけで続行します"
        )
        nil
      end

      def list_url(page_number)
        page_number == 1 ? "#{BASE_URL}/archives/project" : "#{BASE_URL}/archives/project/page/#{page_number}"
      end
    end
  end
end
