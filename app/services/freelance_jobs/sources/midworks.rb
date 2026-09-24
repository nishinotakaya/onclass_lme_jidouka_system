# frozen_string_literal: true

require "nokogiri"

module FreelanceJobs
  module Sources
    # Midworks: 技術ID別の一覧ページ（ul#projectIndexList 配下のカード）をHTMLパースする。
    # 完全SSRで、一覧カードだけで単価・勤務地・スキル・業務内容まで揃うため、詳細ページは取得しない。
    #
    # robots.txt について（重要）: Disallow は `/project/`（単数形）のみで、一覧・案件詳細が
    # 使う `/projects/...`（複数形）は対象外。単数形と複数形を混同して弾かないよう注意すること。
    #
    # ページングは `?page=N` のクエリ型で、1ページ目はクエリを付けない（ブラウザで開くURLと同じ形）。
    # 範囲外のページ番号はカード0件のHTMLが返る想定のため、ページ送りは
    # page_postings.empty? の打ち切りだけで安全に止まる。
    class Midworks
      SITE_NAME = "Midworks"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      # robots.txt に Crawl-delay の指定は無いため、他ソースと同じ既定値を使う。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://mid-works.com"

      DEFAULT_SEARCH_TARGETS = [
        { skill_id: 7, hint: "Ruby" },
        { skill_id: 45, hint: "TypeScript" },
        { skill_id: 78, hint: "React" }
      ].freeze

      # 1スラッグあたり2ページまでに絞る（新着差分取りには十分なため、全件をたどらない）。
      DEFAULT_PAGES_PER_SKILL = 2

      # 一覧カードのセレクタ。カード先頭には案件詳細への空のa要素（見出し外）が別途あるため、
      # カード自体はli直下のdivに限定して拾う。
      CARD_SELECTOR = "ul#projectIndexList > li > div.p-jobSummaryBoard"
      TITLE_HEADING_SELECTOR = "h2.p-jobSummaryBoard__title"
      # 案件詳細へのリンク。カード先頭の空のa要素・見出し内のa要素のどちらも同じhrefを指すため、
      # 先に見つかった方（カード先頭）のhrefをそのまま使う。
      # カード直下の子要素に限定しているのは、「開発環境」欄のスキルリンク
      # （/projects/skills/N）も"/projects/"始まりのhrefを持ち、単純な子孫セレクタでは
      # そちらを誤って拾ってしまうため。
      DETAIL_LINK_SELECTOR = '> a[href^="/projects/"]'
      SALARY_VALUE_SELECTOR = "p.p-jobSummaryBoard__salary b"
      NEW_LABEL_SELECTOR = "span.p-jobSummaryBoard__newLabel"
      # 勤務地はdt.-workplaceの次のdd（クラス無し）。dd.-workplaceは後述のスキル欄に
      # 同名クラスで使い回されているため、dt側のクラスから辿る必要がある。
      WORKPLACE_LABEL_SELECTOR = "dt.-workplace"
      SKILL_LIST_ITEM_SELECTOR = "dd.-workplace ul.-workplace__list__dev li a"
      BUSINESS_CONTENT_SELECTOR = "dd.p-jobSummaryBoard__descriptionListLastChild"

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, pages_per_skill: DEFAULT_PAGES_PER_SKILL)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @pages_per_skill = pages_per_skill
      end

      # 通信あり。技術ID×ページ数ぶん一覧ページを取得し、URLキーで重複排除する。
      # Ruby/TypeScript/Reactを横断する案件があり得るため、重複排除は必須。
      def fetch
        postings_by_url = {}

        @search_targets.each do |search_target|
          collect_skill_postings(search_target, postings_by_url)
        end

        postings_by_url.values
      end

      # 技術ID1つぶんのページ送り。取得した案件を postings_by_url に積む。
      # カードが1件も取れないページに当たったら、ページ終端とみなして打ち切る。
      def collect_skill_postings(search_target, postings_by_url)
        (1..@pages_per_skill).each do |page_number|
          body = @fetcher.get(search_url(search_target[:skill_id], page_number))
          page_postings = self.class.parse(body, today: @today, category_hint: search_target[:hint])
          break if page_postings.empty?

          page_postings.each { |posting| postings_by_url[posting.url] ||= posting }
        end
      end
      private :collect_skill_postings

      # 通信なし（テスト用）。一覧HTML本文から案件一覧を作る。
      def self.parse(body, today:, category_hint: nil)
        document = Nokogiri::HTML(body)
        postings = {}

        document.css(CARD_SELECTOR).each do |card|
          posting = build_posting(card, category_hint)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      # カード1件をJobPostingに組み立てる。案件詳細URLまたはタイトルが取れないカードは
      # 一覧カードではない（または構造が変わった）と判断して nil を返し、呼び出し側で除外する。
      def self.build_posting(card, category_hint)
        detail_link = card.at_css(DETAIL_LINK_SELECTOR)
        return nil unless detail_link

        href = detail_link["href"].to_s.strip
        return nil if href.empty?

        title_link = card.at_css("#{TITLE_HEADING_SELECTOR} a")
        return nil unless title_link

        title = title_link.text.gsub(/\s+/, " ").strip
        return nil if title.empty?

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}#{href}"),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(build_description(card)),
          category_hint: category_hint,
          reward: reward(card),
          work_format: "月額制（業務委託）",
          # 応募状況・締切・掲載日はサイト側に一覧として存在しないため固定値を入れる。
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: skill_names(card),
          client: "",
          tags: tag_names(card),
          posted_on: nil
        )
      end

      # 単価表示。「70万」「100万」のような2つの数値(万単位)を円に換算し、3桁区切りにする。
      # どちらか一方でも取れなければ「要確認」。
      def self.reward(card)
        values = card.css(SALARY_VALUE_SELECTOR)
        return "要確認" unless values.size == 2

        salary_min_yen = values[0].text.strip.to_i * 10_000
        salary_max_yen = values[1].text.strip.to_i * 10_000
        "#{FreelanceJobs.format_number(salary_min_yen)}〜#{FreelanceJobs.format_number(salary_max_yen)}円／月"
      end

      # NEWラベルの有無だけをtagsとして使う。
      def self.tag_names(card)
        card.at_css(NEW_LABEL_SELECTOR) ? ["NEW"] : []
      end

      # 「開発環境」欄のリンク（Ruby / TypeScript 等）をskillsとして使う。
      def self.skill_names(card)
        card.css(SKILL_LIST_ITEM_SELECTOR).map { |anchor| anchor.text.strip }.reject(&:empty?)
      end

      # 勤務地＋業務内容を連結する。勤務地は他サイトと同じくEngineerClassifierの判定材料にするため
      # descriptionに含める。
      def self.build_description(card)
        parts = [workplace_text(card), business_content_text(card)].compact.reject(&:empty?)
        parts.join(" / ")
      end

      # 勤務地。dt.-workplaceの次のdd（クラス無し）を読む。dd.-workplaceはスキル欄に
      # 別用途で使い回されているクラス名のため、dtから辿らないと取り違える。
      def self.workplace_text(card)
        label = card.at_css(WORKPLACE_LABEL_SELECTOR)
        return nil unless label

        value = label.next_element&.text&.strip
        value.nil? || value.empty? ? nil : value
      end

      def self.business_content_text(card)
        text = card.at_css(BUSINESS_CONTENT_SELECTOR)&.text&.strip
        text.nil? || text.empty? ? nil : text
      end

      # 一覧URL。1ページ目はpageパラメータを付けず、2ページ目以降は"?page=N"を付ける。
      def search_url(skill_id, page_number)
        url = "#{BASE_URL}/projects/skills/#{skill_id}"
        page_number > 1 ? "#{url}?page=#{page_number}" : url
      end
      private :search_url
    end
  end
end
