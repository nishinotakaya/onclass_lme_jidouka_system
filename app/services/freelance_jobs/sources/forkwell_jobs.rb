# frozen_string_literal: true

require "nokogiri"

module FreelanceJobs
  module Sources
    # Forkwell Jobs: 「業務委託（フリーランス）」の雇用形態一覧（/employment_types/freelance）を
    # HTMLパースする。完全SSRで、一覧カードだけで企業名・技術タグ・雇用形態・報酬まで揃うため、
    # 詳細ページは取得しない。
    #
    # robots.txt について（重要）: User-Agent: * の Disallow は `/entries`（応募）だけで、
    # 一覧はクロール対象。
    #
    # タグ別URL（/t/<tag>?employment_types=freelance）は使わない。実測でフィルタが効かず、
    # 正社員のみの案件まで全部返るため、フリーランス案件の絞り込みにならない。
    # 一方 /employment_types/freelance は雇用形態で絞られた一覧を返す。
    # この一覧には言語の絞り込みが無いので category_hint は nil とし、
    # title / tags（技術タグ）から分類器に Ruby・TypeScript・React を拾わせる。
    #
    # ページングは `?page=N` のクエリ型で、1ページ目はクエリを付けない（15件/頁、実測 全13件）。
    # 範囲外のページ番号はカード0件のHTMLが返る想定のため、ページ送りは
    # page_postings.empty? の打ち切りだけで安全に止まる。
    class ForkwellJobs
      SITE_NAME = "Forkwell Jobs"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      # robots.txt に Crawl-delay の指定は無いため、他ソースと同じ既定値を使う。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://jobs.forkwell.com"
      LIST_PATH = "/employment_types/freelance"

      # 実測で全13件・1頁15件のため、2ページ目まで取れば全件を網羅できる。
      DEFAULT_MAX_PAGES = 2

      # 案件カードは div.job-list 直下の div.card。カード内のサムネイルが div.card.mb-2 で、
      # 「保存された検索条件はありません」の枠も div.card のため、単に div.card を拾うと
      # 案件でないものが混ざる。直下の子に絞った上で、案件リンクを持たないものを build_posting で除外する。
      CARD_SELECTOR = "div.job-list > div.card"
      JOB_LINK_SELECTOR = "a.job-list__link"
      CLIENT_SELECTOR = ".avatar__detail"
      TAG_SELECTOR = "a.tag"
      SUMMARY_ITEM_SELECTOR = "ul.list-inline.space-bottom-0 > li.list-inline-item"
      REWARD_ITEM_SELECTOR = "ul.list-inline > li.list-inline-item"
      FEATURE_SELECTOR = ".feature-table__desc li"

      EMPLOYMENT_LABEL = "雇用形態"
      # 時給があれば時給を優先する（フリーランスの実態に近いため）。無ければ年収。
      REWARD_LABELS = %w[時給 年収].freeze
      # 一覧が「タグが多い場合の省略表示」に使う「…」リンクのテキスト。
      TRUNCATION_TAG_TEXT = "…"

      def initialize(fetcher:, today:, max_pages: DEFAULT_MAX_PAGES, **_options)
        @fetcher = fetcher
        @today = today
        @max_pages = max_pages
      end

      # 通信あり。ページ数ぶん一覧を取得し、URLキーで重複排除する。
      def fetch
        postings_by_url = {}

        (1..@max_pages).each do |page_number|
          body = @fetcher.get(list_url(page_number))
          page_postings = self.class.parse(body, today: @today)
          break if page_postings.empty?

          page_postings.each { |posting| postings_by_url[posting.url] ||= posting }
        end

        postings_by_url.values
      end

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

      # カード1件をJobPostingに組み立てる。案件リンクまたはタイトルが取れないカードは
      # 案件カードではない（または構造が変わった）と判断して nil を返す。
      def self.build_posting(card, category_hint)
        job_link = card.at_css(JOB_LINK_SELECTOR)
        return nil unless job_link

        href = job_link["href"].to_s.strip
        return nil if href.empty?

        title = job_link.at_css("span")&.text.to_s.gsub(/[[:space:]]+/, " ").strip
        return nil if title.empty?

        summary = summary_values(card)

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}#{href}"),
          title: title,
          description: build_description(summary, card),
          category_hint: category_hint,
          reward: reward_text(card),
          work_format: summary[:employment].to_s,
          # 応募状況・締切は一覧に無く、掲載日も「約1ヶ月前更新」の相対表記しか無いため固定値を入れる。
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: skill_names(card),
          client: card.at_css(CLIENT_SELECTOR)&.text.to_s.strip,
          tags: [],
          posted_on: nil
        )
      end

      # 雇用形態・職種・勤務地を取り出す。3つとも li.list-inline-item で、
      # ラベル（span.text-muted）の後ろにある li 直下のテキストノードが値になる。
      # 職種・勤務地はラベルがアイコン（fa-user / fa-map-marker-alt）なので、アイコンで見分ける。
      def self.summary_values(card)
        values = {}

        card.css(SUMMARY_ITEM_SELECTOR).each do |summary_item|
          value = direct_text(summary_item)
          next if value.empty?

          if summary_item.at_css("i.fa-user")
            values[:profession] = value
          elsif summary_item.at_css("i.fa-map-marker-alt")
            values[:location] = value
          elsif label_text(summary_item) == EMPLOYMENT_LABEL
            values[:employment] = value
          end
        end

        values
      end

      # description は title 以外の本文（雇用形態・職種・勤務地・特徴バッジ）を連結する。
      def self.build_description(summary, card)
        feature_texts = card.css(FEATURE_SELECTOR).map { |feature| feature.text.gsub(/[[:space:]]+/, " ").strip }
        parts = [summary[:employment], summary[:profession], summary[:location], *feature_texts]

        FreelanceJobs::JobPosting.normalize_description(parts.compact.reject(&:empty?).join(" "))
      end

      # 報酬。時給があれば時給、無ければ年収を "時給 3,000円 〜 6,000円" の形に整える。
      def self.reward_text(card)
        reward_items = card.css(REWARD_ITEM_SELECTOR).select { |item| REWARD_LABELS.include?(label_text(item)) }

        REWARD_LABELS.each do |reward_label|
          reward_item = reward_items.find { |item| label_text(item) == reward_label }
          next unless reward_item

          amount = reward_item.children.reject { |node| label_node?(node) }.map(&:text).join
          # 区切りの "&nbsp;〜&nbsp;" は通常の空白に畳む。
          return "#{reward_label} #{amount.gsub(/[[:space:]]+/, ' ').strip}"
        end

        "要確認"
      end

      # 技術タグ（小文字スラッグ。分類器は大小文字を区別しないためそのまま使う）。
      # 末尾の「…」は省略表示用のリンクで技術名ではないため除く。
      def self.skill_names(card)
        card.css(TAG_SELECTOR).map { |tag| tag.text.strip }.reject { |name| name.empty? || name == TRUNCATION_TAG_TEXT }
      end

      def self.label_node?(node)
        node.element? && node.name == "span" && node["class"].to_s.include?("text-muted")
      end

      def self.label_text(item)
        item.children.find { |node| label_node?(node) }&.text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      # li 直下のテキストノードだけを連結する（ラベル span やアイコンの中身を含めない）。
      def self.direct_text(item)
        item.children.select(&:text?).map(&:text).join.gsub(/[[:space:]]+/, " ").strip
      end

      private_class_method :build_posting, :summary_values, :build_description, :reward_text,
                           :skill_names, :label_node?, :label_text, :direct_text

      # 一覧URL。1ページ目はpageパラメータを付けず、2ページ目以降は"?page=N"を付ける。
      def list_url(page_number)
        url = "#{BASE_URL}#{LIST_PATH}"
        page_number > 1 ? "#{url}?page=#{page_number}" : url
      end
      private :list_url
    end
  end
end
