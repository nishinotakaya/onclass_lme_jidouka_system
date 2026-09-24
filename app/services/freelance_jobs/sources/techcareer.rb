# frozen_string_literal: true

require "nokogiri"

module FreelanceJobs
  module Sources
    # テクフリ: 技術スラッグ別の一覧ページ（div.pageResult.new-card のカード）をHTMLパースする。
    # 完全SSRで、一覧カードだけで単価・タグ・スキル・業務内容まで揃うため、詳細ページは取得しない。
    #
    # robots.txt について: Disallow は `/search` `/magazine_api/*` `/magazine_caches` のみで、
    # 一覧・案件詳細が使う `/projects/...` は対象外。
    #
    # ページングはパス型（1ページ目 `/projects/skills/<slug>/`、2ページ目以降
    # `/projects/skills/<slug>/p<N>/`）。範囲外のページ番号はカード0件のHTMLが返る想定のため、
    # ページ送りは page_postings.empty? の打ち切りだけで安全に止まる。
    #
    # 募集終了カードの扱いについて（重要）: h3.new-card__title 内に div.title-tag「募集終了」が
    # 出るカードも黙って除外せず、application_status に CLOSED_STATUS を入れて返す。実シートには
    # 掲載終了後も残り続ける行があり、募集終了カードを取得しないと新規取得結果に一致せず、
    # 60日ルールが効くまで古い行が残ってしまうため（AC-02、closed_urls経由で既存行を削除する）。
    class Techcareer
      SITE_NAME = "テクフリ"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      # robots.txt に Crawl-delay の指定は無いため、他ソースと同じ既定値を使う。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://freelance.techcareer.jp"

      DEFAULT_SEARCH_TARGETS = [
        { skill_slug: "ruby", hint: "Ruby" },
        { skill_slug: "typescript", hint: "TypeScript" },
        { skill_slug: "react", hint: "React" }
      ].freeze

      # 1スラッグあたり2ページまでに絞る（新着差分取りには十分なため、全件をたどらない）。
      DEFAULT_PAGES_PER_SKILL = 2

      CARD_SELECTOR = "div.pageResult.new-card"
      TITLE_SELECTOR = "h3.new-card__title .content-title .title-job"
      DETAIL_LINK_SELECTOR = 'a[href^="/projects/detail/"]'
      CLOSED_TAG_SELECTOR = "div.title-tag"
      TAG_SELECTOR = "a.tag-label"
      ITEM_SELECTOR = ".new-card-tbl__item"
      ITEM_ICON_SELECTOR = ".new-card-tbl__item__icon"
      ITEM_TEXT_SELECTOR = ".new-card-tbl__item__text"
      REWARD_AMOUNT_SELECTOR = ".text-amount"
      SKILL_ITEM_LABEL = "開発環境"
      SKILL_TEXT_SELECTOR = ".languages"

      # descriptionに載せる .new-card-tbl__item のラベル（この並び順がそのまま説明文の順序になる）。
      # ラベルは完全一致ではなく部分一致で探す（「想定年収」の実際のアイコン表記が
      # 「想定年収(税込)」で完全一致しないため）。
      DESCRIPTION_LABELS = ["職種", "契約形態", "想定年収", "業務内容", "必須スキル"].freeze

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, pages_per_skill: DEFAULT_PAGES_PER_SKILL)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @pages_per_skill = pages_per_skill
      end

      # 通信あり。技術スラッグ×ページ数ぶん一覧ページを取得し、URLキーで重複排除する。
      # Ruby/TypeScript/Reactを横断する案件があり得るため、重複排除は必須。
      def fetch
        postings_by_url = {}

        @search_targets.each do |search_target|
          collect_skill_postings(search_target, postings_by_url)
        end

        postings_by_url.values
      end

      # 技術スラッグ1つぶんのページ送り。取得した案件を postings_by_url に積む。
      # カードが1件も取れないページに当たったら、ページ終端とみなして打ち切る。
      def collect_skill_postings(search_target, postings_by_url)
        (1..@pages_per_skill).each do |page_number|
          body = @fetcher.get(search_url(search_target[:skill_slug], page_number))
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

      # カード1件をJobPostingに組み立てる。タイトルまたは詳細URLが取れないカードは
      # 一覧カードではない（または構造が変わった）と判断して nil を返し、呼び出し側で除外する。
      # 募集終了カードは除外せず、application_statusにCLOSED_STATUSを入れて返す（クラス冒頭コメント参照）。
      def self.build_posting(card, category_hint)
        title = squish(card.at_css(TITLE_SELECTOR)&.text)
        return nil if title.empty?

        detail_link = card.at_css(DETAIL_LINK_SELECTOR)
        return nil unless detail_link

        href = detail_link["href"].to_s.strip
        return nil if href.empty?

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}#{href}"),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(build_description(card)),
          category_hint: category_hint,
          reward: reward(card),
          work_format: work_format(card),
          application_status: application_status(card),
          # 締切・掲載日はサイト側に一覧として存在しないため固定値を入れる。
          deadline_text: "-",
          deadline_on: nil,
          skills: skill_names(card),
          client: "",
          tags: tag_names(card),
          posted_on: nil
        )
      end

      # div.title-tagのテキストがCLOSED_STATUSと一致するかどうかで判定する。リテラルの「募集終了」を
      # 直接書かないのは、AC-02: closed?判定・closed_urls経由の既存行削除がこの定数一致に依存するため。
      def self.application_status(card)
        closed_text = squish(card.at_css(CLOSED_TAG_SELECTOR)&.text)
        closed_text == FreelanceJobs::JobPosting::CLOSED_STATUS ? FreelanceJobs::JobPosting::CLOSED_STATUS : "募集中"
      end

      def self.tag_names(card)
        card.css(TAG_SELECTOR).map { |anchor| squish(anchor.text) }.reject(&:empty?)
      end

      # 単価。「単価(税込)」項目（.text-amountを含む項目）の全文をそのまま表示に使う。
      def self.reward(card)
        item = reward_item(card)
        return "要確認" unless item

        squish(item.at_css(ITEM_TEXT_SELECTOR)&.text)
      end

      # 業務形態。単価の単位文字列から判定する。他サイトの表記に揃える。
      def self.work_format(card)
        item = reward_item(card)
        text = item ? squish(item.at_css(ITEM_TEXT_SELECTOR)&.text) : ""

        return "月額制（業務委託）" if text.include?("/月") || text.include?("万円")
        return "時間単価制" if text.include?("/時") || text.include?("時給")

        "業務委託（フリーランス）"
      end

      # 「単価(税込)」項目を.text-amountの有無で探す（アイコンのラベル文字列に頼らない）。
      def self.reward_item(card)
        card.css(ITEM_SELECTOR).find { |item| item.at_css(REWARD_AMOUNT_SELECTOR) }
      end

      # 「開発環境」項目のリンク・スパン（.languages）をskillsとして使う。
      def self.skill_names(card)
        item = card.css(ITEM_SELECTOR).find { |candidate| squish(candidate.at_css(ITEM_ICON_SELECTOR)&.text) == SKILL_ITEM_LABEL }
        return [] unless item

        item.css(SKILL_TEXT_SELECTOR).map { |element| squish(element.text) }.reject(&:empty?)
      end

      # 「職種: … / 契約形態: … / 想定年収: … / 業務内容: … / 必須スキル: …」の形に連結する。
      # 欠落しうる項目ばかりなので、取れたものだけを並べる。
      def self.build_description(card)
        DESCRIPTION_LABELS.map { |label| description_part(card, label) }.compact.join(" / ")
      end

      def self.description_part(card, label)
        value = item_value_by_label(card, label)
        value && "#{label}: #{value}"
      end

      # ラベルは部分一致で探す（「想定年収」のアイコン表記が「想定年収(税込)」で完全一致しないため）。
      def self.item_value_by_label(card, label)
        item = card.css(ITEM_SELECTOR).find { |candidate| squish(candidate.at_css(ITEM_ICON_SELECTOR)&.text).include?(label) }
        return nil unless item

        value = squish(item.at_css(ITEM_TEXT_SELECTOR)&.text)
        value.empty? ? nil : value
      end

      def self.squish(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      # 一覧URL。1ページ目はページ番号を付けず、2ページ目以降はパス型"/p<N>/"を付ける。
      def search_url(skill_slug, page_number)
        url = "#{BASE_URL}/projects/skills/#{skill_slug}/"
        page_number > 1 ? "#{url}p#{page_number}/" : url
      end
      private :search_url
    end
  end
end
