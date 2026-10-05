# frozen_string_literal: true

require "nokogiri"

module FreelanceJobs
  module Sources
    # mijicaフリーランス: スキル別の一覧ページ（SSR の HTML）をパースする。
    # 一覧カードだけでタイトル・単価・契約形態・スキルまで揃うため、詳細ページは取得しない。
    #
    # robots.txt について（重要）: `/api/*` `/enterprise/*` `/admin/*` などが Disallow。
    # そのため JSON API は使わず、許可範囲の一覧 `/jobs/skill-N` の SSR HTML だけを使う。
    #
    # ページ送りは無い（1頁30件。`?page=2` は同じ内容を返すため使わない）。スキルごとに1頁だけ取得する。
    #
    # URL について: カードのHTMLには href が無い。一方、同じHTMLの `window.__NUXT__` の中に
    # 案件ID（`id_by_enterprise_id:3451`）がカードと同じ順で並ぶ。この「出現順＝カード順」を前提に、
    # ID件数とカード件数が一致する場合だけ順に対応付ける。一致しなければ誤った案件にURLを付けないよう、
    # そのページは全件スキップして警告ログを出す。
    class MijicaFreelance
      SITE_NAME = "mijicaフリーランス"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://mijica-job.com"

      # skill-4=Ruby, skill-379=TypeScript, skill-175=React
      DEFAULT_SEARCH_TARGETS = [
        { path: "/jobs/skill-4", hint: "Ruby" },
        { path: "/jobs/skill-379", hint: "TypeScript" },
        { path: "/jobs/skill-175", hint: "React" }
      ].freeze

      # class の並び順に依存しないよう、見出しから祖先をたどってカードのルートを得る。
      TITLE_SELECTOR = "h3.fs-16"
      CARD_ROOT_SELECTOR = "div.cursor-pointer"
      SKILL_SELECTOR = 'a[href^="/jobs/skill-"] span'
      TAG_SELECTOR = "div.tag"
      REWARD_ICON_SELECTOR = 'img[alt="単価"]'
      CONTRACT_ICON_SELECTOR = 'img[alt="契約形態"]'
      SECTION_HEADING_SELECTOR = "p.fw-bold"
      SECTION_BODY_SELECTOR = "p.whitespace-prewrap"
      ENTERPRISE_ID_PATTERN = /id_by_enterprise_id:(\d+)/
      REMOTE_MARK = "リモートOK"
      NUMBER_PATTERN = /\A[\d,.]+\z/

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, **_options)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
      end

      # 通信あり。スキルごとに1ページだけ取得し、URLキーで重複排除する。
      # 同じ案件が複数スキルに出るため重複排除は必須で、先に出たスキルのhintを残す。
      def fetch
        postings_by_url = {}

        @search_targets.each do |search_target|
          body = @fetcher.get("#{BASE_URL}#{search_target[:path]}")
          self.class.parse(body, today: @today, category_hint: search_target[:hint]).each do |posting|
            postings_by_url[posting.url] ||= posting
          end
        end

        postings_by_url.values
      end

      # 通信なし（テスト用）。一覧HTML本文から案件一覧を作る。
      def self.parse(body, today:, category_hint: nil)
        document = Nokogiri::HTML(body)
        cards = document.css(TITLE_SELECTOR).filter_map { |heading| heading.ancestors(CARD_ROOT_SELECTOR).first }
        enterprise_ids = body.to_s.scan(ENTERPRISE_ID_PATTERN).flatten

        return [] if cards.empty?

        unless cards.size == enterprise_ids.size
          FreelanceJobs.logger.warn(
            "[#{SITE_NAME}] カード件数(#{cards.size})と案件ID件数(#{enterprise_ids.size})が一致しないため、このページを全件スキップします"
          )
          return []
        end

        postings = {}
        cards.zip(enterprise_ids).each do |card, enterprise_id|
          posting = build_posting(card, enterprise_id, category_hint)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      # カード1件をJobPostingに組み立てる。タイトルが取れないカードは nil を返して除外する。
      def self.build_posting(card, enterprise_id, category_hint)
        title = extract_title(card)
        return nil if title.empty?

        skills = card.css(SKILL_SELECTOR).map { |skill| normalize_text(skill.text) }.reject(&:empty?).uniq
        tag_texts = card.css(TAG_SELECTOR).map { |tag| normalize_text(tag.text) }.reject(&:empty?)
        contract = extract_contract(card)

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}/jobs/detail/#{enterprise_id}"),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(build_description(card, title, skills)),
          category_hint: category_hint,
          reward: extract_reward(card),
          work_format: tag_texts.include?(REMOTE_MARK) ? REMOTE_MARK : "要確認",
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: skills,
          client: nil,
          tags: ([contract] + tag_texts).reject { |tag| tag.nil? || tag.empty? }.uniq,
          posted_on: nil
        )
      end

      # 「見出し: 本文」（案件の内容 / 求めるスキル / 案件担当のコメント）を改行で連結する。
      # 見出しと本文の組が1つも取れないカードは title + skills の短文にフォールバックする。
      def self.build_description(card, title, skills)
        sections = card.css(SECTION_HEADING_SELECTOR).filter_map do |heading|
          body = heading.next_element
          next unless body&.matches?(SECTION_BODY_SELECTOR)

          body_text = normalize_text(body.text)
          "#{normalize_text(heading.text)}: #{body_text}" unless body_text.empty?
        end

        sections.empty? ? [title, *skills].join(" ") : sections.join("\n")
      end

      # 見出しは末尾の span（"のフリーランス求人・案件"）を除いたテキスト。元のDOMは壊さず複製して除く。
      def self.extract_title(card)
        heading = card.at_css(TITLE_SELECTOR)&.dup
        return "" unless heading

        heading.css("span").each(&:remove)
        normalize_text(heading.text)
      end

      # 単価アイコンの隣のブロックの末端 span を読む。数値2つなら「60〜70万円/月額」、1つなら「60万円/月額」。
      def self.extract_reward(card)
        block = card.at_css(REWARD_ICON_SELECTOR)&.next_element
        return "要確認" unless block

        leaf_texts = block.css("span").select { |span| span.element_children.empty? }
                          .map { |span| normalize_text(span.text) }.reject(&:empty?)
        numbers = leaf_texts.grep(NUMBER_PATTERN)
        unit = leaf_texts.reject { |text| text.match?(NUMBER_PATTERN) || text.match?(/\A[~〜]\z/) }.last.to_s
        return "要確認" if numbers.empty?

        "#{numbers.first(2).join('〜')}#{unit}"
      end

      # 契約形態アイコンの隣の span（"業務委託(フリーランス)"）。
      def self.extract_contract(card)
        text = normalize_text(card.at_css(CONTRACT_ICON_SELECTOR)&.next_element&.text)
        text.empty? ? nil : text
      end

      def self.normalize_text(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      private_class_method :build_posting, :build_description, :extract_title, :extract_reward, :extract_contract, :normalize_text
    end
  end
end
