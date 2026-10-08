# frozen_string_literal: true

require "nokogiri"

module FreelanceJobs
  module Sources
    # ランサーズエージェント（tech-agent.lancers.jp）: スキル絞り込みの一覧ページ（form.project-list__item のカード）を
    # HTMLパースする。一覧カードだけで報酬・スキル・勤務地・稼働日数・本文まで揃うため、詳細ページは取得しない。
    #
    # robots.txt について（2026-10-08 実測）: `Allow: /` のみで取得を制限していない。
    #
    # 一覧は /project?q_skill[]=<スキルID> の GET で絞り込める（2026-10-08 時点で Ruby 1,287 件）。
    # ただしページ送りは CSRF トークン付きの POST フォームでしか動かず、GET の `page=2` は
    # 1ページ目と同じ10件を返す。ページを辿っても重複するだけなので、各スキル1ページ（新着10件）だけ取る。
    # 毎朝のバッチで新着を拾う用途には、これで足りる。
    #
    # 北海道（q_area=3）は同日の実測で3件しかなく、全てフルリモートの WEB デザイン案件だったため、
    # 北海道プロファイルの取得元には入れない。
    class LancersAgent
      SITE_NAME = "ランサーズエージェント"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://tech-agent.lancers.jp"

      # skill_id は一覧の q_skill[] の値（5=Ruby、10=JavaScript）。
      # TypeScript / React の個別ファセットは無いため、JavaScript を hint なしで取り、分類器に任せる。
      DEFAULT_SEARCH_TARGETS = [
        { skill_id: 5, hint: "Ruby" },
        { skill_id: 10, hint: nil }
      ].freeze

      CARD_SELECTOR = "form.project-list__item"
      TITLE_SELECTOR = "h2.js__offerTitle"
      COMPENSATION_SELECTOR = "div.cp-projects-listItem__compensation"
      SKILL_SELECTOR = "li.item--language"
      DAYS_SELECTOR = "li.item--days"
      PLACE_SELECTOR = "li.item--place"
      DESCRIPTION_SELECTOR = "div.cp-projects-listItem__description__text"

      # 報酬の先頭に付く「週3日 |」（「週4日･5日 |」のような幅表記もある）。
      # 稼働日数は tags / description に回すので reward からは除く。
      DAYS_PREFIX_RE = /週[^|]*\|/
      WORK_FORMAT = "月額制（業務委託）"

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
      end

      # 通信あり。スキルごとに1ページずつ取得し、URLキーで重複排除する。
      # 同じ案件が複数スキルに出るため重複排除は必須で、先に出たスキルのhintを残す。
      def fetch
        postings_by_url = {}

        @search_targets.each do |search_target|
          body = @fetcher.get(list_url(search_target[:skill_id]))
          self.class.parse(body, today: @today, category_hint: search_target[:hint]).each do |posting|
            postings_by_url[posting.url] ||= posting
          end
        end

        postings_by_url.values
      end

      def list_url(skill_id)
        "#{BASE_URL}/project?q_skill%5B%5D=#{skill_id}"
      end
      private :list_url

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

      # カード1件をJobPostingに組み立てる。詳細パス（form の action）またはタイトルが取れないカードは
      # 一覧カードではない（または構造が変わった）と判断して nil を返し、呼び出し側で除外する。
      def self.build_posting(card, category_hint)
        action = card["action"].to_s.strip
        return nil if action.empty?

        title_element = card.at_css(TITLE_SELECTOR)
        title = extract_title(title_element)
        return nil if title.empty?

        days = normalize_text(card.at_css(DAYS_SELECTOR)&.text)

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url(absolute_url(action)),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(build_description(card, days)),
          category_hint: category_hint,
          reward: extract_reward(card),
          work_format: WORK_FORMAT,
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: extract_skills(card),
          client: nil,
          tags: build_tags(title_element, days),
          posted_on: nil
        )
      end

      def self.absolute_url(path)
        path.start_with?("http") ? path : "#{BASE_URL}#{path}"
      end

      # 見出しは「<small>【週5日/Rubyエンジニア】</small><br>本題」の形。small を除いた残りを title にする。
      def self.extract_title(title_element)
        return "" unless title_element

        title_copy = title_element.dup
        title_copy.css("small").each(&:remove)
        normalize_text(title_copy.text)
      end

      # small の文言（職種と稼働日数の見出し）は title から外した代わりに tags へ残し、稼働日数も tags に入れる。
      def self.build_tags(title_element, days)
        label = normalize_text(title_element&.at_css("small")&.text)
        [label, days].reject(&:empty?)
      end

      # 「週5日 | 530,000円〜 / 月」→「530,000円〜／月」。
      # 下限型（530,000円〜）と上限型（〜550,000円）が実HTMLに混在するため、〜の向きは加工せずそのまま保つ。
      def self.extract_reward(card)
        compensation = card.at_css(COMPENSATION_SELECTOR)&.text.to_s
        reward = compensation.gsub(DAYS_PREFIX_RE, "").gsub(/[[:space:]]+/, "").tr("/", "／")
        reward.include?("円") ? reward : "要確認"
      end

      # スキルは「Ruby・Rails」の ・ 区切り。区切りが崩れて空要素や改行入りの要素が出るため、strip して空を捨てる。
      def self.extract_skills(card)
        card.css(SKILL_SELECTOR).flat_map { |element| element.text.split("・") }
            .map { |skill| normalize_text(skill) }.reject(&:empty?).uniq
      end

      # 分類器の判定テキスト兼シート要約。本文 → 勤務地 → 稼働の順にラベル連結する。
      def self.build_description(card, days)
        parts = [card.at_css(DESCRIPTION_SELECTOR)&.text.to_s.strip]
        place = normalize_text(card.at_css(PLACE_SELECTOR)&.text)
        parts << "勤務地: #{place}" unless place.empty?
        parts << "稼働: #{days}" unless days.empty?
        parts.reject(&:empty?).join("\n")
      end

      def self.normalize_text(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      private_class_method :build_posting, :absolute_url, :extract_title, :build_tags, :extract_reward,
                           :extract_skills, :build_description, :normalize_text
    end
  end
end
