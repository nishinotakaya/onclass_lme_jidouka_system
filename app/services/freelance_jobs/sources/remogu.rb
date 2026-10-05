# frozen_string_literal: true

require "nokogiri"

module FreelanceJobs
  module Sources
    # Remogu: 技術タグ別の一覧ページ（dl.jobCard のカード）をHTMLパースする。
    # 一覧カードだけで報酬・働き方・業務内容・開発経験（技術タグ）まで揃うため、詳細ページは取得しない。
    #
    # robots.txt について: コメント行のみで、取得を制限する Disallow は無い。
    #
    # 一覧は技術タグのパス型URL（/T12/ など）。ページ送りは `?page=N` で、1ページ目はクエリを付けない。
    # 範囲外のページ番号は 404 を返し得るため、2ページ目以降の 404 はページ終端として打ち切る
    # （1ページ目の 404 は障害として例外を上げる）。カード0件のHTMLが返った場合も同様に打ち切る。
    class Remogu
      SITE_NAME = "Remogu"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://remogu.jp"

      # /T1/=Ruby on Rails, /T29/=TypeScript, /T12/=React
      DEFAULT_SEARCH_TARGETS = [
        { path: "/T1/", hint: "Ruby" },
        { path: "/T29/", hint: "TypeScript" },
        { path: "/T12/", hint: "React" }
      ].freeze

      # 1タグあたり2ページまで。新着差分取りには十分なため、全件をたどらない。
      DEFAULT_PAGES_PER_TARGET = 2

      CARD_SELECTOR = "dl.jobCard"
      TITLE_LINK_SELECTOR = "dt.jobTitle a"
      REWARD_SELECTOR = "li.reward"
      PLACE_SELECTOR = "li.place"
      WORK_STYLE_SELECTOR = "p.workStyle"
      # カード内の項目（職種・業務内容・求めるスキル・開発経験）。見出し dt の文言で読み分ける。
      OUTLINE_ITEM_SELECTOR = "dd.outline dl"

      PROFESSION_HEADING = "職種"
      DESCRIPTION_HEADING = "業務内容"
      REQUIRED_SKILL_HEADING = "求めるスキル"
      ENVIRONMENT_HEADING = "開発経験"
      NEW_MARK = "新着案件"

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS,
                     pages_per_target: DEFAULT_PAGES_PER_TARGET)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @pages_per_target = pages_per_target
      end

      # 通信あり。タグ×ページ数ぶん一覧ページを取得し、URLキーで重複排除する。
      # 同じ案件が複数タグに出るため重複排除は必須で、先に出たタグのhintを残す。
      def fetch
        postings_by_url = {}

        @search_targets.each do |search_target|
          collect_target_postings(search_target, postings_by_url)
        end

        postings_by_url.values
      end

      # タグ1つぶんのページ送り。カードが1件も取れないページに当たったらページ終端とみなして打ち切る。
      def collect_target_postings(search_target, postings_by_url)
        (1..@pages_per_target).each do |page_number|
          begin
            body = @fetcher.get(search_url(search_target[:path], page_number))
          rescue FreelanceJobs::FetchError => fetch_error
            break if end_of_pages?(fetch_error, page_number)

            raise
          end
          page_postings = self.class.parse(body, today: @today, category_hint: search_target[:hint])
          break if page_postings.empty?

          page_postings.each { |posting| postings_by_url[posting.url] ||= posting }
        end
      end
      private :collect_target_postings

      # 2ページ目以降の 404（"HTTP 404 <url>"）は範囲外ページ＝終端。1ページ目の 404 は本当の障害。
      def end_of_pages?(fetch_error, page_number)
        page_number > 1 && fetch_error.message.match?(/\AHTTP 404 /)
      end
      private :end_of_pages?

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

      # カード1件をJobPostingに組み立てる。案件URLまたはタイトルが取れないカードは
      # 一覧カードではない（または構造が変わった）と判断して nil を返し、呼び出し側で除外する。
      def self.build_posting(card, category_hint)
        title_link = card.at_css(TITLE_LINK_SELECTOR)
        href = title_link && title_link["href"].to_s.strip
        return nil if href.nil? || href.empty?

        title = normalize_text(title_link.text)
        return nil if title.empty?

        outline = read_outline(card)

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}#{href}"),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(outline[:description_text]),
          category_hint: category_hint,
          reward: extract_reward(card),
          work_format: text_or_default(card.at_css(WORK_STYLE_SELECTOR)),
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: outline[:skills],
          # li.place は勤務地であって発注者ではないため、client は取れない。勤務地は tags に入れる。
          client: nil,
          tags: build_tags(card, outline[:professions]),
          posted_on: nil
        )
      end

      # 「~ 1,300,000 円 ／月」→「〜1,300,000円／月」。ASCII の ~ は他ソースに合わせて 〜 にそろえ、空白は除く。
      def self.extract_reward(card)
        reward_element = card.at_css(REWARD_SELECTOR)
        return "要確認" unless reward_element

        reward = reward_element.text.gsub(/[[:space:]]+/, "").tr("~", "〜")
        reward.empty? ? "要確認" : reward
      end

      def self.text_or_default(element)
        text = normalize_text(element&.text)
        text.empty? ? "要確認" : text
      end

      # dd.outline 内の dl を見出し（dt）で読み分ける。見出しが無い項目は無視する。
      # 戻り値: { professions:, description_text:, skills: }
      def self.read_outline(card)
        professions = []
        description_parts = []
        skills = []

        card.css(OUTLINE_ITEM_SELECTOR).each do |item|
          heading = normalize_text(item.at_css("dt")&.text)
          case heading
          when PROFESSION_HEADING
            professions.concat(item.css("dd a").map { |link| normalize_text(link.text) }.reject(&:empty?))
          when DESCRIPTION_HEADING, REQUIRED_SKILL_HEADING
            description_parts << block_text(item.at_css("dd"))
          when ENVIRONMENT_HEADING
            skills.concat(item.css("ul li a").map { |link| normalize_text(link.text) }.reject(&:empty?))
          end
        end

        { professions: professions.uniq, description_text: description_parts.reject(&:empty?).join("\n"), skills: skills.uniq }
      end

      # 本文ブロックのテキスト。<br> を改行として読む。
      def self.block_text(element)
        return "" unless element

        element.css("br").each { |line_break| line_break.replace("\n") }
        element.text.strip
      end

      # tags: 職種 + 勤務地 + 新着（表示があれば）。
      def self.build_tags(card, professions)
        tags = professions.dup
        place = normalize_text(card.at_css(PLACE_SELECTOR)&.text)
        tags << place unless place.empty?
        tags << "新着" if normalize_text(card.text).include?(NEW_MARK)
        tags
      end

      def self.normalize_text(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      private_class_method :build_posting, :extract_reward, :text_or_default, :read_outline,
                           :block_text, :build_tags, :normalize_text

      # 一覧URL。1ページ目はpageパラメータを付けず、2ページ目以降は"?page=N"を付ける。
      def search_url(path, page_number)
        url = "#{BASE_URL}#{path}"
        page_number > 1 ? "#{url}?page=#{page_number}" : url
      end
      private :search_url
    end
  end
end
