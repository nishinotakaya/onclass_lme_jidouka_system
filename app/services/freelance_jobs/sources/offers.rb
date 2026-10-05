# frozen_string_literal: true

require "date"
require "nokogiri"

module FreelanceJobs
  module Sources
    # Offers: スキル別の副業向け一覧ページ（article のカード）をHTMLパースする。
    # 一覧カードだけで報酬・雇用形態・勤務地・スキルまで揃うため、詳細ページは取得しない。
    # 一覧に本文は無いため、description は「職種 / 勤務地 / 雇用形態」を連結して作る。
    #
    # robots.txt について（重要）: User-Agent: * に `Disallow: /jobs*?` があり、クエリ付きの
    # /jobs URL は禁止されている。そのため `?page=N` などのクエリは絶対に付けず、ページ送りもしない。
    # 副業ファセット `/jobs/skills/{スキルID}/side-job` は 2026-10-05 に HTTP 410 で廃止された。
    # 雇用形態で絞るパス型ファセットは無くなったため、全件一覧 `/jobs/skills/{スキルID}` と
    # リモート一覧 `/jobs/skills/{スキルID}/remote` の2ページ（各20件）を3スキル分取得し、
    # 雇用形態に「業務委託」を含むものだけ残す。正社員求人が大半なので絞り込みは必須。
    # 新着差分取りには十分なため、全件はたどらない。
    class Offers
      SITE_NAME = "Offers"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://offers.jp"

      # skill_id: 229=Ruby, 252=TypeScript, 261=React
      DEFAULT_SEARCH_TARGETS = [
        { skill_id: 229, hint: "Ruby" },
        { skill_id: 252, hint: "TypeScript" },
        { skill_id: 261, hint: "React" }
      ].freeze

      # クラス名は `JobWideCard-module__<ハッシュ>__container` のようにビルドごとにハッシュが変わるため、
      # 固定せず部分一致で拾う。以降の `__heading` 等も同じ理由で接尾辞の部分一致にしている。
      CARD_SELECTOR = 'article[class*="JobWideCard-module"]'
      HEADING_LINK_SELECTOR = 'a[class*="__heading"]'
      TITLE_SELECTOR = "h3"
      CLIENT_SELECTOR = 'a[class*="__company"] p'
      PROFESSION_SELECTOR = 'a[href^="/jobs/engineer/"]'
      SKILL_SELECTOR = 'a[href^="/jobs/skills/"]'
      # 勤務地li内には都道府県の入れ子liがあるため、外側の項目だけを `>` で読む。
      SUMMARY_ITEM_SELECTOR = 'ul[class*="__summaries"] > li'
      PREFECTURE_SELECTOR = 'ul[class*="Prefectures"] a'

      WORK_FORMAT_PREFIX = "雇用形態:"
      CLOSED_MARK = "募集停止"
      # 雇用形態にこの文言を含むカード（業務委託・業務委託から正社員）だけを fetch で残す。
      CONTRACT_WORK_MARK = "業務委託"

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, **_options)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
      end

      # 通信あり。スキルごとに全件一覧とリモート一覧の2ページを取得し、業務委託だけに絞ってURLキーで重複排除する。
      # 同じ案件が複数スキル・複数一覧に出るため重複排除は必須で、先に出たスキルのhintを残す。
      def fetch
        postings_by_url = {}

        @search_targets.each do |search_target|
          search_urls(search_target[:skill_id]).each do |url|
            body = @fetcher.get(url)
            postings = self.class.parse(body, today: @today, category_hint: search_target[:hint])
            postings.select { |posting| posting.work_format.include?(CONTRACT_WORK_MARK) }.each do |posting|
              postings_by_url[posting.url] ||= posting
            end
          end
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

      # カード1件をJobPostingに組み立てる。案件URLまたはタイトルが取れないカードは
      # 一覧カードではない（または構造が変わった）と判断して nil を返し、呼び出し側で除外する。
      def self.build_posting(card, category_hint)
        heading_link = card.at_css(HEADING_LINK_SELECTOR)
        href = heading_link && heading_link["href"].to_s.strip
        return nil if href.nil? || href.empty?

        title = normalize_text(heading_link.at_css(TITLE_SELECTOR)&.text)
        return nil if title.empty?

        summary_items = card.css(SUMMARY_ITEM_SELECTOR)
        professions = card.css(PROFESSION_SELECTOR).map { |link| normalize_text(link.text) }.reject(&:empty?)
        work_format = extract_work_format(summary_items)

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}#{href}"),
          title: title,
          description: build_description(professions, extract_prefectures(summary_items), work_format),
          category_hint: category_hint,
          reward: extract_reward(summary_items),
          work_format: work_format,
          # 「募集停止」はカード要素のtextだけで判定する（ページ全体には絞り込みUIの
          # 「募集停止を非表示」があり、全件が停止扱いになってしまう）。
          application_status: normalize_text(card.text).include?(CLOSED_MARK) ? FreelanceJobs::JobPosting::CLOSED_STATUS : "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: card.css(SKILL_SELECTOR).map { |link| normalize_text(link.text) }.reject(&:empty?),
          client: normalize_text(card.at_css(CLIENT_SELECTOR)&.text),
          tags: professions,
          posted_on: extract_posted_on(summary_items)
        )
      end

      # 報酬は summaries の1つ目（時給 / 月給 / 年収）。ASCIIの ~ は他ソースに合わせて 〜 にそろえる。
      def self.extract_reward(summary_items)
        normalize_text(summary_items.first&.text).tr("~", "〜")
      end

      # 「雇用形態: <!-- -->業務委託」はコメントノードが挟まるため、textから接頭辞を除いて整形する。
      def self.extract_work_format(summary_items)
        item = summary_items.find { |summary_item| normalize_text(summary_item.text).start_with?(WORK_FORMAT_PREFIX) }
        normalize_text(item&.text).delete_prefix(WORK_FORMAT_PREFIX).strip
      end

      def self.extract_prefectures(summary_items)
        summary_items.flat_map { |summary_item| summary_item.css(PREFECTURE_SELECTOR).map { |link| normalize_text(link.text) } }
                     .reject(&:empty?)
      end

      # 更新日は time[datetime="YYYY-MM-DD"]。解釈できない値は nil にする。
      def self.extract_posted_on(summary_items)
        datetime = summary_items.filter_map { |summary_item| summary_item.at_css("time[datetime]") }.first
        return nil unless datetime

        Date.iso8601(datetime["datetime"].to_s)
      rescue ArgumentError
        nil
      end

      # 一覧に本文が無いため、分類器が当たりを付けられるよう職種・勤務地・雇用形態を連結する。
      # 職種リンクが無いカードは「職種:」の部分ごと省く。
      def self.build_description(professions, prefectures, work_format)
        parts = []
        parts << "職種: #{professions.join('、')}" unless professions.empty?
        parts << "勤務地: #{prefectures.join('、')}" unless prefectures.empty?
        parts << "雇用形態: #{work_format}" unless work_format.empty?
        FreelanceJobs::JobPosting.normalize_description(parts.join(" / "))
      end

      def self.normalize_text(text)
        text.to_s.gsub(/\s+/, " ").strip
      end

      # 一覧URL（全件・リモート可の順）。robots.txt でクエリ付きが禁止のため、パスだけで組み立てる。
      def search_urls(skill_id)
        ["#{BASE_URL}/jobs/skills/#{skill_id}", "#{BASE_URL}/jobs/skills/#{skill_id}/remote"]
      end
      private :search_urls
    end
  end
end
