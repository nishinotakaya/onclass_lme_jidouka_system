# frozen_string_literal: true

require "nokogiri"

module FreelanceJobs
  module Sources
    # テックリーチ（tech-reach.jp）: スキル別の一覧ページ（section.m-result のカード）をHTMLパースする。
    # 一覧カードだけで単価・スキル・勤務地・契約形態・職務内容まで揃うため、詳細ページは取得しない。
    #
    # robots.txt について（2026-10-08 実測）: 個別ボット（ClaudeBot・Amazonbot 等）の Disallow のみで、
    # `User-agent: *` の制限は無い。HttpFetcher のブラウザ UA で取得できる。
    #
    # 一覧は /jobs/s-<スキルslug>（2026-10-08 時点で Ruby 1,047 件、全体 14,892 件）。1ページ50件で、
    # ページ送りは `?page=N`（1ページ目はクエリを付けない）。新着差分取りには先頭2ページ（100件）で足りるため、
    # 全件はたどらない。範囲外のページ番号は 404 を返し得るので、2ページ目以降の 404 はページ終端として打ち切る
    # （1ページ目の 404 は障害として例外を上げる）。カード0件のHTMLが返った場合も同様に打ち切る。
    class TechReach
      SITE_NAME = "テックリーチ"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://tech-reach.jp"

      DEFAULT_SEARCH_TARGETS = [
        { skill_slug: "ruby", hint: "Ruby" },
        { skill_slug: "typescript", hint: "TypeScript" },
        { skill_slug: "react", hint: "React" }
      ].freeze

      # 1スキルあたり2ページ（50件×2）まで。
      MAX_PAGES = 2

      CARD_SELECTOR = "section.m-result"
      TITLE_SELECTOR = "h2.m-result__ttl"
      PRICE_SELECTOR = "em.oc-price"
      SKILL_SELECTOR = "ul.oc-skills li a"
      LOCATION_SELECTOR = "dd.oc-loc"
      EMPLOYMENT_SELECTOR = "dd.oc-emp_st"
      POSITION_SELECTOR = "dd.oc-pos"
      DESCRIPTION_SELECTOR = "dd.oc-desc"
      # 詳細リンクは PC用とSP用で同じカード内に2回出る。URLキーで重複排除する。
      DETAIL_LINK_SELECTOR = "a.m-result__btn"
      TAG_SELECTOR = "ul.m-result-tags li"

      # 単価は「65万 ~ 70万」形式。片側だけ・空はレンジとして使えないため「要確認」にする。
      # チルダは ASCII の ~ のほか全角の 〜 ～ も受け付ける。
      PRICE_RANGE_RE = /\A(\d[\d,.]*万)[~〜～](\d[\d,.]*万)\z/
      # タイトル末尾に付くSEO定型句。末尾一致のときだけ削る。
      TITLE_SEO_SUFFIX_RE = /の案件・求人\z/

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, max_pages: MAX_PAGES)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @max_pages = max_pages
      end

      # 通信あり。スキル×ページ数ぶん一覧ページを取得し、URLキーで重複排除する。
      # 同じ案件が複数スキルに出るため重複排除は必須で、先に出たスキルのhintを残す。
      def fetch
        postings_by_url = {}

        @search_targets.each do |search_target|
          collect_target_postings(search_target, postings_by_url)
        end

        postings_by_url.values
      end

      # スキル1つぶんのページ送り。カードが1件も取れないページに当たったらページ終端とみなして打ち切る。
      def collect_target_postings(search_target, postings_by_url)
        (1..@max_pages).each do |page_number|
          begin
            body = @fetcher.get(list_url(search_target[:skill_slug], page_number))
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

      # 一覧URL。1ページ目はpageパラメータを付けず、2ページ目以降は"?page=N"を付ける。
      def list_url(skill_slug, page_number)
        url = "#{BASE_URL}/jobs/s-#{skill_slug}"
        page_number > 1 ? "#{url}?page=#{page_number}" : url
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

      # カード1件をJobPostingに組み立てる。詳細URLまたはタイトルが取れないカードは
      # 一覧カードではない（または構造が変わった）と判断して nil を返し、呼び出し側で除外する。
      def self.build_posting(card, category_hint)
        href = card.at_css(DETAIL_LINK_SELECTOR)&.[]("href").to_s.strip
        return nil if href.empty?

        title = normalize_text(card.at_css(TITLE_SELECTOR)&.text).sub(TITLE_SEO_SUFFIX_RE, "")
        return nil if title.empty?

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url(absolute_url(href)),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(build_description(card)),
          category_hint: category_hint,
          reward: extract_reward(card),
          work_format: text_or_default(card.at_css(EMPLOYMENT_SELECTOR)),
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: card.css(SKILL_SELECTOR).map { |link| normalize_text(link.text) }.reject(&:empty?).uniq,
          client: nil,
          tags: card.css(TAG_SELECTOR).map { |tag| normalize_text(tag.text) }.reject(&:empty?),
          posted_on: nil
        )
      end

      def self.absolute_url(href)
        href.start_with?("http") ? href : "#{BASE_URL}#{href}"
      end

      # 「65万 ~ 70万」→「65万〜70万円／月」。チルダ（~ 〜 ～）は他ソースに合わせて 〜 にそろえる。
      def self.extract_reward(card)
        price = card.at_css(PRICE_SELECTOR)&.text.to_s.gsub(/[[:space:]]+/, "")
        matched = PRICE_RANGE_RE.match(price)
        return "要確認" unless matched

        "#{matched[1]}〜#{matched[2]}円／月"
      end

      # 分類器の判定テキスト兼シート要約。職務内容 → 募集職種 → 勤務地 → 契約形態の順にラベル連結する。
      # 募集職種は無いカードがあるため、取れたときだけ入れる。
      def self.build_description(card)
        parts = [block_text(card.at_css(DESCRIPTION_SELECTOR))]
        { "募集職種" => POSITION_SELECTOR, "勤務地" => LOCATION_SELECTOR, "契約形態" => EMPLOYMENT_SELECTOR }.each do |label, selector|
          value = normalize_text(card.at_css(selector)&.text)
          parts << "#{label}: #{value}" unless value.empty?
        end
        parts.reject(&:empty?).join("\n")
      end

      # 本文ブロックのテキスト。<br> を改行として読む。
      def self.block_text(element)
        return "" unless element

        element.css("br").each { |line_break| line_break.replace("\n") }
        element.text.strip
      end

      def self.text_or_default(element)
        text = normalize_text(element&.text)
        text.empty? ? "要確認" : text
      end

      def self.normalize_text(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      private_class_method :build_posting, :absolute_url, :extract_reward, :build_description,
                           :block_text, :text_or_default, :normalize_text
    end
  end
end
