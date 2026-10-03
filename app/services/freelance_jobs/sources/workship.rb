# frozen_string_literal: true

require "nokogiri"

module FreelanceJobs
  module Sources
    # Workship: キーワード別の一覧ページ（li.projects_item のカード）をHTMLパースする。
    # 完全SSRで、一覧カードだけで職種・業務内容・企業名まで揃うため、詳細ページは取得しない。
    # 一覧に単価の表示が無いため、reward は「要確認」固定とする。
    #
    # robots.txt について（重要）: Disallow は `/social-auth/` のみで、一覧・案件詳細が使う
    # `/portal/...` は対象外。キーワード一覧（/portal/keyword-xxx）はブラウザで開くURLと同じ形のため、
    # 検索フォームのクエリ型URLではなくこちらを使う。
    #
    # ページングは `?page=N` のクエリ型で、1ページ目はクエリを付けない（20件/頁）。
    # 範囲外のページ番号は 404 を返す（実測: ruby は 2頁目まで 200、3頁目は 404）。
    # そのため2ページ目以降の 404 はページ終端として打ち切る。1ページ目の 404 は障害として例外を上げる。
    # カード0件のHTMLが返った場合も、page_postings.empty? で同様に打ち切る。
    class Workship
      SITE_NAME = "Workship"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      # robots.txt に Crawl-delay の指定は無いため、他ソースと同じ既定値を使う。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://goworkship.com"

      DEFAULT_SEARCH_TARGETS = [
        { keyword: "ruby", hint: "Ruby" },
        { keyword: "typescript", hint: "TypeScript" },
        { keyword: "react", hint: "React" }
      ].freeze

      # 1キーワードあたり3ページ（最大60件）まで。新着差分取りには十分なため、全件をたどらない。
      DEFAULT_PAGES_PER_KEYWORD = 3

      CARD_SELECTOR = "li.projects_item"
      # 案件詳細（/portal/<会社スラッグ>/job/<id>）へのリンク。カード内には会社ページ
      # （/portal/<会社スラッグ>）へのリンクも同じ接頭辞で存在するため、/job/ を含むパスで絞る。
      # リンクの並び順に依存すると、会社リンクが先に来る構造変更で会社ページを案件として登録してしまう。
      DETAIL_LINK_SELECTOR = 'a[href^="/portal/"][href*="/job/"]'
      TITLE_SELECTOR = "h3.projects_item_ttl"
      PROFESSION_SELECTOR = ".projects_item_profession span"
      DESCRIPTION_SELECTOR = "p.projects_item_description"
      CLIENT_SELECTOR = ".projects_item_icon .name"

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS,
                     pages_per_keyword: DEFAULT_PAGES_PER_KEYWORD)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @pages_per_keyword = pages_per_keyword
      end

      # 通信あり。キーワード×ページ数ぶん一覧ページを取得し、URLキーで重複排除する。
      # 同じ案件が複数キーワードに出るため重複排除は必須で、先に出たキーワードのhintを残す。
      def fetch
        postings_by_url = {}

        @search_targets.each do |search_target|
          collect_keyword_postings(search_target, postings_by_url)
        end

        postings_by_url.values
      end

      # キーワード1つぶんのページ送り。取得した案件を postings_by_url に積む。
      # カードが1件も取れないページに当たったら、ページ終端とみなして打ち切る。
      def collect_keyword_postings(search_target, postings_by_url)
        (1..@pages_per_keyword).each do |page_number|
          begin
            body = @fetcher.get(search_url(search_target[:keyword], page_number))
          rescue FreelanceJobs::FetchError => fetch_error
            break if end_of_pages?(fetch_error, page_number)

            raise
          end
          page_postings = self.class.parse(body, today: @today, category_hint: search_target[:hint])
          break if page_postings.empty?

          page_postings.each { |posting| postings_by_url[posting.url] ||= posting }
        end
      end
      private :collect_keyword_postings

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

      # カード1件をJobPostingに組み立てる。案件詳細URLまたはタイトルが取れないカードは
      # 一覧カードではない（または構造が変わった）と判断して nil を返し、呼び出し側で除外する。
      def self.build_posting(card, category_hint)
        detail_link = card.at_css(DETAIL_LINK_SELECTOR)
        return nil unless detail_link

        href = detail_link["href"].to_s.strip
        return nil if href.empty?

        title = card.at_css(TITLE_SELECTOR)&.text.to_s.gsub(/\s+/, " ").strip
        return nil if title.empty?

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}#{href}"),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(card.at_css(DESCRIPTION_SELECTOR)&.text),
          category_hint: category_hint,
          # 単価・応募状況・締切・掲載日は一覧に存在しないため固定値を入れる。
          reward: "要確認",
          work_format: "業務委託（フリーランス）",
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          # 技術名はtitle/descriptionから分類器が拾うため、skillsは空にする。
          skills: [],
          client: card.at_css(CLIENT_SELECTOR)&.text.to_s.strip,
          tags: profession_names(card),
          posted_on: nil
        )
      end

      # 職種（バックエンドエンジニア 等）をtagsとして使う。
      def self.profession_names(card)
        card.css(PROFESSION_SELECTOR).map { |profession| profession.text.strip }.reject(&:empty?)
      end

      private_class_method :build_posting, :profession_names

      # 一覧URL。1ページ目はpageパラメータを付けず、2ページ目以降は"?page=N"を付ける。
      def search_url(keyword, page_number)
        url = "#{BASE_URL}/portal/keyword-#{keyword}"
        page_number > 1 ? "#{url}?page=#{page_number}" : url
      end
      private :search_url
    end
  end
end
