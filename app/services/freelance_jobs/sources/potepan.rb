# frozen_string_literal: true

require "nokogiri"
require "cgi"

module FreelanceJobs
  module Sources
    # ポテパンフリーランス: キーワード検索結果ページ（section.single-project のカード）をHTMLパースする。
    # 完全SSR（Rails + turbolinks）でJSON-LD・__NEXT_DATA__の類は一切無いため、カード内の
    # 定義リスト（dtのclassで引く）から業務内容・必須スキル・勤務地・キーワード・特徴を取り出す。
    #
    # 掲載日・企業名・応募状況・締切はサイト全体に存在しない。案件詳細ページが一覧に対して
    # 追加する情報は「尚可スキル」だけなので、詳細ページは取得せず一覧だけで完結させる
    # （リクエストが10倍になる割に得られる情報が無いため）。
    class Potepan
      SITE_NAME = "ポテパンフリーランス"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://freelance.potepan.com"

      DEFAULT_SEARCH_TARGETS = [
        { keyword: "Ruby", hint: "Ruby" },
        { keyword: "TypeScript", hint: "TypeScript" },
        { keyword: "React", hint: "React" }
      ].freeze

      # 1ページ10件固定。実測の該当件数は Ruby 136件 / TypeScript 98件 / React 133件なので、
      # 3ページ（各30件）を上限にして 3キーワード×3ページ＝9リクエストに収める。
      DEFAULT_PAGES_PER_KEYWORD = 3

      # 一覧カードのセレクタ。詳細ページにも同じclassが出るが、詳細ページのh3はリンクでは
      # ないため build_posting で弾かれる（parseは一覧ページ専用）。
      CARD_SELECTOR = "section.single-project"
      TITLE_LINK_SELECTOR = "h3 a.single-project__title"
      PRICE_SELECTOR = "p.single-project__price"
      # 担当者コメント。10件中2件程度しか存在しない任意項目。
      RECOMMEND_COMMENT_SELECTOR = "section.RecommendPoint p.RecommendPoint__body"

      # descriptionに載せる定義リスト項目（dtのclass => 表示ラベル）。この並び順がそのまま
      # 説明文の順序になる。必須スキル欄に技術名（Ruby on Rails / React / TypeScript 等）が
      # 濃く入るため、EngineerClassifier の判定精度はここを含めるかどうかで決まる。
      DESCRIPTION_DEFINITIONS = {
        "single-project__content" => "業務内容",
        "single-project__required" => "必須スキル",
        "single-project__location" => "勤務地"
      }.freeze

      # ページャ。`<nav class="pagination"><a class="page-numbers" href="/projects?keyword=Ruby&page=4">4</a>…</nav>`
      # の形で、現在ページの近傍だけが並び離れたページは "..." に畳まれる。1ページに収まる
      # 検索ではnav.pagination自体が出力されない。
      # なお右矢印のリンクには次ページのhrefに対して rel="prev" が付く（サイト側の誤り）ため、
      # rel属性は当てにせずhrefのpage番号だけで判定する。
      PAGINATION_LINK_SELECTOR = "nav.pagination a.page-numbers"

      # キーワード（使用技術）欄。dd内は `<a href="/project/skill-1">Ruby</a>` の集合。
      SKILL_DT_CLASS = "single-project__skill"
      # 特徴欄。"リモート勤務可" / "高単価" / "フレックス可" / "自社サービス" などのラベル。
      FEATURE_DT_CLASS = "single-project__feature"

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, pages_per_keyword: DEFAULT_PAGES_PER_KEYWORD)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @pages_per_keyword = pages_per_keyword
      end

      # 通信あり。キーワード×ページ数ぶん検索結果ページを取得し、URLキーで重複排除する。
      # 3キーワードの検索結果には同じ案件が混ざるため（1案件が Ruby/TypeScript/React すべてに
      # 出る例を実測で確認）、重複排除は必須。
      def fetch
        postings_by_url = {}

        @search_targets.each do |search_target|
          collect_keyword_postings(search_target, postings_by_url)
        end

        postings_by_url.values
      end

      # キーワード1つぶんのページ送り。取得した案件を postings_by_url に積む。
      #
      # 存在しないページ番号（最終ページの次・該当0件のキーワード）にアクセスすると、
      # ポテパンは案件検索トップへの HTTP 302 を返す。HttpFetcher はリダイレクトを追わず
      # 非200を FetchError にするため、踏むと ResearchService 側でこの取得元が丸ごと失敗扱いになり、
      # そのキーワードまでに取れていた案件も含めて全部失われる。
      # そのため「ページャに次ページへのリンクがあるときだけ次を取りに行く」方式にして、
      # 存在しないページ番号を最初から要求しない（実測: 最終ページのページャは前ページしか
      # リンクせず、1ページに収まる検索ではページャ自体が無い）。
      def collect_keyword_postings(search_target, postings_by_url)
        (1..@pages_per_keyword).each do |page_number|
          body = @fetcher.get(search_url(search_target[:keyword], page_number))
          page_postings = self.class.parse(body, today: @today, category_hint: search_target[:hint])
          # カードが1件も取れないページはDOMが変わったとみなし、以降のページは取りに行かない。
          break if page_postings.empty?

          page_postings.each { |posting| postings_by_url[posting.url] ||= posting }
          break unless self.class.next_page_linked?(body, page_number)
        end
      end
      private :collect_keyword_postings

      # 通信なし。ページャに「現在ページ+1」へのリンクがあるか。
      # ページャは現在ページの前後だけを並べるので、1ページずつ順に進む限り
      # 「次ページのリンクの有無」＝「次ページが存在するか」になる。
      def self.next_page_linked?(body, current_page_number)
        document = Nokogiri::HTML(body)

        document.css(PAGINATION_LINK_SELECTOR).any? do |link|
          link["href"].to_s[/[?&]page=(\d+)/, 1].to_i == current_page_number + 1
        end
      end

      # 通信なし（テスト用）。検索結果1ページ分のHTML本文から案件一覧を作る。
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

      # カード1件をJobPostingに組み立てる。案件URLとタイトルが取れないカードは
      # 一覧カードではない（または構造が変わった）と判断して nil を返し、呼び出し側で除外する。
      def self.build_posting(card, category_hint)
        title_link = card.at_css(TITLE_LINK_SELECTOR)
        return nil unless title_link

        href = title_link["href"].to_s.strip
        return nil if href.empty?

        raw_price = raw_price_text(card)

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}#{href}"),
          # aタグ内は前後に改行が入るため必ず空白を畳む。
          title: title_link.text.gsub(/\s+/, " ").strip,
          description: FreelanceJobs::JobPosting.normalize_description(build_description(card)),
          category_hint: category_hint,
          reward: display_reward(raw_price),
          work_format: work_format(raw_price),
          # 応募状況・締切・掲載日・企業名はサイト側に存在しないため固定値を入れる。
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: definition_links(card, SKILL_DT_CLASS),
          client: "",
          tags: definition_links(card, FEATURE_DT_CLASS),
          posted_on: nil
        )
      end

      # 単価の生テキスト。DOMは
      #   <p class="single-project__price"><span class="--prefix">~</span>600,000<span class="--unit">円/月</span></p>
      # で、区切りはASCIIの「~」「/」。spanで分断された空白を落とすため空白文字を全て除去する。
      # 単価欄ごと空のカード（例: 「受託開発案件」）が実在するため nil を許容する。
      def self.raw_price_text(card)
        text = card.at_css(PRICE_SELECTOR)&.text&.gsub(/[[:space:]]/, "")
        text.nil? || text.empty? ? nil : text
      end

      # 表示用の単価。他サイト（レバテック等）の "〜7,680円／時" に表記を揃えるため、
      # ASCIIの「~」「/」を全角に直す。例: "~600,000円/月" → "〜600,000円／月"
      # （Classifier.first_reward_amount はどちらの表記でも金額を拾えることを実測で確認済み）。
      def self.display_reward(raw_price)
        return "要確認" unless raw_price

        raw_price.tr("~", "〜").tr("/", "／")
      end

      # 契約形態は単位で判定する。判定は必ず全角化する前の生テキスト（ASCIIスラッシュ）で行う。
      # 実測40カードは全て「円/月」だったが、単位がspanで分離されている以上「円/時」もあり得る
      # ため分岐を残す。
      def self.work_format(raw_price)
        return "業務委託（フリーランス）" unless raw_price

        if raw_price.include?("円/時")
          "時間単価制"
        elsif raw_price.include?("円/月")
          "月額制（業務委託）"
        else
          "業務委託（フリーランス）"
        end
      end

      # 「業務内容: … / 必須スキル: … / 勤務地: … / 担当者コメント: …」の形に連結する。
      # 欠落しうる項目ばかりなので、取れたものだけを並べる。
      def self.build_description(card)
        parts = DESCRIPTION_DEFINITIONS.map do |dt_class, label|
          value = definition_value(card, dt_class)
          value && "#{label}: #{value}"
        end.compact

        comment = recommend_comment(card)
        parts << "担当者コメント: #{comment}" if comment
        parts.join(" / ")
      end

      # `<dl><dt class="single-project__content">業務内容</dt><dd>…</dd></dl>` の並びから、
      # dtのclassを手がかりに隣接するddの文字列を取る。dd内は<br>区切りの箇条書きなので
      # 空白を1つに畳む。勤務地のddはリンクの場合と素テキストの場合が混在するため、
      # a要素ではなくdd全体のテキストを使う。
      def self.definition_value(card, dt_class)
        value = card.at_css("dt.#{dt_class} + dd")&.text&.gsub(/\s+/, " ")&.strip
        value.nil? || value.empty? ? nil : value
      end

      # キーワード（使用技術）・特徴のddからラベルを取り出す。ddのテキストは " / " 区切りだが、
      # 技術名自体が "/" を含む可能性を避けるため、テキストをsplitせずa要素を個別に読む。
      # 定義リストごと存在しないカード・中身が空のカードがあるため空配列を許容する。
      def self.definition_links(card, dt_class)
        card.css("dt.#{dt_class} + dd a").map { |anchor| anchor.text.strip }.reject(&:empty?)
      end

      def self.recommend_comment(card)
        comment = card.at_css(RECOMMEND_COMMENT_SELECTOR)&.text&.gsub(/\s+/, " ")&.strip
        comment.nil? || comment.empty? ? nil : comment
      end

      # 一覧URL。1ページ目はpageパラメータを付けない（ブラウザで開くURLと同じ形にする）。
      def search_url(keyword, page_number)
        url = "#{BASE_URL}/projects?keyword=#{CGI.escape(keyword)}"
        page_number > 1 ? "#{url}&page=#{page_number}" : url
      end
      private :search_url
    end
  end
end
