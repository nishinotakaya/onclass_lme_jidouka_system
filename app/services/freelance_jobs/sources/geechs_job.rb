# frozen_string_literal: true

require "nokogiri"

module FreelanceJobs
  module Sources
    # ギークスジョブ: スキル別一覧ページ（li.c-card.p-card-project のカード）をHTMLパースする。
    # 完全SSRで、単価・勤務地・契約形態はカード内の箇条書き、募集スキル・ポジションは
    # ラベル違いの定義リスト（dl.project-detail-table）に分かれて入っている。
    class GeechsJob
      SITE_NAME = "ギークスジョブ"

      # robots.txt に `Crawl-Delay: 5` が明記されているため、他ソース（大半1.5秒）より
      # 大きく空ける。5未満に緩めるとクロール規約違反になる。
      REQUEST_INTERVAL = 5.0

      # robots.txt には `User-agent: python-requests` 宛の `Disallow: /` ブロックが別途あり、
      # requestsライブラリの既定UAを狙い撃ちで弾いている。HttpFetcherはChromeのUser-Agentを
      # 送るためこのブロックには該当しないが、将来UAをpython-requests系の文字列に変える／
      # デフォルトUAのHTTPクライアントに載せ替えると即ブロック対象になるので要注意。
      BASE_URL = "https://geechs-job.com"

      # 一覧はキーワード検索ではなくスキル別のパス（/project/<スラッグ>）。
      # スラッグは実地確認済みのものだけを使う。/project/react は404で存在しないため、
      # React技術者向けの案件はJavaScriptスラッグ（React案件も多く含まれる）で代替する。
      # 副作用として、Vue.js・jQuery限定など「JavaScriptだがReactではない」案件が、本文に
      # React系の語を1つも含まない場合に category_hint 経由でReact扱いになりうる
      # （EngineerClassifier は本文で技術語を拾えないとき hint をそのまま採用するため）。
      # Vue/Angular等が本文に明記されていれば正しく分類されるので、許容している。
      DEFAULT_SEARCH_TARGETS = [
        { slug: "ruby", hint: "Ruby" },
        { slug: "typescript", hint: "TypeScript" },
        { slug: "javascript", hint: "React" }
      ].freeze

      # 1ページ20件固定。実測の該当件数はRuby 704件／TypeScript 1,009件など桁違いに多く、
      # 全件取りに行くと Crawl-Delay 5秒 × スラッグ数 × ページ数 で所要時間が膨れ上がるため、
      # 1スラッグ3ページ（60件）までに抑える。新着順に並んでいるので直近案件はここでほぼ拾える。
      DEFAULT_PAGES_PER_SLUG = 3

      CARD_SELECTOR = "li.c-card.p-card-project"
      TITLE_LINK_SELECTOR = "h3.c-card_title a.c-card_title_link"

      # 単価・勤務地・契約形態は `ul.c-card-info01 > li` に並ぶが、classでは区別できず
      # アイコン／ラベルでしか見分けられない。
      PRICE_LABEL_TEXT = "単価"
      LOCATION_ICON_SELECTOR = "i.fa-map-marker-alt"
      CONTRACT_ICON_SELECTOR = "i.far.fa-handshake"

      # 「安定稼働」「BtoB」「ベテラン歓迎」等のタグ。
      TAG_SELECTOR = "div.project-preference a.p-preferenceIcon_link"

      # 募集スキル／ポジションは同じclass(dl.project-detail-table)の定義リストで、
      # dt内のラベルテキストでしか区別できない。
      SKILL_DEFINITION_LABEL = "募集スキル"
      POSITION_DEFINITION_LABEL = "ポジション"

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, pages_per_slug: DEFAULT_PAGES_PER_SLUG)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @pages_per_slug = pages_per_slug
      end

      # 通信あり。スラッグ×ページ数ぶん一覧ページを取得し、URLキーで重複排除する。
      # 同じ案件が複数スラッグ（例: Ruby×TypeScript）にまたがって出るため重複排除は必須。
      def fetch
        postings_by_url = {}

        @search_targets.each do |search_target|
          collect_slug_postings(search_target, postings_by_url)
        end

        postings_by_url.values
      end

      # スラッグ1つぶんのページ送り。取得した案件を postings_by_url に積む。
      # カードが1件も取れないページに出会ったら、そのスラッグの案件が尽きた（または
      # DOM構造が変わった）とみなして以降のページは取りに行かない。
      def collect_slug_postings(search_target, postings_by_url)
        @pages_per_slug.times do |page_index|
          page_number = page_index + 1
          body = @fetcher.get(search_url(search_target[:slug], page_number))
          page_postings = self.class.parse(body, today: @today, category_hint: search_target[:hint])
          break if page_postings.empty?

          page_postings.each { |posting| postings_by_url[posting.url] ||= posting }
        end
      end
      private :collect_slug_postings

      # 通信なし（テスト用）。一覧ページ1ページ分のHTML本文から案件一覧を作る。
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

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url(absolute_url(href)),
          title: title_link.text.gsub(/\s+/, " ").strip,
          description: FreelanceJobs::JobPosting.normalize_description(build_description(card)),
          category_hint: category_hint,
          reward: reward_text(card),
          work_format: contract_text(card),
          # 応募状況・締切・掲載日・企業名はサイト側に存在しないため固定値を入れる。
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: definition_links(card, SKILL_DEFINITION_LABEL),
          client: "",
          tags: tag_texts(card),
          posted_on: nil
        )
      end

      # 詳細ページのhrefは実測では常に絶対URLだが、相対URLで来た場合にも備えてBASE_URLを補う。
      def self.absolute_url(href)
        href.start_with?("http://", "https://") ? href : "#{BASE_URL}#{href}"
      end

      # 単価行。DOMは
      #   <li><span class="project-label">単価(税抜)</span><span class="c-text_price">70</span>
      #     <span class="u-text-coral">〜</span><span class="c-text_price">90</span>
      #     <span class="c-text_price-unit">万円/月</span></li>
      # と複数spanに分かれているため、li全体のテキストから空白を畳んでラベルを取り除き
      # "70〜90万円/月" の形にする。単価欄ごと無いカードは「要確認」にする。
      def self.reward_text(card)
        line = info_line(card) { |li| li.at_css(".project-label")&.text&.include?(PRICE_LABEL_TEXT) }
        return "要確認" unless line

        # 他サイト（ポテパン等）の "〜600,000円／月" と列の表記を揃えるため、ASCIIの
        # 「~」「/」は全角に直す（Classifier.first_reward_amount はどちらでも金額を拾える）。
        line.gsub(/\s+/, "").sub(/\A単価[（(]税抜[）)]/, "").tr("~", "〜").tr("/", "／")
      end

      # 勤務地。アイコン（fa-map-marker-alt）を目印に同じliのテキストを取る。
      def self.location_text(card)
        line = info_line(card) { |li| li.at_css(LOCATION_ICON_SELECTOR) }
        line&.gsub(/\s+/, "")
      end

      # 契約形態。「業務委託契約（フリーランス）」等。アイコン（fa-handshake）を目印にする。
      # 欄ごと無いカードは他サイトに合わせた既定値にする。
      def self.contract_text(card)
        line = info_line(card) { |li| li.at_css(CONTRACT_ICON_SELECTOR) }
        text = line&.gsub(/\s+/, "")
        text.nil? || text.empty? ? "業務委託（フリーランス）" : text
      end

      # `ul.c-card-info01 > li` を対象に、目印（アイコン or ラベル）で該当行を探しテキストを返す。
      def self.info_line(card, &matcher)
        line = card.css("ul.c-card-info01 > li").find(&matcher)
        line&.text
      end

      # タグ（安定稼働・BtoB・ベテラン歓迎など）。定義リストごと無いカードは空配列を許容する。
      def self.tag_texts(card)
        card.css(TAG_SELECTOR).map { |anchor| anchor.text.strip }.reject(&:empty?)
      end

      # 「募集スキル: Ruby / ポジション: システムエンジニア（SE）、プログラマ（PG）」の形に連結する。
      # 欠落しうる項目なので、取れたものだけを並べる。
      def self.build_description(card)
        parts = []
        location = location_text(card)
        parts << "勤務地: #{location}" if location && !location.empty?

        positions = definition_links(card, POSITION_DEFINITION_LABEL)
        parts << "ポジション: #{positions.join('、')}" unless positions.empty?

        skills = definition_links(card, SKILL_DEFINITION_LABEL)
        parts << "募集スキル: #{skills.join('、')}" unless skills.empty?

        parts.join(" / ")
      end

      # `dl.project-detail-table` は「募集スキル」「ポジション」の両方が同じclassで並ぶため、
      # dt内のラベルテキストで目的のdlを探し、dd内のリンクテキストを配列で返す。
      def self.definition_links(card, label)
        target_dd = card.css("dl.project-detail-table").find do |definition_list|
          definition_list.at_css(".project-label")&.text&.strip == label
        end&.at_css("dd.project-detail-dd")

        return [] unless target_dd

        target_dd.css("a.c-category_link").map { |anchor| anchor.text.strip }.reject(&:empty?)
      end

      # 一覧URL。1ページ目はpageパラメータを付けない（ブラウザで開くURLと同じ形にする）。
      def search_url(slug, page_number)
        url = "#{BASE_URL}/project/#{slug}"
        page_number > 1 ? "#{url}?page=#{page_number}" : url
      end
      private :search_url
    end
  end
end
