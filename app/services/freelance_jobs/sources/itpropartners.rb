# frozen_string_literal: true

require "nokogiri"
require "date"

module FreelanceJobs
  module Sources
    # ITプロパートナーズ: 技術スラッグ別の一覧ページ（article.itp-job-card のカード）をHTMLパースする。
    # 完全SSRで一覧の情報だけで案件名・単価・スキル・求める経験が揃うため、詳細ページは取得しない。
    #
    # 絞り込みの経路について（重要）:
    # robots.txt は `Disallow: /job*?*` でクエリ付きURLを一括禁止したうえで、
    # `Allow: /job*?page=` だけをページング用に例外許可している。この Allow は「?」の直後に
    # 文字列 "page=" が続く場合だけに一致する（間に他のクエリを挟むと一致しない）ため、
    # サイト自身の絞り込みUIが使う `/job?category=...` のようなクエリ型の検索URLは使えない。
    # 代わりに `/job/engineer/<スラッグ>` というパス型の絞り込みURLを使う。ページ送りも
    # サイトのページャが出す `?sort=new&page=N` をそのまま踏んではいけない（"?" の直後が
    # "sort=" になり Allow に一致しないため）。必ず `?page=N` 単独のクエリを自前で組み立てること。
    class Itpropartners
      SITE_NAME = "ITプロパートナーズ"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      # robots.txt に Crawl-delay の指定は無いため、他ソースと同じ既定値を使う。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://itpropartners.com"

      # スラッグは実地確認済み（件数は2026-09時点）。Reactは "/job/engineer/react" ではなく
      # "/job/engineer/reactjs" が正しいスラッグなので間違えないこと。
      DEFAULT_SEARCH_TARGETS = [
        { slug: "ruby", hint: "Ruby" },               # 463件
        { slug: "typescript", hint: "TypeScript" },   # 1,639件
        { slug: "reactjs", hint: "React" }             # 522件（"react"ではない）
      ].freeze

      # 1ページ40件固定。各スラッグの該当件数は数百〜1,639件と非常に多く、全件取得は現実的でないため、
      # 1スラッグあたり2ページ（=80件）に絞る。3スラッグ×2ページ＝6リクエストに収める。
      DEFAULT_PAGES_PER_SLUG = 2

      # 一覧カードのセレクタ。ページ内には「ランキングカルーセル」にも `div.itp-job-card`（タグはdiv）
      # というよく似たクラスの偽カード（会員登録を促す〜？？円/月のブラー表示）が別途あるため、
      # 実カードのタグである article に限定して弾く。
      CARD_SELECTOR = "article.itp-job-card"
      TITLE_HEADING_SELECTOR = "h2.itp-job-card__title"
      PRICE_SELECTOR = "span.itp-job-card__price"
      CONTRACT_TYPE_SELECTOR = ".itp-job-card__contract-type span"
      DETAIL_ROW_SELECTOR = ".itp-job-card__detail-row"
      DETAIL_LABEL_SELECTOR = ".itp-job-card__detail-label"
      DETAIL_VALUE_SELECTOR = ".itp-job-card__detail-value"
      TAG_SELECTOR = ".itp-job-card__tags .itp-job-card__tag"
      AGENT_COMMENT_SELECTOR = ".itp-job-card__agent-text"
      UPDATED_SELECTOR = ".itp-job-card__updated"

      # 「開発環境」欄はリンク付きの技術名（Ruby / Next.js 等）が並ぶため、これを skills に使う。
      SKILL_DETAIL_LABEL = "開発環境"
      SKILL_LINK_SELECTOR = ".itp-job-card__detail-link"

      # descriptionに載せる detail-row のラベル（この並び順がそのまま説明文の順序になる）。
      # 「開発環境」は上のSKILL_DETAIL_LABELとして skills に構造化して入れるため、
      # ここには含めない（重複を避ける）。
      # 「求めるスキル」には「3年以上」等の経験年数が自由文で入っており、後段の
      # EngineerClassifier のレベル判定（初級/中級/上級）がこの文字列を見るため必ず含める。
      DESCRIPTION_LABELS = ["働き方", "場所", "求めるスキル"].freeze

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, pages_per_slug: DEFAULT_PAGES_PER_SLUG)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @pages_per_slug = pages_per_slug
      end

      # 通信あり。スラッグ×ページ数ぶん一覧ページを取得し、URLキーで重複排除する。
      # Ruby/TypeScript/Reactを横断する案件（例: 「Ruby/Next.js」案件がRubyとTypeScript双方の
      # スラッグに出る）があり得るため、重複排除は必須。
      def fetch
        postings_by_url = {}

        @search_targets.each do |search_target|
          collect_slug_postings(search_target, postings_by_url)
        end

        postings_by_url.values
      end

      # スラッグ1つぶんのページ送り。取得した案件を postings_by_url に積む。
      # 該当件数がpages_per_slug×40件に満たないスラッグを踏んでも存在しないページ番号を
      # 要求しないよう、カードが1件も取れなかった時点で打ち切る。
      def collect_slug_postings(search_target, postings_by_url)
        (1..@pages_per_slug).each do |page_number|
          body = @fetcher.get(search_url(search_target[:slug], page_number))
          page_postings = self.class.parse(body, today: @today, category_hint: search_target[:hint])
          break if page_postings.empty?

          page_postings.each { |posting| postings_by_url[posting.url] ||= posting }
        end
      end
      private :collect_slug_postings

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

      # カード1件をJobPostingに組み立てる。案件タイトルの見出しがリンクの中に無いカードは
      # 一覧カードではない（または構造が変わった）と判断して nil を返し、呼び出し側で除外する。
      def self.build_posting(card, category_hint)
        heading = card.at_css(TITLE_HEADING_SELECTOR)
        return nil unless heading

        title_link = heading.parent
        return nil unless title_link&.name == "a"

        href = title_link["href"].to_s.strip
        return nil if href.empty?

        raw_price = raw_price_text(card)

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          # hrefは既に絶対URル（https://itpropartners.com/job/detail/<ID>）で来る。
          url: FreelanceJobs::JobPosting.normalize_url(href),
          title: heading.text.gsub(/\s+/, " ").strip,
          description: FreelanceJobs::JobPosting.normalize_description(build_description(card)),
          category_hint: category_hint,
          reward: display_reward(raw_price),
          work_format: work_format(card),
          # 応募状況・締切はサイト側に存在しないため固定値を入れる。
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: skill_names(card),
          client: "",
          tags: tag_names(card),
          posted_on: updated_on(card)
        )
      end

      # 単価の生テキスト。DOMは `<span class="itp-job-card__price">\n  〜600,000円/月\n</span>` で、
      # 改行・インデント分の空白文字を全て除去する。単価欄ごと空のカードを念のため許容する。
      def self.raw_price_text(card)
        text = card.at_css(PRICE_SELECTOR)&.text&.gsub(/[[:space:]]/, "")
        text.nil? || text.empty? ? nil : text
      end

      # 表示用の単価。他サイト（ポテパン等）の表記に揃えるため、ASCIIの「~」「/」を全角に直す。
      # 実測では既に全角チルダ「〜」だが、将来ASCII表記の案件が混ざっても崩れないようにしておく。
      def self.display_reward(raw_price)
        return "要確認" unless raw_price

        raw_price.tr("~", "〜").tr("/", "／")
      end

      # 業務形態。ITプロパートナーズは単価欄の隣に「業務委託」「準委任」等のラベルを明示しているため、
      # （ポテパンのように）単価の単位から推測するのではなく、このラベルをそのまま使う。
      def self.work_format(card)
        text = card.at_css(CONTRACT_TYPE_SELECTOR)&.text&.strip
        text.nil? || text.empty? ? "業務委託（フリーランス）" : text
      end

      # 「働き方: フルリモート 週3日〜5日 / 場所: … / 求めるスキル: … / エージェントより: …」の形に連結する。
      # 欠落しうる項目ばかりなので、取れたものだけを並べる。
      def self.build_description(card)
        parts = DESCRIPTION_LABELS.map do |label|
          value = detail_value(card, label)
          value && "#{label}: #{value}"
        end.compact

        comment = agent_comment(card)
        parts << "エージェントより: #{comment}" if comment
        parts.join(" / ")
      end

      # `.itp-job-card__detail-row` の中から、ラベル（働き方／場所／開発環境／求めるスキル）が
      # 一致する行を探して値のテキストを返す。DOM上はdt/dd形式ではなくラベルdivと値divの並びのため、
      # potepanのような「dt+隣接dd」ではなく行ごとラベル一致で探す必要がある。
      def self.detail_value(card, label)
        row = card.css(DETAIL_ROW_SELECTOR).find do |detail_row|
          detail_row.at_css(DETAIL_LABEL_SELECTOR)&.text&.strip == label
        end
        return nil unless row

        value = row.at_css(DETAIL_VALUE_SELECTOR)&.text&.gsub(/\s+/, " ")&.strip
        value.nil? || value.empty? ? nil : value
      end

      # 「開発環境」行のリンク（Ruby / Next.js 等）をskillsとして使う。技術名自体に空白が
      # 含まれる可能性を避けるため、値テキストをsplitせずa要素を個別に読む。
      def self.skill_names(card)
        row = card.css(DETAIL_ROW_SELECTOR).find do |detail_row|
          detail_row.at_css(DETAIL_LABEL_SELECTOR)&.text&.strip == SKILL_DETAIL_LABEL
        end
        return [] unless row

        row.css(SKILL_LINK_SELECTOR).map { |anchor| anchor.text.strip }.reject(&:empty?)
      end

      # カード上部の「スキルタグ」欄（技術名・職種・フルリモート等が混在するバッジ）。
      def self.tag_names(card)
        card.css(TAG_SELECTOR).map { |span| span.text.strip }.reject(&:empty?)
      end

      def self.agent_comment(card)
        comment = card.at_css(AGENT_COMMENT_SELECTOR)&.text&.gsub(/\s+/, " ")&.strip
        comment.nil? || comment.empty? ? nil : comment
      end

      # 「最終更新日：2026/09/01」を posted_on として使う。厳密には初回掲載日ではなく
      # 最終更新日だが、サイト側に掲載日相当の情報がこれしか無いため代用する。
      def self.updated_on(card)
        text = card.at_css(UPDATED_SELECTOR)&.text&.strip
        return nil unless text

        match = text.match(%r{(\d{4})/(\d{1,2})/(\d{1,2})})
        return nil unless match

        Date.new(match[1].to_i, match[2].to_i, match[3].to_i)
      rescue ArgumentError
        nil
      end

      # 一覧URL。1ページ目はpageパラメータを付けない（ブラウザで開くURLと同じ形にする）。
      # page_number>=2のときも、クラス冒頭のコメントの通り必ず"?page=N"単独のクエリにする
      # （サイトのページャが出す"?sort=new&page=N"をそのまま使ってはいけない）。
      def search_url(slug, page_number)
        url = "#{BASE_URL}/job/engineer/#{slug}"
        page_number > 1 ? "#{url}?page=#{page_number}" : url
      end
      private :search_url
    end
  end
end
