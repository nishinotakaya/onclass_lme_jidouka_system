# frozen_string_literal: true

require "nokogiri"

module FreelanceJobs
  module Sources
    # Relance: スキル別タクソノミー一覧（/project/<スキルslug>/、2ページ目以降は /page/<n>/）をHTMLパースする。
    # WordPress製だがトップの検索（?s=）は使わない。検索結果は li.c-articles__item に URL と案件名しか無く、
    # ブログ記事（/blog/）も混ざるため。スキル別一覧は SSR で1ページ10件のカードが生HTMLに含まれ、
    # 単価・募集職種・スキル・稼働日数・働き方まで揃う。JSON-LD は無い。
    #
    # HTML構造の前提（壊れたらここを疑う）:
    #   - カード: article.p-project_item、直下の a[href] が案件詳細への絶対URL（例 https://relance.jp/project/ruby/jd02664/）。
    #     slug 部分は案件の主スキル（ruby / rubyonrails / typescript / other など）で、検索したスキルと一致するとは限らない
    #   - 案件名: h2.p-project_item__title
    #   - 単価: .p-project_item__price_range（例 "63万円 ～ 78万円 / 月"）。数値は span.p-project_item__price_lg に万円単位で入る
    #     （単価非公開の案件は数値spanが無く "～ / 月" だけが残る）
    #   - バッジ: .p-project_item__meta .c-badge（"NEW" / "高単価"）
    #   - スペック表: dl.p-project_spec .p-project_spec__item。ラベルは dt .u-sp-sr-only、値は dd .c-term の配列
    #     （c-term が無い項目は dd のテキスト）。ラベルは「募集職種」「スキル」「稼働日数」「働き方」の4種で、
    #     「働き方」は無いカードもある
    #   - ページング: section.p-pagination a.next があれば次ページあり。最終ページにも a.prev と page/1/ への
    #     a.p-pagination__num が残るため、「/page/ を含むリンクの有無」では判定できない
    #
    # 注意: 掲載日・応募状況・締切・発注企業名はサイトのどこにも無いため、posted_on/deadline_on は nil、
    #   application_status/deadline_text は "-"、client は "" で固定する。NEW バッジを tags に残して新着の目安にする。
    class Relance
      SITE_NAME = "Relance"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://relance.jp"

      # slug は絞り込みフォーム select[data-taxonomy="lang"] option[value] の値。
      # ruby 一覧には rubyonrails スラッグの案件も一部含まれるが全てではない（2026-09-13計測: rubyonrails 5件中
      # 2件が ruby に無い）ため、両方たどってURLキーの重複除去で吸収する。
      DEFAULT_SEARCH_TARGETS = [
        { skill_slug: "ruby",         category_hint: "Ruby" },
        { skill_slug: "rubyonrails",  category_hint: "Ruby" },
        { skill_slug: "typescript",   category_hint: "TypeScript" },
        { skill_slug: "react",        category_hint: "React" }
      ].freeze

      # 1スキルあたりの取得ページ数の上限（安全弁）。1ページ10件で、2026-09-13時点の最多は typescript の2ページ。
      # 既定4スキルで一覧のみ取得するため、1バッチのリクエストは最大 4 × 3 = 12回（実測は5回）。
      MAX_PAGES_PER_SKILL = 3

      CARD_SELECTOR = "article.p-project_item"
      CARD_LINK_SELECTOR = "a[href]"
      TITLE_SELECTOR = "h2.p-project_item__title"
      PRICE_RANGE_SELECTOR = ".p-project_item__price_range"
      PRICE_AMOUNT_SELECTOR = ".p-project_item__price_lg"
      BADGE_SELECTOR = ".p-project_item__meta .c-badge"
      SPEC_ITEM_SELECTOR = "dl.p-project_spec .p-project_spec__item"
      SPEC_LABEL_SELECTOR = "dt .u-sp-sr-only"
      SPEC_TERM_SELECTOR = "dd .c-term"
      NEXT_PAGE_LINK_SELECTOR = "section.p-pagination a.next"

      # 案件詳細URLのパス形（例 /project/ruby/jd02664/、/project/ruby/jd01585-2/）。
      # 一覧内に将来別種のリンクが混ざっても拾わないよう、この形だけを案件として扱う。
      JOB_PATH_RE = %r{\A/project/[^/]+/jd\d+[^/]*/?\z}.freeze

      # 単価の数値は "63" "135" のような万円単位の整数（小数が来ても読めるようにしておく）。
      PRICE_AMOUNT_RE = /\A\d+(?:\.\d+)?\z/.freeze

      SPEC_LABEL_OCCUPATION = "募集職種"
      SPEC_LABEL_SKILLS = "スキル"
      SPEC_LABEL_WORKING_DAYS = "稼働日数"
      SPEC_LABEL_WORK_STYLE = "働き方"

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, max_pages: MAX_PAGES_PER_SKILL)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @max_pages = max_pages
      end

      # 通信あり。スキルslugごとに一覧を1ページ目から「次ページリンクが無くなるまで」（上限 max_pages）たどる。
      # ページ単位の取得失敗は握りつぶして次のスキルへ進むが、1件も取れずに失敗だけが残った場合は
      # 最初の失敗を送出し、ResearchServiceに「取得失敗」として記録させる
      # （黙って0件を返すと、サイト構造の崩れや全面的な障害に気付けなくなるため）。
      def fetch
        postings = {}
        fetch_failures = []

        @search_targets.each do |target|
          fetch_target(target, fetch_failures).each { |posting| postings[posting.url] ||= posting }
        end

        raise fetch_failures.first if postings.empty? && !fetch_failures.empty?

        postings.values
      end

      # 通信なし（テスト用）。一覧1ページ分のHTML本文から案件一覧を作る。
      # todayは全取得元共通のインターフェースとして受け取るが、Relanceには掲載日・締切の概念が無いため参照しない。
      def self.parse(body, today:, category_hint: nil)
        parse_document(Nokogiri::HTML(body), category_hint)
      end

      def self.parse_document(document, category_hint)
        postings = {}

        document.css(CARD_SELECTOR).each do |card|
          posting = build_posting(card, category_hint)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      # 次ページの有無。a.next はページ送りの右端ボタンで、最終ページでは描画されない。
      def self.next_page?(document)
        !document.at_css(NEXT_PAGE_LINK_SELECTOR).nil?
      end

      # 案件URLと案件名は表示・重複除去の両方に必須なので、欠けたカードは黙って捨てる
      # （セレクタが変わって全カードが欠損した場合は0件になり、呼び出し側のログで気付ける）。
      def self.build_posting(card, category_hint)
        job_url = job_url(card)
        return nil unless job_url

        title = squish(card.at_css(TITLE_SELECTOR)&.text)
        return nil if title.empty?

        build_posting_from_parts(card, category_hint, job_url, title)
      end

      def self.build_posting_from_parts(card, category_hint, job_url, title)
        spec_table = spec_table(card)
        price_unit = price_unit(card.at_css(PRICE_RANGE_SELECTOR)&.text)
        badges = card.css(BADGE_SELECTOR).map { |badge| squish(badge.text) }.reject(&:empty?)
        work_styles = spec_table.fetch(SPEC_LABEL_WORK_STYLE, [])

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: job_url,
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(build_description(spec_table)),
          category_hint: category_hint,
          reward: reward(price_amounts(card), price_unit),
          work_format: work_format(price_unit),
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: spec_table.fetch(SPEC_LABEL_SKILLS, []),
          client: "",
          tags: badges + work_styles,
          posted_on: nil
        )
      end

      # カード直下のリンクから案件URLを取り出す。実データは絶対URLだが、相対パスで来ても組み立てられるようにする。
      # 案件詳細のパス形に合わないリンク（別サイトへの誘導など）は案件として扱わない。
      def self.job_url(card)
        href = card.at_css(CARD_LINK_SELECTOR)&.[]("href").to_s.strip
        return nil if href.empty?

        absolute_url = href.start_with?("/") ? "#{BASE_URL}#{href}" : href
        return nil unless absolute_url.start_with?("#{BASE_URL}/")

        path = absolute_url.delete_prefix(BASE_URL).split("?", 2).first
        path.match?(JOB_PATH_RE) ? FreelanceJobs::JobPosting.normalize_url(absolute_url) : nil
      end

      # スペック表を「ラベル => 値の配列」のHashにする。c-term（タグ表示）の項目は複数値、
      # それ以外（稼働日数）は dd のテキスト1件。「週3~5日日」のような表記揺れはサイト側のデータ入力ミスなのでそのまま格納する。
      def self.spec_table(card)
        card.css(SPEC_ITEM_SELECTOR).each_with_object({}) do |item, table|
          label = squish(item.at_css(SPEC_LABEL_SELECTOR)&.text)
          next if label.empty?

          values = spec_values(item)
          table[label] = values unless values.empty?
        end
      end

      def self.spec_values(item)
        terms = item.css(SPEC_TERM_SELECTOR).map { |term| squish(term.text) }.reject(&:empty?)
        return terms unless terms.empty?

        plain_value = squish(item.at_css("dd")&.text)
        plain_value.empty? ? [] : [plain_value]
      end

      # 募集職種・スキル・稼働日数・働き方を1行に連結する。EngineerClassifierはtitle+description+skillsを
      # 連結したテキストで判定するため、技術名とリモート可否（フルリモート）が本文に出ていると分類精度が上がる。
      def self.build_description(spec_table)
        [SPEC_LABEL_OCCUPATION, SPEC_LABEL_SKILLS, SPEC_LABEL_WORKING_DAYS, SPEC_LABEL_WORK_STYLE]
          .select { |label| spec_table.key?(label) }
          .map { |label| "#{label}: #{spec_table[label].join(", ")}" }
          .join(" / ")
      end

      # 単価の数値（万円単位）を円に換算して返す。想定外の表記（数値でない）は読み飛ばす。
      def self.price_amounts(card)
        card.css(PRICE_AMOUNT_SELECTOR).map { |amount_node| squish(amount_node.text) }
            .select { |amount_text| amount_text.match?(PRICE_AMOUNT_RE) }
            .map { |amount_text| (amount_text.to_f * 10_000).to_i }
      end

      # 単価の単位。実データは全件 "/ 月" だが、時間単価表記に備えて "/ 時" も読む。どちらでもなければ空文字。
      # "/" と単位のあいだに改行・スペースが入るため、空白を全て除いてから判定する。
      def self.price_unit(price_range_text)
        compact_text = price_range_text.to_s.gsub(/[[:space:]]+/, "")
        if compact_text.include?("/時")
          "／時"
        elsif compact_text.include?("/月")
          "／月"
        else
          ""
        end
      end

      # "63万円 ～ 78万円 / 月" を "630,000〜780,000円／月" に組み直す。EngineerClassifier.high_reward? が
      # カンマを除いた先頭の数値を読むため、万円のままにせず円に換算して3桁区切りにする。
      # 数値が1つだけなら範囲にせず単独表記、数値が無ければ "要確認"。
      def self.reward(amounts, price_unit)
        return "要確認" if amounts.empty?

        formatted_amounts = amounts.map { |amount| FreelanceJobs.format_number(amount) }
        "#{formatted_amounts.uniq.join("〜")}円#{price_unit}"
      end

      # 単価の単位で契約形態を判定する。単価非公開でも単位（"/ 月"）は表示されるため、金額の有無とは切り離して判定する。
      def self.work_format(price_unit)
        case price_unit
        when "／時" then "時間単価制"
        when "／月" then "月額制（業務委託）"
        else "業務委託（フリーランス）"
        end
      end

      def self.squish(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      private

      # 1スキルぶんのページ送り。カードが0件のページはページ終端（またはセレクタ崩れ）、
      # a.next が無いページは最終ページなので、それ以上リクエストしても無駄になるため打ち切る。
      def fetch_target(target, fetch_failures)
        postings = []

        (1..@max_pages).each do |page_number|
          body = fetch_page_body(target[:skill_slug], page_number, fetch_failures)
          break if body.nil?

          document = Nokogiri::HTML(body)
          page_postings = self.class.parse_document(document, target[:category_hint])
          break if page_postings.empty?

          postings.concat(page_postings)
          break unless self.class.next_page?(document)
        end

        postings
      end

      # 散発的なHTTPエラー・通信層のタイムアウトは1ページの失敗でスキル全体・取得元全体を落とさず、
      # そのスキルのページ送りだけを打ち切って次のスキルへ進む。
      # rescueの範囲が@fetcher.getの1回だけなので、StandardErrorで受けてもパース側のバグは覆い隠さない。
      def fetch_page_body(skill_slug, page_number, fetch_failures)
        get_with_single_retry(list_url(skill_slug, page_number))
      rescue FreelanceJobs::AccessBlockedError
        raise
      rescue StandardError => error
        fetch_failures << error
        FreelanceJobs.logger.warn(
          "[FreelanceJobs::Sources::Relance] #{error.message} このスキルのページ送りを打ち切ります"
        )
        nil
      end

      # 散発的な失敗は1回だけ取り直す。HttpFetcherが2回目以降のリクエスト前に
      # REQUEST_INTERVAL秒 sleep するため、ここで追加のsleepは要らない。
      # WAFのアクセス制限は取り直しても解消しないので、AccessBlockedErrorはそのまま送出する。
      def get_with_single_retry(url)
        @fetcher.get(url)
      rescue FreelanceJobs::AccessBlockedError
        raise
      rescue StandardError => error
        FreelanceJobs.logger.warn("[FreelanceJobs::Sources::Relance] #{error.message} 1回だけ再取得します")
        @fetcher.get(url)
      end

      # 1ページ目は /project/<slug>/、2ページ目以降は /project/<slug>/page/<n>/（WordPressのページング形式。
      # 実データの a.next の href と同じ形）。
      def list_url(skill_slug, page_number)
        return "#{BASE_URL}/project/#{skill_slug}/" if page_number == 1

        "#{BASE_URL}/project/#{skill_slug}/page/#{page_number}/"
      end
    end
  end
end
