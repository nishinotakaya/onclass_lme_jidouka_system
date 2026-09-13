# frozen_string_literal: true

require "nokogiri"

module FreelanceJobs
  module Sources
    # PE-BANK: 言語別の案件一覧LP（/project/<言語slug>/?p=<ページ>）をHTMLパースする。
    # Laravel の完全SSRで、1ページ50件ぶんのカードマークアップが生HTMLに含まれる。
    # 一覧ページには JSON-LD が無い（詳細ページにだけ JobPosting JSON-LD がある）ため、DOMから項目を取り出す。
    # 並びは既定が新着順（sortパラメータ無し）なので、先頭数ページで新着案件を拾える。
    #
    # スキルID検索（/project/search/?skill[]=8&p=2）を使わない理由（2026-09-13 実測）:
    #   HttpFetcher(Faraday) は NestedParamsEncoder がクエリキーをアルファベット順に並べ替えるため
    #   実際のリクエストは ?p=2&skill%5B%5D=8 になる。サイト側はクエリ順が正規形（skill[]→p）と違うと
    #   301 で正規URLへ飛ばし、HttpFetcher はリダイレクトを追わないので2ページ目以降が全て HTTP 301 で落ちる。
    #   言語別LPはクエリが p だけなので並べ替えの影響を受けない。掲載内容もスキル検索とほぼ同じ
    #   （Ruby: LP 77件 / スキル検索 78件。差分はスキル欄に全言語を列挙した1件のみ）。
    #
    # HTML構造の前提（2026-09-13 実測。壊れたらここを疑う）:
    #   - カードは <ul class="projectList"><div class="box"><li>…</li>…</div></ul> という不正なネスト。
    #     Nokogiri もそのまま ul > div.box > li として木を作るため、`ul.projectList > li` では0件になる。
    #     また子孫セレクタ `ul.projectList li` はカード内の ul.projectListMerit > li まで拾ってしまう。
    #     そのため CARD_SELECTOR は必ず `ul.projectList > div.box > li` にする。
    #   - 案件名と詳細URLは h3.projectTitle a（href は絶対URL、例 https://pe-bank.jp/project/ruby/54618-65/）。
    #     同じ href が a.projectDetailBtn にもあるが h3 側を使う。
    #   - 項目は dl.projectListCts dd が4つ。各 dd の先頭 span がラベル（"単　価："/"勤務地："/"内　容："/"スキル："、
    #     全角スペース入り）、値は p。ラベルは空白と末尾の「：」を落として "単価"/"勤務地"/"内容"/"スキル" で引く。
    #   - 単価は p の中に <span>80</span>万円～<span>85</span>万円 のように数値だけ span で囲まれている。
    #   - スキルは p 内の span 1つに "Ruby , Typescript , Vue.js , React" と半角カンマ区切りで並ぶ
    #     （1スキルだけなら "Ruby"。サイト側の表記は "Typescript" と小文字s）。カンマで分割して配列にする。
    #   - こだわりタグは ul.projectListMerit li（担当者オススメ案件 / リモート可 / 長期案件 / 高単価 など）。
    #   - ページャは ul.pagiNation。最終ページでは li.next a に href が付かない。
    #   - 掲載日・応募状況・締切・発注企業名は一覧に無い（詳細の JSON-LD でも hiringOrganization は「社名非公開」）ため、
    #     posted_on/deadline_on は nil、application_status/deadline_text は "-"、client は "" で固定する。
    class PeBank
      SITE_NAME = "PE-BANK"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://pe-bank.jp"

      # 言語slugは一覧ページのフッター「言語から探す」リンク（/project/ruby/ 等）で実測した値。
      DEFAULT_SEARCH_TARGETS = [
        { language_slug: "ruby",       category_hint: "Ruby" },
        { language_slug: "typescript", category_hint: "TypeScript" },
        { language_slug: "react",      category_hint: "React" }
      ].freeze

      # 1言語あたりの取得ページ数。1ページ50件・新着順なので2ページで新着100件を拾える
      # （2026-09-13実測: Ruby 77件=2頁で全件 / Typescript 195件=4頁 / React 188件=4頁）。
      # 言語3種 × 2頁 = 最大6リクエスト/バッチ（再取得込みでも12回）で、リクエスト予算40回に十分収まる。
      MAX_PAGES = 2

      CARD_SELECTOR = "ul.projectList > div.box > li"
      TITLE_LINK_SELECTOR = "h3.projectTitle a"
      DEFINITION_ITEM_SELECTOR = "dl.projectListCts dd"
      MERIT_SELECTOR = "ul.projectListMerit li"
      NEXT_PAGE_LINK_SELECTOR = "ul.pagiNation li.next a[href]"

      # 案件詳細URLは /project/<言語slug>/<案件コード>/ の形（例 /project/ruby/54618-65/、/project/php/H5018-H01/）。
      # slug は検索スキルに関わらず java/php などが混在する。/project/ruby/ のような言語別LPは
      # 案件コードが無いので一致せず、案件として扱わない。
      JOB_URL_RE = %r{\A(?:https://pe-bank\.jp)?/project/[\w-]+/[A-Za-z0-9-]+/?\z}.freeze

      REWARD_LABEL = "単価"
      LOCATION_LABEL = "勤務地"
      CONTENT_LABEL = "内容"
      SKILLS_LABEL = "スキル"

      # 一覧の単価は全件「80万円～85万円」の月額表記（時間単価の表記は無い）。
      # 「80万」を Classifier::MAN_UNIT_RE が 800,000 と読めるよう数値はそのまま残し、
      # 単位「／月」を付けて levtech と同じ判定関数で work_format を分岐させる。
      MONTHLY_UNIT = "／月"

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, max_pages: MAX_PAGES)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @max_pages = max_pages
      end

      # 通信あり。言語slugごとに一覧を1ページ目から max_pages ページ分たどる。
      # ページ単位の取得失敗は握りつぶして次の言語へ進むが、1件も取れずに失敗だけが
      # 残った場合は最初の失敗を送出し、ResearchServiceに「取得失敗」として記録させる
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
      # todayは全取得元共通のインターフェースとして受け取るが、一覧に日付情報が無いため未使用。
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

      # 「次へ」リンクに href があれば次ページが存在する（最終ページでは <a> に href が付かない）。
      def self.next_page?(document)
        !document.at_css(NEXT_PAGE_LINK_SELECTOR).nil?
      end

      # 案件URLと案件名は表示・重複除去の両方に必須なので、欠けたカードは黙って捨てる
      # （セレクタが変わって全カードが欠損した場合は0件になり、呼び出し側のログで気付ける）。
      def self.build_posting(card, category_hint)
        title_link = card.at_css(TITLE_LINK_SELECTOR)
        return nil unless title_link

        job_url = job_url(title_link["href"])
        return nil unless job_url

        title = squish(title_link.text)
        return nil if title.empty?

        build_posting_from_parts(card, category_hint, job_url, title)
      end

      def self.build_posting_from_parts(card, category_hint, job_url, title)
        definition_items = definition_items(card)
        skills = split_skills(definition_items[SKILLS_LABEL]&.text)
        tags = card.css(MERIT_SELECTOR).map { |merit| squish(merit.text) }.reject(&:empty?)
        work_location = squish(definition_items[LOCATION_LABEL]&.text)
        reward = reward(definition_items[REWARD_LABEL])

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url(job_url),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(
            build_description(squish(definition_items[CONTENT_LABEL]&.text), skills, work_location, tags)
          ),
          category_hint: category_hint,
          reward: reward,
          work_format: work_format(reward),
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: skills,
          client: "",
          tags: tags,
          posted_on: nil
        )
      end

      # 実データの href は絶対URLだが、相対パスに変わっても動くよう BASE_URL を補う。
      def self.job_url(href)
        href = href.to_s.strip
        return nil unless href.match?(JOB_URL_RE)

        href.start_with?("/") ? "#{BASE_URL}#{href}" : href
      end

      # dl.projectListCts の dd を「ラベル => 値の p ノード」のHashにする。
      # ラベル span は "単　価：" のように全角スペースと末尾の「：」を含むので、両方を落として引き当てる。
      def self.definition_items(card)
        card.css(DEFINITION_ITEM_SELECTOR).each_with_object({}) do |definition_item, items|
          label = definition_item.at_css("span")&.text.to_s.gsub(/[[:space:]]+/, "").sub(/：\z/, "")
          value_node = definition_item.at_css("p")
          items[label] = value_node if !label.empty? && value_node
        end
      end

      # スキル欄は "Ruby , Typescript , Vue.js , React" のようにカンマ区切り1本のテキスト。
      # スキル名自体にカンマは含まれない（"Shell(C/B/K)" や "ノーコード/ローコード" はスラッシュ）ので
      # カンマだけで割る。全55スキルを列挙した「全部盛り」カードも数件あるがそのまま格納する
      # （EngineerClassifierはtitle+descriptionも見るため害は小さい）。
      def self.split_skills(skills_text)
        squish(skills_text).split(/\s*,\s*/).map(&:strip).reject(&:empty?)
      end

      # 内容・使用技術・勤務地・こだわりタグをdescriptionに入れる。EngineerClassifierはtitle+description+skillsを
      # 連結したテキストで判定するため、技術名と「リモート可」「長期案件」が本文に出ていると分類精度が上がる。
      def self.build_description(content, skills, work_location, tags)
        parts = []
        parts << "#{CONTENT_LABEL}: #{content}" unless content.empty?
        parts << "使用技術: #{skills.join(" / ")}" unless skills.empty?
        parts << "#{LOCATION_LABEL}: #{work_location}" unless work_location.empty?
        parts << tags.join("・") unless tags.empty?
        parts.join(" / ")
      end

      # 単価 p の中身は <span>80</span>万円～<span>85</span>万円。数値 span があれば月額として
      # "80万円～85万円／月" にする。span が無い場合はサイトが金額を出していない
      # （テキストが空なら "要確認"、「応相談」のような文言ならそのまま表示）とみなし単位を付けない。
      def self.reward(reward_node)
        reward_text = squish(reward_node&.text)
        return "要確認" if reward_text.empty?
        return reward_text if reward_node.at_css("span").nil?

        "#{reward_text}#{MONTHLY_UNIT}"
      end

      # 実データは全件「／月」だが、将来の時間単価表記に備えて単位で分岐させる。
      def self.work_format(reward_text)
        if reward_text.include?("／時")
          "時間単価制"
        elsif reward_text.include?(MONTHLY_UNIT)
          "月額制（業務委託）"
        else
          "業務委託（フリーランス）"
        end
      end

      def self.squish(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      private

      # 1言語ぶんのページ送り。カードが0件のページはページ終端（またはセレクタ崩れ）、
      # 「次へ」リンクに href が無いページは最終ページなので、それ以上リクエストしても無駄になるため打ち切る。
      def fetch_target(target, fetch_failures)
        postings = []

        (1..@max_pages).each do |page_number|
          body = fetch_page_body(target[:language_slug], page_number, fetch_failures)
          break if body.nil?

          document = Nokogiri::HTML(body)
          page_postings = self.class.parse_document(document, target[:category_hint])
          break if page_postings.empty?

          postings.concat(page_postings)
          break unless self.class.next_page?(document)
        end

        postings
      end

      # 散発的な取得失敗（HTTPエラー・タイムアウト・切断）で1ページ落ちても、言語全体・取得元全体を
      # 落とさず、その言語のページ送りだけを打ち切って次の言語へ進む。
      # rescueの範囲が@fetcher.getの1回だけなので、StandardErrorで受けてもパース側のバグは覆い隠さない。
      def fetch_page_body(language_slug, page_number, fetch_failures)
        get_with_single_retry(list_url(language_slug, page_number))
      rescue FreelanceJobs::AccessBlockedError
        raise
      rescue StandardError => error
        fetch_failures << error
        FreelanceJobs.logger.warn(
          "[FreelanceJobs::Sources::PeBank] #{error.message} この言語のページ送りを打ち切ります"
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
        FreelanceJobs.logger.warn("[FreelanceJobs::Sources::PeBank] #{error.message} 1回だけ再取得します")
        @fetcher.get(url)
      end

      # 実サイトのページャ（li.next a[href="?p=2"]）と同じ形にする（1ページ目には p を付けず、
      # 2ページ目以降だけ ?p=N を付ける）。クエリは p の1つだけなので Faraday の並べ替えの影響を受けない。
      def list_url(language_slug, page_number)
        url = "#{BASE_URL}/project/#{language_slug}/"
        page_number > 1 ? "#{url}?p=#{page_number}" : url
      end
    end
  end
end
