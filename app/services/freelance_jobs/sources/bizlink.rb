# frozen_string_literal: true

require "nokogiri"
require "date"

module FreelanceJobs
  module Sources
    # ビズリンク: スキル別一覧（/jobs/skill_cate/<スキルslug>/p/<ページ>）をHTMLパースする。
    # Next.js App Router のSSR出力で、1ページ9件ぶんのカードマークアップが生HTMLに含まれる。
    # JSON-LDは WebSite のみで JobPosting は無いため、JSONではなくDOMから項目を取り出す。
    #
    # 注意1: CSS Modules のためクラス名には "ProjectCardOnList_jobTitle__Z7bct" のような
    #   ビルドごとに変わるハッシュ接尾辞が付く。そのため全セレクタを前方一致（class^=）にしている。
    #   2026-09-12時点の実クラス名は
    #     a.ProjectCardOnList_ProjectCard__BypSw / div.ProjectCardOnList_datePosted__DL907 /
    #     p.ProjectCardOnList_jobTitle__Z7bct / span.ProjectCardLabel_label__54kG0 /
    #     span.ProjectCardOnList_salary__DbOs1 / span.ProjectCardOnList_currency__JHk_i /
    #     div.ProjectCardOnList_locationInfo__49IGY / li.SkillSetLabel_skill__WDerZ /
    #     div.ProjectCardDetails_section__CU_7o / p.ProjectCardDetails_sectionTitle__2WKQn /
    #     p.ProjectCardDetails_content__pyURC
    #   ハッシュ部分が変わってもプレフィックスが同じなら壊れない。
    # 注意2: RSC flight payload（<script>self.__next_f.push(...)</script>）に同じカードのJSONが
    #   重複して埋まっているため、生HTMLを文字列grepするとカードが18件に見える。
    #   Nokogiriは<script>の中身を要素化しないので、document.cssは正しく9件だけ返す。
    # 注意3: 応募状況・応募締切・発注企業名はサイトのどこにも無いため、
    #   application_status/deadline_text は "-"、deadline_on は nil、client は "" で固定する。
    class Bizlink
      SITE_NAME = "ビズリンク"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://freelance.bizlink.io"

      # スキルslugは jobs-skills.xml に実在するものだけを並べている。
      # ruby と ruby-on-rails は結果が一部重複するが、URLキーの重複除去で吸収される。
      DEFAULT_SEARCH_TARGETS = [
        { skill_slug: "ruby",          category_hint: "Ruby" },
        { skill_slug: "ruby-on-rails", category_hint: "Ruby" },
        { skill_slug: "typescript",    category_hint: "TypeScript" },
        { skill_slug: "react",         category_hint: "React" }
      ].freeze

      # 1スキルあたりの取得ページ数。既定の並びは新着順（掲載日の降順）なので、
      # 先頭数ページだけ見れば新着案件を拾える（ruby 全32頁 / react 全58頁まである）。
      MAX_PAGES = 3

      CARD_SELECTOR = 'a[class^="ProjectCardOnList_ProjectCard"]'
      DATE_POSTED_SELECTOR = 'div[class^="ProjectCardOnList_datePosted"]'
      JOB_TITLE_SELECTOR = 'p[class^="ProjectCardOnList_jobTitle"]'
      LABEL_SELECTOR = 'span[class^="ProjectCardLabel_label"]'
      SALARY_SELECTOR = 'span[class^="ProjectCardOnList_salary"]'
      CURRENCY_SELECTOR = 'span[class^="ProjectCardOnList_currency"]'
      LOCATION_SELECTOR = 'div[class^="ProjectCardOnList_locationInfo"] span'
      SKILL_SELECTOR = 'li[class^="SkillSetLabel_skill"]'
      DETAIL_SECTION_SELECTOR = 'div[class^="ProjectCardDetails_section"]'
      DETAIL_SECTION_TITLE_SELECTOR = 'p[class^="ProjectCardDetails_sectionTitle"]'
      DETAIL_SECTION_CONTENT_SELECTOR = 'p[class^="ProjectCardDetails_content"]'

      # カードのhrefは案件詳細への相対パス（例 "/jobs/21187"）。
      # 一覧内に将来別種のリンクが混ざっても拾わないよう、この形だけを案件として扱う。
      JOB_PATH_RE = %r{\A/jobs/\d+\z}.freeze

      # 掲載日は "2026.08.28" の YYYY.MM.DD 固定表記。想定外の表記なら posted_on は nil にする。
      POSTED_ON_RE = /\A(\d{4})\.(\d{1,2})\.(\d{1,2})\z/.freeze

      # 詳細欄の見出しは「必須スキル」「業務内容」の2種類のみ（実データ45件で確認）。
      # HTML上の並びは必須スキルが先だが、読み手には業務内容が先のほうが分かりやすいため
      # description ではこの順に組み立てる。
      BUSINESS_CONTENT_TITLE = "業務内容"
      REQUIRED_SKILLS_TITLE = "必須スキル"

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, max_pages: MAX_PAGES)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @max_pages = max_pages
      end

      # 通信あり。スキルslugごとに一覧を1ページ目から max_pages ページ分たどる。
      # ページ単位の取得失敗は握りつぶして次のスキルへ進むが、1件も取れずに失敗だけが
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

      # 案件パスと案件名は表示・重複除去の両方に必須なので、欠けたカードは黙って捨てる
      # （セレクタが変わって全カードが欠損した場合は0件になり、呼び出し側のログで気付ける）。
      def self.build_posting(card, category_hint)
        detail_path = job_path(card)
        return nil unless detail_path

        title = squish(card.at_css(JOB_TITLE_SELECTOR)&.text)
        return nil if title.empty?

        build_posting_from_parts(card, category_hint, detail_path, title)
      end

      def self.build_posting_from_parts(card, category_hint, detail_path, title)
        currency = squish(card.at_css(CURRENCY_SELECTOR)&.text)
        skills = card.css(SKILL_SELECTOR).map { |skill| squish(skill.text) }.reject(&:empty?)
        tags = card.css(LABEL_SELECTOR).map { |label| squish(label.text) }.reject(&:empty?)
        work_location = squish(card.at_css(LOCATION_SELECTOR)&.text)

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}#{detail_path}"),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(
            build_description(detail_sections(card), skills, work_location, tags)
          ),
          category_hint: category_hint,
          reward: reward(squish(card.at_css(SALARY_SELECTOR)&.text), currency),
          work_format: work_format(currency),
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: skills,
          client: "",
          tags: tags,
          posted_on: posted_on(squish(card.at_css(DATE_POSTED_SELECTOR)&.text))
        )
      end

      def self.job_path(card)
        href = card["href"].to_s.strip
        href.match?(JOB_PATH_RE) ? href : nil
      end

      # 詳細欄を「見出し => 本文」のHashにする（"業務内容" / "必須スキル"）。
      def self.detail_sections(card)
        card.css(DETAIL_SECTION_SELECTOR).each_with_object({}) do |section, sections|
          section_title = squish(section.at_css(DETAIL_SECTION_TITLE_SELECTOR)&.text)
          content = text_with_line_breaks(section.at_css(DETAIL_SECTION_CONTENT_SELECTOR))
          sections[section_title] = content unless section_title.empty? || content.empty?
        end
      end

      # 使用技術と勤務地もdescriptionに入れる。EngineerClassifierはtitle+description+skillsを
      # 連結したテキストで判定するため、技術名とリモート可否が本文に出ていると分類精度が上がる。
      def self.build_description(sections, skills, work_location, tags)
        parts = []
        parts << "#{BUSINESS_CONTENT_TITLE}: #{sections[BUSINESS_CONTENT_TITLE]}" if sections[BUSINESS_CONTENT_TITLE]
        parts << "#{REQUIRED_SKILLS_TITLE}: #{sections[REQUIRED_SKILLS_TITLE]}" if sections[REQUIRED_SKILLS_TITLE]
        parts << "使用技術: #{skills.join(" / ")}" unless skills.empty?
        parts << "勤務地: #{work_location}" unless work_location.empty?
        parts << tags.join("・") unless tags.empty?
        parts.join(" / ")
      end

      # 単価は数値（"1,000,000"）と単位（"円／月"、／は全角U+FF0F）が別spanに分かれているので連結する。
      # EngineerClassifier.high_reward? がカンマを除いた数値を読むため、桁区切りはそのまま残す。
      def self.reward(salary, currency)
        salary.empty? ? "要確認" : "#{salary}#{currency}"
      end

      # 実データは全件「円／月」だが、将来の時間単価表記に備えて単位で分岐させる。
      def self.work_format(currency)
        if currency.include?("／時")
          "時間単価制"
        elsif currency.include?("／月")
          "月額制（業務委託）"
        else
          "業務委託（フリーランス）"
        end
      end

      def self.posted_on(date_posted)
        match = POSTED_ON_RE.match(date_posted)
        return nil unless match

        Date.new(match[1].to_i, match[2].to_i, match[3].to_i)
      end

      # 詳細欄の本文は <br /> で改行されており、そのまま .text すると
      # "運用経験4~5年以上・React/Vue..." のように行が連結して読めなくなる。
      # テキスト化の前に<br>を半角スペースのテキストノードへ置き換えて区切りを残す。
      def self.text_with_line_breaks(node)
        return "" unless node

        copied_node = node.dup
        copied_node.css("br").each do |line_break|
          line_break.replace(Nokogiri::XML::Text.new(" ", copied_node.document))
        end
        squish(copied_node.text)
      end

      def self.squish(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      private

      # 1スキルぶんのページ送り。カードが0件のページはページ終端（またはセレクタ崩れ）なので、
      # それ以上リクエストしても無駄になるため打ち切る。
      def fetch_target(target, fetch_failures)
        postings = []

        (1..@max_pages).each do |page_number|
          body = fetch_page_body(target[:skill_slug], page_number, fetch_failures)
          break if body.nil?

          page_postings = self.class.parse(body, today: @today, category_hint: target[:category_hint])
          break if page_postings.empty?

          postings.concat(page_postings)
        end

        postings
      end

      # ビズリンクは「存在しないページ番号」と「散発的なSSR失敗」の両方をHTTP 500で返す
      # （2026-09-12計測: 同一URLの再取得でも5回に1回ほど500になる）。つまり500は終端の合図とも
      # 一時障害とも区別できないため、1ページの失敗ではスキル全体・取得元全体を落とさず、
      # そのスキルのページ送りだけを打ち切って次のスキルへ進む。
      # rescueの範囲が@fetcher.getの1回だけなので、StandardErrorで受けてもパース側のバグは覆い隠さない
      # （HTTPエラーのFetchErrorだけでなく、通信層のタイムアウト・切断も同じ扱いにしたいため広く受ける）。
      def fetch_page_body(skill_slug, page_number, fetch_failures)
        get_with_single_retry(list_url(skill_slug, page_number))
      rescue FreelanceJobs::AccessBlockedError
        raise
      rescue StandardError => error
        fetch_failures << error
        FreelanceJobs.logger.warn(
          "[FreelanceJobs::Sources::Bizlink] #{error.message} このスキルのページ送りを打ち切ります"
        )
        nil
      end

      # 散発的な500は1回だけ取り直す。HttpFetcherが2回目以降のリクエスト前に
      # REQUEST_INTERVAL秒 sleep するため、ここで追加のsleepは要らない。
      # WAFのアクセス制限は取り直しても解消しないので、AccessBlockedErrorはそのまま送出する。
      def get_with_single_retry(url)
        @fetcher.get(url)
      rescue FreelanceJobs::AccessBlockedError
        raise
      rescue StandardError => error
        FreelanceJobs.logger.warn("[FreelanceJobs::Sources::Bizlink] #{error.message} 1回だけ再取得します")
        @fetcher.get(url)
      end

      def list_url(skill_slug, page_number)
        "#{BASE_URL}/jobs/skill_cate/#{skill_slug}/p/#{page_number}"
      end
    end
  end
end
