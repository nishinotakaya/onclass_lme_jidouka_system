# frozen_string_literal: true

require "nokogiri"
require "cgi"

module FreelanceJobs
  module Sources
    # テックダイレクト: キーワード検索結果ページ（section.list-job-card のカード）をHTMLパースする。
    # 完全SSR（Nuxt）で、一覧カードだけで単価・稼働・勤務地・スキル・業務内容まで揃うため、
    # 詳細ページは取得しない。
    #
    # keyword は全文あいまい一致で、たとえば keyword=Ruby でも「必須スキルはReact、尚可スキルに
    # Rubyがあるだけ」のReact案件が返ってくる（実測: 1件目 "システム移行に伴うフロントエンド
    # エンジニア（React）"）。ここで技術を絞り込もうとはせず、EngineerClassifierが
    # title/description/skillsから改めて技術判定する前提でそのまま返す。
    #
    # ページングは `p=` パラメータで、`page=` は効かない（実地確認済み）。1ページ10件固定。
    class Techdirect
      SITE_NAME = "テックダイレクト"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      # robots.txt に Crawl-delay の指定は無いが、他取得元と同じ間隔にして横並びで配慮する。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://techdirect.jp"

      DEFAULT_SEARCH_TARGETS = [
        { keyword: "Ruby", hint: "Ruby" },
        { keyword: "TypeScript", hint: "TypeScript" },
        { keyword: "React", hint: "React" }
      ].freeze

      # 1ページ10件固定。3キーワード×3ページ＝9リクエストに収め、
      # 1キーワードあたり最大30件（≒新着の数日分）だけ拾えば新着差分取りには十分なため、
      # 総件数を全部たどることはしない（実測の該当件数は Ruby 4,133件 / React 10,121件 /
      # TypeScript 9,602件で、全件たどるのは現実的でない）。
      # 範囲外のページ番号は 2026-09-19 実測で「HTTP 200・カード0件」が返る（リダイレクトや
      # 404にはならない）ため、ページ送りは page_postings.empty? の打ち切りだけで安全に止まる。
      DEFAULT_PAGES_PER_KEYWORD = 3

      CARD_SELECTOR = "section.list-job-card"
      TITLE_LINK_SELECTOR = "div.job-title h3 a"
      # 案件詳細のパスだけを対象にする（絞り込みリンク等の別形式のhrefを弾く）。
      JOB_PATH_RE = %r{\A/jobs/(\d+)\z}.freeze

      # 本文（【案件名】【内容】【必須スキル】…を1ブロックで含む）。必須・尚可スキルの技術名が
      # ここに濃く入るため、EngineerClassifierの判定精度はこのブロックを含めるかどうかで決まる。
      WORK_NOTE_SELECTOR = "div.work-note section.codeal-markdown"

      # 発注元の企業名。
      ORGANIZATION_NAME_SELECTOR = "div.job-org span.label"

      # スキルバッジ。必須スキル・尚可スキルが区別なく混在した1つの一覧なので、そのままskillsに入れる。
      SKILL_BADGE_TEXT_SELECTOR = "div.skill-container .skill-badge span"

      # 「見出し（h4.label）+ 値（div.value）」の項目一覧。実測4項目（報酬例・業務内容・稼働時間目安・
      # はたらく場所）だが、欠けているカードがあっても壊れないようHashで引く。
      REQUIREMENT_ITEM_SELECTOR = "div.job-requirements div.job-requirement-item"
      REWARD_LABEL = "報酬例"
      # job-requirements内の「業務内容」は本文ブロックとは別物で、「システム開発・運用、SES」の
      # ような案件区分の短い分類値。本文ブロック（WORK_NOTE_SELECTOR）の「業務内容」と紛らわしいため、
      # description組み立て時のラベルは「案件区分」に読み替える。
      BUSINESS_CATEGORY_LABEL = "業務内容"
      WORKING_DAYS_LABEL = "稼働時間目安"
      WORK_LOCATION_LABEL = "はたらく場所"

      # 単価は「4,700 ～ 5,000円/時」（範囲あり）と「5,900円/時」（範囲なし）の2パターン。
      # 波ダッシュは全角（U+FF5E）固定。
      HOURLY_REWARD_RE = /(?<lower>[\d,]+)(?:\s*[〜～~]\s*(?<upper>[\d,]+))?\s*円\s*\/\s*時/.freeze

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, pages_per_keyword: DEFAULT_PAGES_PER_KEYWORD)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @pages_per_keyword = pages_per_keyword
      end

      # 通信あり。キーワード×ページ数ぶん検索結果ページを取得し、URLキーで重複排除する。
      # 3キーワードの検索結果には同じ案件が混ざりうる（あいまい一致のため）ので重複排除は必須。
      def fetch
        postings_by_url = {}

        @search_targets.each do |search_target|
          collect_keyword_postings(search_target, postings_by_url)
        end

        postings_by_url.values
      end

      # キーワード1つぶんのページ送り。取得した案件を postings_by_url に積む。
      # カードが1件も取れないページに当たったら、ページ終端（またはDOM崩れ）とみなして打ち切る。
      def collect_keyword_postings(search_target, postings_by_url)
        (1..@pages_per_keyword).each do |page_number|
          body = @fetcher.get(search_url(search_target[:keyword], page_number))
          page_postings = self.class.parse(body, today: @today, category_hint: search_target[:hint])
          break if page_postings.empty?

          page_postings.each { |posting| postings_by_url[posting.url] ||= posting }
        end
      end
      private :collect_keyword_postings

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

        job_id_match = JOB_PATH_RE.match(title_link["href"].to_s.strip)
        return nil unless job_id_match

        title = squish(title_link.text)
        return nil if title.empty?

        requirement_values = requirement_values(card)

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}#{job_id_match[0]}"),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(build_description(card, requirement_values)),
          category_hint: category_hint,
          reward: reward(requirement_values),
          work_format: work_format(requirement_values),
          # 応募状況・締切・掲載日はサイト側に一覧として存在しないため固定値を入れる。
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: skills(card),
          client: client(card),
          tags: [],
          posted_on: nil
        )
      end

      # 「見出し（h4.label）=> 値（div.value）」のHash。項目ごと欠けたカードがあっても
      # Hash#[]がnilを返すだけで済むようにする。
      def self.requirement_values(card)
        card.css(REQUIREMENT_ITEM_SELECTOR).each_with_object({}) do |item, values|
          label = squish(item.at_css("h4.label")&.text)
          value = squish(item.at_css("div.value")&.text)
          values[label] = value unless label.empty? || value.empty?
        end
      end

      # 表示用の単価。「時給」を明記しないと EngineerClassifier.high_reward? が月額の閾値
      # （30万円）で判定してしまう（時給4,700円は高単価にならない）ため、Crowdworksと同じ
      # 「時給 <額>」表記に揃える。単価欄が読めないカードは「要確認」。
      def self.reward(requirement_values)
        match = HOURLY_REWARD_RE.match(requirement_values[REWARD_LABEL].to_s)
        return "要確認" unless match

        amounts = [match[:lower], match[:upper]].compact
        "時給 #{amounts.join("〜")}円"
      end

      # シートの「形式」列に出る値。他サイトと同じ課金・契約形態の語彙に揃える
      # （ここに「週4日/週5日」のような稼働日数を入れると、同じ列で意味が混ざる）。
      # テックダイレクトは報酬が時給建てなので、単価が読めたものは時間単価制として扱う。
      # 稼働日数は build_description の「稼働: 」に残してあり、EngineerClassifier の
      # LONG_TERM_RE（週\d日）はそちらを見る。
      def self.work_format(requirement_values)
        return "時間単価制" if HOURLY_REWARD_RE.match?(requirement_values[REWARD_LABEL].to_s)

        "業務委託（フリーランス）"
      end

      def self.client(card)
        squish(card.at_css(ORGANIZATION_NAME_SELECTOR)&.text)
      end

      # 必須スキル・尚可スキルの区別なく1つのバッジ一覧として出るので、そのままskillsに入れる。
      def self.skills(card)
        card.css(SKILL_BADGE_TEXT_SELECTOR).map { |span| squish(span.text) }.reject(&:empty?)
      end

      # 「本文ブロック / 案件区分 / 稼働 / 勤務地」の順に連結する。稼働・勤務地はwork_formatに
      # 入れた値と重複するが、EngineerClassifierのLONG_TERM_RE（週\d日）・REMOTE_RE（リモート）は
      # descriptionしか見ないため、判定材料として改めてここにも入れる。
      # 単価の生テキストは入れない（Classifier::SUSPICIOUS_REの「月N万」への誤爆を避けるため）。
      def self.build_description(card, requirement_values)
        parts = []
        work_note = squish(card.at_css(WORK_NOTE_SELECTOR)&.text)
        parts << "業務内容: #{work_note}" unless work_note.empty?
        parts << "案件区分: #{requirement_values[BUSINESS_CATEGORY_LABEL]}" if requirement_values[BUSINESS_CATEGORY_LABEL]
        parts << "稼働: #{requirement_values[WORKING_DAYS_LABEL]}" if requirement_values[WORKING_DAYS_LABEL]
        parts << "勤務地: #{requirement_values[WORK_LOCATION_LABEL]}" if requirement_values[WORK_LOCATION_LABEL]
        parts.join(" / ")
      end

      def self.squish(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      # 一覧URL。実際に確認したURL（https://techdirect.jp/jobs?keyword=Ruby&p=1）に合わせ、
      # 1ページ目から常に p=<N> を付ける（page=は効かない）。
      def search_url(keyword, page_number)
        "#{BASE_URL}/jobs?keyword=#{CGI.escape(keyword)}&p=#{page_number}"
      end
      private :search_url
    end
  end
end
