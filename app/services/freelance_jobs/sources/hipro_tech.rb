# frozen_string_literal: true

require "nokogiri"
require "date"

module FreelanceJobs
  module Sources
    # HiPro Tech（パーソルキャリア運営）: 一覧ページ /job-search のカード（div.job-card）を
    # HTMLパースする。Drupal 9 の完全SSR出力で、カード1枚に単価・言語・フレームワーク・
    # 稼働頻度・こだわり条件まで揃っているため、詳細ページ(/job/{id})は取得しない
    # （1ページ20件につき20リクエスト増える割に、追加で得られるのは募集背景の全文だけで、
    #   分類に効く技術名は一覧のスキル欄で既に取れているため）。
    #
    # ★検索は全文検索(?keys=)ではなくタクソノミー絞り込み(?pg_skill_id[n]=n / ?framework_id[n]=n)を使う。
    #   実測では keys=Ruby の20件中8件が Go/Python 等の無関係案件（本文のどこかに "Ruby" の
    #   文字があるだけ）で、EngineerClassifier の category_hint フォールバックに拾われて
    #   Rubyシートへ混入した。タクソノミー絞り込みは80カード全件がスキル欄に該当技術を持ち
    #   誤分類ゼロだったため、こちらを既定にしている。
    class HiproTech
      SITE_NAME = "HiPro Tech"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://tech.hipro-job.jp"
      LIST_PATH = "/job-search"

      # 絞り込みクエリは「?」以降の文字列をそのまま持つ。`pg_skill_id[9]=9` の角括弧は
      # パーセントエンコード済みの形（%5B / %5D）で書く（生の角括弧は未検証のため使わない）。
      # IDの出どころは /job-search の絞り込みフォームの input name
      # （name="pg_skill_id[<id>]" / name="framework_id[<id>]"）で、壊れたら同フォームから再導出できる。
      #   pg_skill_id : 9=Ruby / 47=TypeScript / 18=JavaScript / 6=Python / 2=Java / 4=PHP / 11=Go言語
      #   framework_id: 125=React / 226=Next.js / 26=ReactNative / 5=Ruby on Rails / 126=Vue.js
      # 実測の該当件数は Ruby 202件 / TypeScript 732件 / React 580件。
      DEFAULT_SEARCH_TARGETS = [
        { query: "pg_skill_id%5B9%5D=9", hint: "Ruby" },
        { query: "pg_skill_id%5B47%5D=47", hint: "TypeScript" },
        { query: "framework_id%5B125%5D=125", hint: "React" }
      ].freeze

      # 1ページ20件固定。既定の並び順は「募集中が先頭 → 掲載日降順」なので、募集中案件は
      # 実質1ページ目に収まる（実測: Ruby 1件・TypeScript 10件・React 12件がいずれも1ページ目）。
      # それでも2ページ目まで取るのは、募集中が20件を超えて増えたときの取りこぼし防止
      # （＝安全側の上限）。3絞り込み×2ページ＝6リクエスト。
      DEFAULT_PAGES = 2

      # 一覧カード。実クラス属性は
      # "node node--type-job job-details job list clearfix job-card"。
      CARD_SELECTOR = "div.job-card"
      # 案件URLはカードの about 属性（例 "/job/51779"）。カード内の a[href^="/job/"] も同値だが
      # 1カードに1〜2本あるため、1つしかない about 属性を正とする。
      # 詳細ページにも div.job-card が1個現れるが、そちらは about を持たないのでこの形で弾ける。
      JOB_PATH_RE = %r{\A/job/\d+\z}

      TITLE_SELECTOR = "span.job-list-title"
      # "2026年03月03日掲載開始"。親の .tech-job-new-area 直下にはNEWバッジが将来入りうるため、
      # 必ず span.published に絞って読む。
      PUBLISHED_SELECTOR = ".tech-job-new-area span.published"
      POSTED_ON_RE = /(\d{4})年(\d{1,2})月(\d{1,2})日/
      # 単価は下限・上限が別要素（テキスト "450,000円" / content属性 "450000"）。
      SALARY_LOW_SELECTOR = ".money .field--name-field-job-salary-low"
      SALARY_HIGH_SELECTOR = ".money .field--name-field-job-salary-high"
      # 単価種別。実測360カードの内訳は 月単価336 / 時単価18 / 日単価5 / 週単価1 の4種。
      # 日単価・週単価は少数だが確実に存在するので、単位なしの金額（月額か日額か読み手に
      # 判別できない）を出さないよう SALARY_SYSTEM_UNIT_RE で必ず単位を付ける。
      SALARY_SYSTEM_SELECTOR = ".money .field--name-field-job-salary-system"
      MONTHLY_SALARY_SYSTEM = "月単価"
      HOURLY_SALARY_SYSTEM = "時単価"
      # 「◯単価」の◯をそのまま表示単位に使う（"日単価" → "／日"）。将来 "年単価" が増えても
      # 単位が落ちない。この形に当てはまらない種別（"成果報酬" 等）は単位を付けない。
      SALARY_SYSTEM_UNIT_RE = /\A(.)単価\z/
      # 案件区分は "受託サービス" / "エージェントサービス" の2値のみ（実測160カード）。
      JOB_TYPE_SELECTOR = "div.job_type"
      # 応募可否はこの要素のclassで表される。本文テキストは案件区分によって変わるためclassで見る。
      ENTRY_LINK_SELECTOR = "div.entry-link"
      OPEN_ENTRY_CLASS = "regentry"
      EXPIRED_ENTRY_CLASS = "expired"
      OPEN_STATUS = "募集中"
      EXPIRED_STATUS = "募集期間外"

      # 複数値フィールドはいずれも「コンテナのclass > .field__item」の形。コンテナはカード内に1個。
      OCCUPATION_FIELD_CLASS = "field--name-field-job-occupation"        # 募集職種
      ACTIVITY_FREQUENCY_FIELD_CLASS = "field--name-field-jobex-actfreq" # 稼働頻度 "週3日" 等
      FEATURE_FIELD_CLASS = "field--name-field-job-kodawari"             # こだわり条件 "フルリモート" 等
      PROGRAMMING_LANGUAGE_FIELD_CLASS = "field--name-field-job-pg-skill"
      FRAMEWORK_FIELD_CLASS = "field--name-field-job-framework"
      INFRASTRUCTURE_FIELD_CLASS = "field--name-field-job-infrastructure"

      # カード1枚から取り出した複数値フィールドの入れ物。descriptionとtagsが同じ値を使うため、
      # DOMからの抽出を1か所に集約して持ち回る。
      CardFields = Struct.new(:occupations, :activity_frequencies, :features, :skills, :job_type, keyword_init: true)

      # include_expired: 募集期間外の案件をシートに載せるか。既定はfalse（応募できない案件で
      # シートが埋まるのを避ける）。application_status には両方の値が入るので、残したい場合は
      # trueを渡せばよい。
      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, pages: DEFAULT_PAGES,
                     include_expired: false)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @pages = pages
        @include_expired = include_expired
      end

      # 通信あり。絞り込み×ページ数ぶん一覧ページを取得し、URLキーで重複排除する。
      # 1案件が複数の絞り込み（例: Ruby と React の両方）に出るため重複排除は必須。
      def fetch
        postings = {}

        @search_targets.each do |target|
          (0...@pages).each do |page_index|
            body = @fetcher.get(list_url(target[:query], page_index))
            page_postings = self.class.parse(body, today: @today, category_hint: target[:hint])
            # カードが1件も取れないページは末尾を超えている（またはDOMが変わった）とみなし、
            # 同じ絞り込みの以降のページは取りに行かない。
            break if page_postings.empty?

            adopted_postings(page_postings).each { |posting| postings[posting.url] ||= posting }
          end
        end

        postings.values
      end

      # 1ページ分の取得結果から、シートに載せる案件だけを残す。
      def adopted_postings(page_postings)
        return page_postings if @include_expired

        page_postings.reject { |posting| self.class.recruitment_expired?(posting) }
      end
      private :adopted_postings

      # 通信なし（テスト用）。一覧1ページ分のHTML本文から案件一覧を作る。
      # 募集期間外の案件も application_status: "募集期間外" として**そのまま返す**（ページの写しに徹する）。
      # 採用するかどうかの方針は fetch の include_expired 側で判断する。
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

      # カード1枚をJobPostingに組み立てる。案件URLとタイトルのどちらかが取れないカードは
      # 一覧カードではない（または構造が変わった）と判断してnilを返し、呼び出し側で除外する。
      def self.build_posting(card, category_hint)
        job_path = card["about"].to_s.strip
        return nil unless job_path.match?(JOB_PATH_RE)

        title = squish_text(card.at_css(TITLE_SELECTOR)&.text)
        # タイトルが空のまま行を作るとシート上で案件を識別できなくなるため、ここで落とす。
        return nil if title.empty?

        card_fields = extract_card_fields(card)
        salary_system = squish_text(card.at_css(SALARY_SYSTEM_SELECTOR)&.text)

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}#{job_path}"),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(build_description(card_fields)),
          category_hint: category_hint,
          reward: build_reward(card, salary_system),
          work_format: work_format(salary_system),
          application_status: application_status(card),
          # 応募締切は一覧・詳細のどちらにも存在しないサイトなので固定値にする。
          deadline_text: "-",
          deadline_on: nil,
          skills: card_fields.skills,
          # 詳細ページのJSON-LD hiringOrganization は運営元の「パーソルキャリア株式会社」で
          # 発注企業ではないため、発注者名は取得できないものとして空にする。
          client: "",
          tags: build_tags(card_fields),
          posted_on: parse_posted_on(card)
        )
      end

      def self.extract_card_fields(card)
        CardFields.new(
          occupations: field_items(card, OCCUPATION_FIELD_CLASS),
          activity_frequencies: field_items(card, ACTIVITY_FREQUENCY_FIELD_CLASS),
          features: field_items(card, FEATURE_FIELD_CLASS),
          # 言語・フレームワーク・インフラの3欄を連結したものがEngineerClassifierの判定材料になる。
          # 技術がどの欄に登録されるかはサイト側次第（例: React・Ruby on Rails はフレームワーク欄、
          # Linux はインフラ欄）なので、必ず3欄すべてを足してから重複を除く。
          skills: (field_items(card, PROGRAMMING_LANGUAGE_FIELD_CLASS) +
                   field_items(card, FRAMEWORK_FIELD_CLASS) +
                   field_items(card, INFRASTRUCTURE_FIELD_CLASS)).uniq,
          job_type: squish_text(card.at_css(JOB_TYPE_SELECTOR)&.text)
        )
      end

      def self.field_items(card, field_class)
        card.css(".#{field_class} .field__item").map { |item| squish_text(item.text) }.reject(&:empty?)
      end

      # EngineerClassifier の判定テキストは title + description + skills だけなので、
      # 稼働頻度（"週3日"→長期・継続あり）とこだわり条件（"フルリモート"→リモート可）は
      # tagsだけでなく必ずdescriptionにも入れる。tagsは分類器に渡らない。
      def self.build_description(card_fields)
        parts = []
        parts << "募集職種: #{card_fields.occupations.join("・")}" unless card_fields.occupations.empty?
        parts << "開発環境: #{card_fields.skills.join("・")}" unless card_fields.skills.empty?
        parts << "稼働: #{card_fields.activity_frequencies.join("・")}" unless card_fields.activity_frequencies.empty?
        parts << "案件区分: #{card_fields.job_type}" unless card_fields.job_type.empty?
        parts << "特徴: #{card_fields.features.join("・")}" unless card_fields.features.empty?
        parts.join(" / ")
      end

      def self.build_tags(card_fields)
        job_type_tags = card_fields.job_type.empty? ? [] : [card_fields.job_type]
        job_type_tags + card_fields.features
      end

      # "450,000円" + "550,000円" + "月単価" → "450,000〜550,000円／月"。
      # 下限が取れないときだけ "要確認"（実測160カードでは下限・上限とも欠損ゼロ）。
      # なお単価種別と金額が食い違うカード（時単価なのに80万円など）が実在するが、サイト側の
      # 入力ミスなので補正はせずページの表記どおりに出す。
      def self.build_reward(card, salary_system)
        salary_low = squish_text(card.at_css(SALARY_LOW_SELECTOR)&.text).delete("円")
        salary_high = squish_text(card.at_css(SALARY_HIGH_SELECTOR)&.text).delete("円")
        return "要確認" if salary_low.empty?

        amount = salary_high.empty? || salary_high == salary_low ? salary_low : "#{salary_low}〜#{salary_high}"
        "#{amount}円#{salary_unit_suffix(salary_system)}"
      end

      # "月単価" → "／月"。「◯単価」の形でない未知の種別のときだけ単位を付けない
      # （誤った単位を出すより無い方がよい）。
      def self.salary_unit_suffix(salary_system)
        matched = SALARY_SYSTEM_UNIT_RE.match(salary_system)
        matched ? "／#{matched[1]}" : ""
      end

      # work_format は「時間単価制 / 月額制（業務委託） / 業務委託（フリーランス）」の3値しか
      # 取らない決まりなので、日単価・週単価と未知の種別はまとめて汎用の業務委託に寄せる
      # （単位そのものは reward 側に "／日" として残るため情報は失われない）。
      def self.work_format(salary_system)
        case salary_system
        when HOURLY_SALARY_SYSTEM then "時間単価制"
        when MONTHLY_SALARY_SYSTEM then "月額制（業務委託）"
        else "業務委託（フリーランス）"
        end
      end

      # "entry-link regentry" → 募集中 / "entry-link expired" → 募集期間外。
      # div.entry-link 自体が無いカードは判定材料が無いものとして "-" にする。
      def self.application_status(card)
        entry_link = card.at_css(ENTRY_LINK_SELECTOR)
        entry_link_class = entry_link ? entry_link["class"].to_s : ""

        return EXPIRED_STATUS if entry_link_class.include?(EXPIRED_ENTRY_CLASS)
        return OPEN_STATUS if entry_link_class.include?(OPEN_ENTRY_CLASS)

        "-"
      end

      def self.recruitment_expired?(posting)
        posting.application_status == EXPIRED_STATUS
      end

      # "2026年03月03日掲載開始" → Date。日付が読めないカードは掲載日なし(nil)として扱う。
      def self.parse_posted_on(card)
        published_text = squish_text(card.at_css(PUBLISHED_SELECTOR)&.text)
        matched = POSTED_ON_RE.match(published_text)
        return nil unless matched

        Date.new(matched[1].to_i, matched[2].to_i, matched[3].to_i)
      rescue ArgumentError
        # "2026年02月31日" のような存在しない日付が入っていた場合の保険。
        nil
      end

      # Nokogiriのtextは前後に改行・インデントが入るため、表示用の値は必ず空白を畳んでから使う。
      # ノーブレークスペースも畳めるよう [[:space:]] を使う。
      def self.squish_text(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      # "https://tech.hipro-job.jp/job-search?pg_skill_id%5B9%5D=9&page=1"。
      # page は0始まり。1ページ目は page パラメータ無しが正規URLなので省く（付けても結果は同じ）。
      def list_url(query, page_index)
        url = "#{BASE_URL}#{LIST_PATH}?#{query}"
        page_index.zero? ? url : "#{url}&page=#{page_index}"
      end
      private :list_url
    end
  end
end
