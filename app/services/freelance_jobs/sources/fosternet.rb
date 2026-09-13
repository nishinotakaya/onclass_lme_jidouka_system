# frozen_string_literal: true

require "nokogiri"
require "cgi"

module FreelanceJobs
  module Sources
    # フォスターフリーランス: キーワード検索一覧（/projects/list/?search_word=<語>）をHTMLパースする。
    # 完全SSRで、1ページ20件ぶんのカード（div.c-jobitem）に案件本文の全文まで含まれている。
    #
    # 注意1: 一覧ページに JSON-LD は無い（script[type="application/ld+json"] は0個）。
    #   JobPosting の JSON-LD を持つのは詳細ページ（/projects/detail/J41086）だけだが、
    #   その description は一覧カードの .c-jobitem__body と同一の全文だった（2026-09-12 実測）。
    #   一覧だけで本文・技術スタックが揃うため、案件数ぶんの詳細リクエストは行わない。
    # 注意2: 一覧には掲載日が一切無いため posted_on は nil 固定。
    #   新着かどうかの代替シグナルは tags の "NEW"（詳細ページの datePosted を取るには
    #   案件数ぶんのリクエストが増えるので、一覧のみの設計では取りにいかない）。
    # 注意3: 応募締切・発注企業名はサイトのどこにも無い
    #   （詳細ページの hiringOrganization も "社名非公開" 固定だった）。
    #   deadline_text は "-"、deadline_on は nil、client は "" で固定する。
    #   応募状況の専用欄も無いが、募集を終えた案件は本文末尾に
    #   "※※こちらの案件は現在募集を終了しております※※" と書かれたまま一覧に残り続ける
    #   （実測120カード中26件＝約3割）。この文言だけを application_status に反映し、
    #   文言が無い案件は「募集中と断定できない」ため "-" のままにする。
    # 注意4: レスポンスは Set-Cookie: PHPSESSID を返すが、Cookieを送り返さなくても全ページ200で返る。
    #   WAFチャレンジ（x-amzn-waf-action）もCAPTCHAも無いため、HttpFetcher をそのまま使える。
    class Fosternet
      SITE_NAME = "フォスターフリーランス"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://freelance.fosternet.jp"

      # search_word は実際に絞り込みが効くことを実測で確認済み
      # （Ruby 97件 / TypeScript 304件 / React 302件。1頁目どうしのID重複は0〜2件で結果集合は別物）。
      DEFAULT_SEARCH_TARGETS = [
        { keyword: "Ruby", hint: "Ruby" },
        { keyword: "TypeScript", hint: "TypeScript" },
        { keyword: "React", hint: "React" }
      ].freeze

      # 1キーワードあたりの取得ページ数。1ページ20件・既定の並びは新着順なので、
      # 先頭2ページ（40件）で新着は十分拾える（3キーワード×2頁＝6リクエスト）。
      DEFAULT_PAGE_COUNT = 2

      # 募集終了の告知文。本文末尾に "※※こちらの案件は現在募集を終了しております※※" の形で入る
      # （末尾にゼロ幅スペースが付くことがあるため、飾りの※には一致条件を置かない）。
      # 実測120カードでこの言い回し以外の終了表現は出現しなかった。
      CLOSED_NOTICE_RE = /募集を終了しております/.freeze
      CLOSED_STATUS = "募集終了"
      # 募集中である旨はサイトのどこにも書かれていないため、終了告知が無い案件は
      # 「募集中」と断定せず不明（"-"）として扱う。
      UNKNOWN_STATUS = "-"

      # カードは必ず一覧コンテナ .l-joblist の中にある。実測では一覧外に .c-jobitem は
      # 無かったが、将来おすすめ枠などが増えたときに拾わないよう .l-joblist で絞る。
      CARD_SELECTOR = ".l-joblist div.c-jobitem"
      # カード直下の子divは常にちょうど4つ（title / tags / side / body）。
      TITLE_LINK_SELECTOR = ".c-jobitem__title h2 a"
      SIDE_DEFINITION_SELECTOR = ".c-jobitem__side dl"
      TAG_LINK_SELECTOR = ".c-jobitem__tags a"
      BODY_SELECTOR = ".c-jobitem__body"

      # 案件IDは数値ではなく "J" + 英数字（例 /projects/detail/J41086）。
      # 同じhrefがカード下部の「詳細を見る」ボタンにも出るため、hrefは必ず
      # .c-jobitem__title 配下のリンクから取り、この形のものだけを案件として扱う。
      JOB_PATH_RE = %r{\A/projects/detail/[A-Za-z0-9]+\z}.freeze

      # .c-jobitem__side の dl は dt/dd 1組がちょうど2つ（実測100カード）。
      # dtの実値は "単価/月" と "勤務地" の2種類だけだったが、並び順に依存しないよう
      # index ではなく dt のラベル文字列で引く。
      UNIT_PRICE_LABEL_KEYWORD = "単価"
      WORK_LOCATION_LABEL_KEYWORD = "勤務地"
      EMPTY_DEFINITION = ["", ""].freeze

      # 単価ddの実値は100カードすべて "N～N万円" 形式（区切りはサイト側のU+FF5E "～"）。
      # 出力の範囲区切りはレバテックの表記に合わせてU+301C "〜" にする。
      RANGE_SEPARATOR = "〜"
      AMOUNT_RE = /\d+(?:\.\d+)?/.freeze
      MAN_UNIT = 10_000

      # 単価dtのラベル（"単価/月" / "単価/時"）から契約単位を判定した結果に対応する表記。
      # 実測100カードはすべて "単価/月" で "単価/時" は一度も出現しなかったが、
      # サイトが時間単価の案件を持つ可能性があるためラベルで分岐しておく。
      REWARD_UNIT_SUFFIXES = { month: "円／月", hour: "円／時", unknown: "円" }.freeze
      WORK_FORMATS = {
        month: "月額制（業務委託）",
        hour: "時間単価制",
        unknown: "業務委託（フリーランス）"
      }.freeze

      # 一覧HTMLには使用技術の専用欄が無いため、本文から技術名を抜き出す。本文の技術欄は2形式ある。
      # 形式A（インライン）: "・バックエンド：Python（Django）" / "言語：JavaScript、Ruby、SQL、HTML、CSS"
      SKILL_LABEL_RE = /\A[・■◆\-\s]*(?:現在の)?(?:開発)?(?:言語|フレームワーク|インフラ(?:基盤)?|DB|データベース|ミドルウェア|バックエンド|フロントエンド|サーバ[ー]?(?:構成|サイド)?|コンテナ技術|(?:開発|AI|その他)?ツール|技術スタック|モニタリング|CI\/CD|使用技術|環境)\s*[:：]\s*(.+)\z/.freeze

      # 形式B（見出し＋ぶら下がり）: 見出し行の下に技術名の行が続く。実測120カードでは
      # 見出しは "開発環境"37 / "技術環境"17 / "環境"12 / "技術スタック"3 ... と形式Bが約半数を占め、
      # 形式Aだけを見ていると120カード中52件のskillsが空になっていた。
      #   【技術環境】
      #   Cursor, Claude, Dart, Terraform, TypeScript, Go, Ruby
      #   【稼働日数】            ← 別の見出しが来たら技術欄は終わり
      # 見出し行＝行頭が括弧で始まりその括弧が閉じる行。閉じ括弧の後ろに値が続くこともある
      # （"【 環境 】Ruby(RoR)、RDB、NoSQL" のような1行完結形）ので残りも一緒に拾う。
      SECTION_HEADING_RE = /\A[【〈<＜\[［]\s*([^】〉>＞\]］]{1,20}?)\s*[】〉>＞\]］]\s*(.*)\z/.freeze
      # 見出しが技術欄かどうか。ここに当たらない見出し（勤務時間・業務内容・服装・備考など）が
      # 来た時点で技術欄を閉じる。空行でも閉じる（本文は空行でセクションが区切られている）。
      TECHNOLOGY_SECTION_RE = /環境|技術|スキル|言語|フレームワーク|インフラ|ミドルウェア|クラウド|ツール|フロント|バックエンド|サーバ|データベース|DB/.freeze

      # 括弧とコロンを区切りに含めるのが肝。"Python（Django）" → ["Python", "Django"]、
      # "AWS (ECS, EC2, RDS/Aurora)" → ["AWS", "ECS", "EC2", "RDS", "Aurora"]、
      # 技術欄の中の "Backend: Kotlin" / "FW：React" → ["Backend", "Kotlin"] と分解できる
      # （左側の役割ラベルは SKILL_STOP_WORDS で落とす）。
      SKILL_SEPARATOR_RE = /[、,／\/・:：]|[（(]|[）)]/.freeze
      # トークン前後の日本語を削って技術名だけ残す。"SQL系データベース" → "SQL"、
      # "PostgreSQLなど" → "PostgreSQL"、"一部GCP" → "GCP"。
      # 行末に句点が付く場合があるため "。" と "、" も日本語ランに含める（"Oracle。" 対策）。
      JAPANESE_RUN_RE = /[ぁ-んァ-ヶ一-龥ー。、]+/.freeze
      # 行頭の箇条書き記号・全角スペース（"　　‐Backend: Kotlin"）と行末の空白を削る。
      # String#strip は全角スペースを落とさないため、ここで明示的に対象へ含める。
      SKILL_BULLET_RE = /\A[・■◆\-‐–—\s　]+|[\s　]+\z/.freeze
      # 列挙の末尾に付く "etc" を削る（"Elasticsearch/OpenSearch etc.." 対策）。
      TRAILING_ETC_RE = /[\s　]+etc\.*\z/i.freeze
      # 技術名ではない語。コロン分割で左側に現れる役割ラベル（"FW"／"Backend"）と、
      # 単独では意味を成さない略語を落とす。大小文字は区別しない。
      SKILL_STOP_WORDS = [
        "etc", "AI", "他", "FW", "DB", "OS", "PC", "PC Web", "BFF", "KVS", "LLM",
        "IDE", "CI", "CD", "QA", "UI", "Backend", "Frontend", "Mobile App"
      ].freeze
      # 技術名は必ず英字を含み、内側に日本語を残さない（"4日～週5日※" のような
      # 非技術トークンが技術欄に紛れ込むのを防ぐ）。
      ASCII_LETTER_RE = /[A-Za-z]/.freeze
      JAPANESE_CHAR_RE = /[ぁ-んァ-ヶ一-龥]/.freeze
      # "v20" / "18" のようなバージョン番号だけのトークンは技術名として扱わない。
      VERSION_ONLY_RE = /\A[vV]?\d+(?:\.\d+)*\z/.freeze
      SKILL_MIN_LENGTH = 2
      SKILL_MAX_LENGTH = 24

      # include_closed: 募集終了の案件をシートに載せるか。既定はfalse（応募できない案件で
      # シートが埋まるのを避ける）。application_status には両方の値が入るので、残したい場合は
      # trueを渡せばよい。
      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS,
                     page_count: DEFAULT_PAGE_COUNT, include_closed: false)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @page_count = page_count
        @include_closed = include_closed
      end

      # 通信あり。キーワード×ページ数ぶんの一覧を取得する。
      # 同じ案件が複数キーワードに出る（実測でRuby×Reactに2件の重複）ため、URLキーで重複除去する。
      def fetch
        postings = {}

        @search_targets.each do |target|
          1.upto(@page_count) do |page_number|
            body = @fetcher.get(list_url(target[:keyword], page_number))
            self.class.parse(body, today: @today, category_hint: target[:hint]).each do |posting|
              postings[posting.url] ||= posting
            end
          end
        end

        adopted_postings(postings.values)
      end

      # 募集終了を採用するかどうかの方針はここだけが持つ（parseはページの写しに徹する）。
      def adopted_postings(postings)
        return postings if @include_closed

        postings.reject { |posting| self.class.recruitment_closed?(posting) }
      end
      private :adopted_postings

      # 通信なし（テスト用）。一覧1ページ分のHTML本文から案件一覧を作る。
      # 募集終了の案件も application_status: "募集終了" として**そのまま返す**（ページの写しに徹する）。
      # 採用するかどうかの方針は fetch の include_closed 側で判断する。
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

      # カード1件をJobPostingにする。案件URLとタイトルが取れないカードは、
      # セレクタ変更時に誤った空データを混ぜないよう黙って除外する（nilを返す）。
      def self.build_posting(card, category_hint)
        title_link = card.at_css(TITLE_LINK_SELECTOR)
        return nil unless title_link

        href = title_link["href"]
        return nil unless href&.match?(JOB_PATH_RE)

        title = title_link.text.gsub(/\s+/, " ").strip
        return nil if title.empty?

        side_definitions = side_definitions(card)
        unit_price_label, unit_price_text = find_definition(side_definitions, UNIT_PRICE_LABEL_KEYWORD)
        unit_price_kind = price_unit(unit_price_label)
        # 勤務地ddは空文字のことがある（実測: TypeScript1頁目 3/20件）。
        work_location = find_definition(side_definitions, WORK_LOCATION_LABEL_KEYWORD).last
        tags = build_tags(card)
        body_text = card.at_css(BODY_SELECTOR)&.text.to_s

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}#{href}"),
          title: title,
          description: build_description(body_text, work_location, tags),
          category_hint: category_hint,
          reward: build_reward(unit_price_text, unit_price_kind),
          work_format: WORK_FORMATS.fetch(unit_price_kind),
          application_status: application_status(body_text),
          deadline_text: "-",
          deadline_on: nil,
          skills: extract_skills(body_text),
          client: "",
          tags: tags,
          posted_on: nil
        )
      end

      # .c-jobitem__side の dl を [dtラベル, dd値] の配列にする（dtが空のdlは捨てる）。
      def self.side_definitions(card)
        card.css(SIDE_DEFINITION_SELECTOR).filter_map do |definition_list|
          label = definition_list.at_css("dt")&.text.to_s.strip
          next if label.empty?

          [label, definition_list.at_css("dd")&.text.to_s.strip]
        end
      end

      # 本文末尾の募集終了告知を応募状況にする。告知が無くても「募集中」とは書かれていないため
      # 断定せず "-"（不明）を返す。
      def self.application_status(body_text)
        body_text.match?(CLOSED_NOTICE_RE) ? CLOSED_STATUS : UNKNOWN_STATUS
      end

      def self.recruitment_closed?(posting)
        posting.application_status == CLOSED_STATUS
      end

      # ラベルに keyword を含む定義を返す。無ければ ["", ""]（後段は "要確認" / 空扱いになる）。
      def self.find_definition(side_definitions, label_keyword)
        side_definitions.find { |label, _value| label.include?(label_keyword) } || EMPTY_DEFINITION
      end

      # 単価表示を作る。生値 "85～95万円" をそのまま入れてはいけない:
      # FreelanceJobs::Classifier.first_reward_amount の MAN_UNIT_RE は "85～95万円" から
      # 先頭の "85" を素の数値として拾うため、高単価判定（>=300,000円）が常にfalseになる。
      # 万表記を円に展開して "850,000〜950,000円／月" の形にすることで正しく数値を拾わせる。
      def self.build_reward(unit_price_text, unit_price_kind)
        # 桁区切りカンマが入った表記でも数値が割れないよう、先にカンマを落としてから数値を拾う。
        amount_texts = unit_price_text.delete(",").scan(AMOUNT_RE)
        return "要確認" if amount_texts.empty?

        # 「万」表記のときだけ×10,000する。将来サイトが "6,000円／時" のような円単位の
        # 表記を出したときに桁を誤って膨らませないため、単位の有無で分岐する。
        multiplier = unit_price_text.include?("万") ? MAN_UNIT : 1
        amounts = amount_texts.map { |amount_text| (amount_text.to_f * multiplier).round }

        amounts.map { |amount| with_thousands_separator(amount) }
               .join(RANGE_SEPARATOR) + REWARD_UNIT_SUFFIXES.fetch(unit_price_kind)
      end

      def self.with_thousands_separator(amount)
        amount.to_s.gsub(/(\d)(?=(?:\d{3})+\z)/, '\1,')
      end

      # 契約単位は dd（金額）ではなく dt のラベル文字列で判定する。
      def self.price_unit(unit_price_label)
        return :hour if unit_price_label.include?("時")
        return :month if unit_price_label.include?("月")

        :unknown
      end

      # タグはテキストだけを使う。href は "NEW" タグが "/projects/list/" を指すダミーなので見ない。
      # 実測で出現した値は "NEW" / "フルリモート" / "原則リモート" / "一部リモート" / "オンサイト" の5種類。
      def self.build_tags(card)
        card.css(TAG_LINK_SELECTOR).map { |tag_link| tag_link.text.strip }.reject(&:empty?)
      end

      # 本文の先頭に勤務地と働き方（タグ）を足す。
      # EngineerClassifier は title + description + skills しか見ず tags フィールドを見ないため、
      # ここで混ぜておかないと REMOTE_RE（リモート可の加点）が効かない。
      def self.build_description(body_text, work_location, tags)
        parts = []
        parts << "勤務地: #{work_location}" unless work_location.empty?
        parts << "働き方: #{tags.join("・")}" unless tags.empty?
        parts << body_text

        FreelanceJobs::JobPosting.normalize_description(parts.join(" / "))
      end

      # 本文から技術名を抜き出す。インライン形式（SKILL_LABEL_RE）と、技術欄の見出しに
      # ぶら下がる形式（SECTION_HEADING_RE）の両方を1回の走査で拾う。
      # 技術欄が散文で書かれている案件は空配列になるが、技術名はdescriptionに載っているため
      # 分類には影響しない（実データ120件で分類不能は0件）。
      def self.extract_skills(body_text)
        inside_technology_section = false

        skills = body_text.lines.flat_map do |line|
          stripped_line = line.strip

          # 空行はセクションの区切り。技術欄の続きが後続の散文へ流れ込むのを防ぐ。
          if stripped_line.empty?
            inside_technology_section = false
            next []
          end

          heading = stripped_line.match(SECTION_HEADING_RE)
          if heading
            heading_label, inline_value = heading.captures
            inside_technology_section = heading_label.match?(TECHNOLOGY_SECTION_RE)
            next inside_technology_section ? split_skill_tokens(inline_value) : []
          end

          inline_label = stripped_line.match(SKILL_LABEL_RE)
          next split_skill_tokens(inline_label[1]) if inline_label
          next split_skill_tokens(stripped_line) if inside_technology_section

          []
        end

        skills.uniq
      end

      def self.split_skill_tokens(text)
        text.split(SKILL_SEPARATOR_RE).filter_map { |token| normalize_skill_token(token) }
      end

      # トークン前後の記号・日本語を削り、短すぎ・長すぎ・技術名ではない語を捨てる。
      def self.normalize_skill_token(token)
        skill = token.gsub(SKILL_BULLET_RE, "").sub(TRAILING_ETC_RE, "")
                     .sub(/\A#{JAPANESE_RUN_RE}/, "").sub(/#{JAPANESE_RUN_RE}\z/, "").strip
        return nil unless skill.length.between?(SKILL_MIN_LENGTH, SKILL_MAX_LENGTH)
        return nil unless skill.match?(ASCII_LETTER_RE)
        return nil if skill.match?(JAPANESE_CHAR_RE)
        return nil if skill.match?(VERSION_ONLY_RE)
        return nil if SKILL_STOP_WORDS.any? { |stop_word| stop_word.casecmp?(skill) }

        skill
      end

      # ページャのURL形式はサイト自身が出力しているリンクに合わせる。
      #   1頁目: /projects/list/?search_word=Ruby
      #   2頁目: /projects/list/page:2?search_word=Ruby
      # page:N の直後にスラッシュを挟まず "?" を続ける点に注意（page:1 の形はサイトが出力しない）。
      def list_url(keyword, page_number)
        path = page_number <= 1 ? "/projects/list/" : "/projects/list/page:#{page_number}"
        "#{BASE_URL}#{path}?search_word=#{CGI.escape(keyword)}"
      end
      private :list_url
    end
  end
end
