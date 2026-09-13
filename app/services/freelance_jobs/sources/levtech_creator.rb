# frozen_string_literal: true

require "nokogiri"
require "cgi"

module FreelanceJobs
  module Sources
    # レバテッククリエイター: キーワード検索結果ページ（li.projectCard）をHTMLパースする。
    #
    # 姉妹サイトのレバテックフリーランス(Sources::Levtech)とURL形式（/project/search/・/project/detail/{id}/）は
    # 同じだが、DOM構造は別物（Nuxt製SSRの出力）のため共通化はせず独立した実装にしている。
    #   freelance.levtech.jp : article.projectCard / h3.nameGroup span.name / ul.summaryList li.item / dl.tableItem
    #   creator.levtech.jp   : li.projectCard      / h3.projectNameWrapper a.projectName / ul.summaryArea li.summaryList / dl.detailList
    # window.__NUXT__ には環境設定しか入っておらず案件データのJSONは持たないため、DOMパース一択。
    # 各要素には data-v-xxxxxxxx のスコープ属性が付くが、ビルドごとに変わるためセレクタには使わない。
    #
    # 掲載日・締切・応募数・発注企業名は一覧にも詳細ページにも存在しないため、
    # 表示用フィールドは "-"、日付は nil で埋める（詳細ページは取得しない）。
    class LevtechCreator
      SITE_NAME = "レバテッククリエイター"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://creator.levtech.jp"

      DEFAULT_SEARCH_TARGETS = [
        { keyword: "Ruby", hint: "Ruby" },
        { keyword: "TypeScript", hint: "TypeScript" },
        { keyword: "React", hint: "React" }
      ].freeze

      # 案件詳細のパス。カードのフッタにも同じURLを指す a.linkButton があるため、
      # 案件名のアンカー(a.projectName)から取れたhrefだけをこのパターンで検証する。
      JOB_DETAIL_PATH_RE = %r{/project/detail/\d+}

      # 職種の p.summaryText にだけ付くクラス（リンクを横並びにするための修飾クラス）。
      OCCUPATION_CLASS_NAME = "-flex"

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
      end

      # 通信あり。キーワードごとに検索結果の1ページ目のみを取得する（3キーワード = 3リクエスト）。
      # キーワード間で結果集合が重なるため、URLキーで重複排除する。
      def fetch
        postings = {}

        @search_targets.each do |target|
          body = @fetcher.get(search_url(target[:keyword]))
          self.class.parse(body, today: @today, category_hint: target[:hint]).each do |posting|
            postings[posting.url] ||= posting
          end
        end

        postings.values
      end

      # 通信なし（テスト用）。検索結果1ページ分のHTML本文から案件一覧を作る。
      def self.parse(body, today:, category_hint: nil)
        document = Nokogiri::HTML(body)
        postings = {}

        document.css("li.projectCard").each do |card|
          posting = build_posting(card, category_hint)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      def self.build_posting(card, category_hint)
        job_name_link = card.at_css("a.projectName")
        href = job_name_link&.[]("href")
        # 案件名リンクが無い／詳細URLでないカードはURLもタイトルも作れないので黙って除外する。
        # サイト改修でセレクタが外れた場合もここで0件になるため、テストで件数を固定して検知する。
        return nil unless href&.match?(JOB_DETAIL_PATH_RE)

        summary_texts = extract_summary_texts(card)
        detail_table = extract_detail_table(card)
        tool_languages = extract_tool_languages(card)
        reward_text = summary_texts[:reward]

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}#{href}"),
          # 親のh3全体を読むとアンカー外の定型接尾辞「の求人・案件」が混ざるため、アンカーに限定する。
          title: job_name_link.text.gsub(/\s+/, " ").strip,
          description: FreelanceJobs::JobPosting.normalize_description(
            build_description(summary_texts, detail_table, tool_languages, extract_business_comment(card))
          ),
          category_hint: category_hint,
          reward: reward_text.empty? ? "要確認" : reward_text,
          work_format: work_format(reward_text),
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: tool_languages,
          client: "",
          tags: build_tags(summary_texts[:occupation]),
          posted_on: nil
        )
      end

      # ul.summaryArea の各項目を「内容」で振り分ける。
      # 実際の並びは [単価, 契約形態, 勤務地, 職種] だが、職種のliごと欠けるカードが実在するため
      # インデックス固定にはせず、単価の単位表記・業務委託の語・-flexクラスで判別する。
      def self.extract_summary_texts(card)
        summary_texts = { reward: "", contract: "", location: "", occupation: "" }

        card.css("ul.summaryArea > li.summaryList").each do |summary_item|
          paragraph = summary_item.at_css("p.summaryText")
          next unless paragraph

          text = paragraph.text.gsub(/\s+/, " ").strip
          next if text.empty?

          key = summary_key(paragraph["class"].to_s, text)
          # 同じ種類が2つ現れた場合は先頭を採用する（想定外のDOM変化で上書きされないようにする）。
          summary_texts[key] = text if summary_texts[key].empty?
        end

        summary_texts
      end

      # p.summaryText 1つがどの項目かを、クラス名と本文から決める。
      def self.summary_key(class_names, text)
        # 職種だけがリンク列（a.blackLink）を横並びにする -flex 付き。
        return :occupation if class_names.split.include?(OCCUPATION_CLASS_NAME)
        # 単価は必ず「〜5,180円／時」「〜900,000円／月」のように全角スラッシュ付きの単位を伴う。
        return :reward if text.include?("／時") || text.include?("／月")
        return :contract if text.include?("業務委託")

        :location
      end

      # dl.detailList を「dt.head => dd.data」のHashにする（作業内容／求めるスキル／ツール・言語）。
      # dd.data の末尾はサイト側の仕様で "..." に省略されているが、そのまま使う。
      def self.extract_detail_table(card)
        card.css("dl.detailList").each_with_object({}) do |definition_list, detail_table|
          head = definition_list.at_css("dt.head")&.text&.strip
          data = definition_list.at_css("dd.data")&.text&.gsub(/\s+/, " ")&.strip
          detail_table[head] = data if head && data && !data.empty?
        end
      end

      # 「ツール・言語」のddはリンク列で、区切りのカンマはVueのフラグメント出力（<!--[-->,<!--]-->）。
      # テキストをsplitするより a.blackLink を直接拾うほうが堅いのでアンカーから取る。
      # ツール・言語のdl自体が無いカードが実在する（実測 43件中3件）ため、無ければ空配列を返す。
      def self.extract_tool_languages(card)
        tool_definition_list = card.css("dl.detailList").find do |definition_list|
          definition_list.at_css("dt.head")&.text&.strip == "ツール・言語"
        end
        return [] unless tool_definition_list

        tool_definition_list.css("dd.data a.blackLink").map { |anchor| anchor.text.strip }.reject(&:empty?)
      end

      def self.extract_business_comment(card)
        card.at_css("p.businessComment")&.text&.gsub(/\s+/, " ")&.strip || ""
      end

      # EngineerClassifier は title + description + skills の連結テキストで判定するため、
      # 一覧に出ている情報はできるだけ description に集める。
      # 営業コメント(p.businessComment)は「リモートでの作業が可能でございます」等、一覧で唯一
      # リモート勤務に言及する箇所なので必ず含める（勤務地テキストにはリモート表記が一切無い）。
      def self.build_description(summary_texts, detail_table, tool_languages, business_comment)
        parts = []
        parts << "募集職種: #{summary_texts[:occupation]}" unless summary_texts[:occupation].empty?
        parts << "作業内容: #{detail_table["作業内容"]}" if detail_table["作業内容"]
        parts << "求めるスキル: #{detail_table["求めるスキル"]}" if detail_table["求めるスキル"]
        parts << "ツール・言語: #{tool_languages.join(" / ")}" unless tool_languages.empty?
        parts << "勤務地: #{summary_texts[:location]}" unless summary_texts[:location].empty?
        parts << business_comment unless business_comment.empty?
        parts.join(" / ")
      end

      # 一覧には featureList / statusLabel（"New" "リモートOK" 等）が存在しないため、
      # タグ欄は職種で代用する。複数職種はカンマ連結（"フロントエンドエンジニア,HTMLコーダー"）で出る。
      def self.build_tags(occupation_text)
        occupation_text.split(",").map(&:strip).reject(&:empty?)
      end

      def self.work_format(reward_text)
        if reward_text.include?("／時")
          "時間単価制"
        elsif reward_text.include?("／月")
          "月額制（業務委託）"
        else
          "業務委託（フリーランス）"
        end
      end

      # 1ページ目はクエリのみ（2ページ目以降は /project/search/p2/?keyword=... 形式だが、
      # 1キーワードあたり1ページ（20件）で十分なため、ページングは行わない）。
      def search_url(keyword)
        "#{BASE_URL}/project/search/?keyword=#{CGI.escape(keyword)}"
      end
      private :search_url
    end
  end
end
