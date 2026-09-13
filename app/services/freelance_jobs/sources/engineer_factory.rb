# frozen_string_literal: true

require "nokogiri"
require "json"
require "date"

module FreelanceJobs
  module Sources
    # エンジニアファクトリー: スキル別一覧（/freelance/jobs/skill/<skill_id>?page=<ページ>）をHTMLパースする。
    # Nuxt 3 の完全SSRで、1ページ40件ぶんのカード（section.modJobBlock）が生HTMLに含まれる。
    # 掲載日と業務内容全文はカードHTMLに無く、同じページの `script#__NUXT_DATA__`（devalue形式のJSON）
    # にだけ入っているため、カードの案件IDをキーにそこから補完する。
    #
    # HTML構造の前提（壊れたらここを疑う。2026-09-13時点の実データ）:
    #   - カード: section.modJobBlock（実属性 "modJobBlock modJobBlock--large"）。Vueのscoped属性
    #     data-v-xxxx や <!--[--> コメントが混ざるがNokogiriでは無視できる
    #   - 案件URL: h3.modJobBlock__title a[href] → "/freelance/jobs/139683"。同じhrefが下部の
    #     a.modBtnSearch（詳細を見る）にもあるので、h3側だけを正とする
    #   - 案件名: 同じ a の最初のテキストノード。"〜エンジニア | " のように末尾に " | " が付き、
    #     子の <span> はSEO接尾辞「東京都の案件・求人」なので a.text をそのまま使ってはいけない
    #   - 項目: dl のうち dt の見出しが 単価 / エリア / 職種 / スキル / 業界 / 必須スキル のもの。
    #     単価は dd に "月額 62 ～ 68 万円(税抜)"（数字は <strong>、～は全角U+FF5E）、
    #     エリア・職種・スキル・業界は dd a の各テキスト、必須スキルは dd p の全文
    #     （class line-clamp-3 はCSS省略のみ）。業界の dl は無いカードがある
    #   - タグ: ul.modListTag li（長期案件 / リモート可 / フルリモート）、バッジ: span.tag-job（New / Hot）
    #   - 掲載日・応募状況・締切・発注者名はカードにもJSONにも無い（発注者は詳細でも「社名非公開」）
    #
    # __NUXT_DATA__（devalue形式）の前提:
    #   - JSON全体が1本の配列で、Hashの値・配列の要素は同じ配列への Integer インデックス参照。
    #     文字列・数値は参照先にそのまま入る。負のインデックスは undefined
    #   - 案件は {"id" => idx, "type" => idx, "attributes" => idx, ...} の Hash で、
    #     type の参照先が "freelance_job"、id の参照先が "136813" のような String
    #   - attributes の参照先 Hash に publication_date_ef（"2026-07-17"）/ duties（業務内容全文）/
    #     skills_preferred（歓迎スキル）/ nearest_stations（"飯田橋（東京都）"）がある
    #   - 補完は任意扱い: script が無い・JSONが壊れた・形式が変わったときは posted_on nil・
    #     業務内容なしでHTML分だけのJobPostingを返す（バッチは落とさない）
    class EngineerFactory
      SITE_NAME = "エンジニアファクトリー"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://www.engineer-factory.com"

      # skill_id は一覧の絞り込みリンク（a[href^="/freelance/jobs/skill/"]）の実データから確定したもの。
      # Ruby on Rails(22045) は1ページ目40件が全て Ruby(22003) の1〜2ページ目に含まれるため入れていない。
      # Next.js(22069) を React の網羅目的で足す場合は { skill_id: 22069, category_hint: "React" } を追加する
      # （EngineerClassifier の REACT_RE は Next.js も React 判定する）。
      DEFAULT_SEARCH_TARGETS = [
        { skill_id: 22003, category_hint: "Ruby" },
        { skill_id: 22013, category_hint: "TypeScript" },
        { skill_id: 22064, category_hint: "React" }
      ].freeze

      # 1スキルあたりの取得ページ数。既定の並びは新着順（created_at 降順）なので、
      # 先頭2ページ（80件）で新着を拾える（Ruby 13頁 / TypeScript 30頁 / React 7頁まである）。
      # 3スキル×2ページ＝6リクエスト（1ページ約1.4MB）。リクエスト予算40回の範囲に収める。
      MAX_PAGES = 2

      CARD_SELECTOR = "section.modJobBlock"
      TITLE_LINK_SELECTOR = "h3.modJobBlock__title a"
      TAG_SELECTOR = "ul.modListTag li"
      BADGE_SELECTOR = "span.tag-job"
      NUXT_DATA_SELECTOR = "script#__NUXT_DATA__"

      # 案件リンクのhrefは "/freelance/jobs/<数字>" の形だけを案件として扱う
      # （エリア・職種・スキル等の絞り込みリンクは /freelance/jobs/area/13 のように別形式）。
      JOB_PATH_RE = %r{\A/freelance/jobs/(\d+)\z}.freeze

      # 案件名の末尾に付く " | "（この後ろにSEO接尾辞の <span> が続く）を落とす。
      TITLE_SUFFIX_SEPARATOR_RE = /\s*\|\s*\z/.freeze

      # 単価の dd は "月額 65 ～ 70 万円(税抜)"（実データ120件中120件がこの形）。
      # 上限が省かれた "月額 65 万円" も読めるよう上限は任意にし、
      # 波ダッシュは全角チルダ（U+FF5E）・波ダッシュ（U+301C）・半角チルダのどれでも受ける。
      MONTHLY_WAGE_RE = /月額\s*(\d+)(?:\s*[～〜~]\s*(\d+))?\s*万円/.freeze

      # dl の見出し（dt の squish テキスト）。
      WAGE_LABEL = "単価"
      AREA_LABEL = "エリア"
      OCCUPATION_LABEL = "職種"
      SKILL_LABEL = "スキル"
      INDUSTRY_LABEL = "業界"
      REQUIRED_SKILLS_LABEL = "必須スキル"

      # __NUXT_DATA__ 内で案件オブジェクトを見分ける type 値と、補完に使う attributes のキー。
      NUXT_JOB_TYPE = "freelance_job"
      NUXT_PUBLICATION_DATE_KEY = "publication_date_ef"
      NUXT_DUTIES_KEY = "duties"
      NUXT_PREFERRED_SKILLS_KEY = "skills_preferred"
      NUXT_NEAREST_STATIONS_KEY = "nearest_stations"

      # devalue が値を包むラッパー配列の先頭要素（["Reactive", idx] のように第2要素が参照先）。
      NUXT_WRAPPER_TYPES = %w[ShallowReactive Reactive Ref ShallowRef].freeze

      # 掲載日は "2026-07-17" の YYYY-MM-DD 固定表記（空文字のこともある）。想定外なら posted_on は nil。
      PUBLICATION_DATE_RE = /\A\d{4}-\d{2}-\d{2}\z/.freeze

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, max_pages: MAX_PAGES)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @max_pages = max_pages
      end

      # 通信あり。スキルIDごとに一覧を1ページ目から max_pages ページ分たどる。
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
      # todayは全取得元共通のインターフェースとして受け取るが、締切の概念が無いサイトなので参照しない。
      def self.parse(body, today:, category_hint: nil)
        document = Nokogiri::HTML(body)
        nuxt_attributes_by_job_id = nuxt_job_attributes(document)
        postings = {}

        document.css(CARD_SELECTOR).each do |card|
          posting = build_posting(card, category_hint, nuxt_attributes_by_job_id)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      # 案件パスと案件名は表示・重複除去の両方に必須なので、欠けたカードは黙って捨てる
      # （セレクタが変わって全カードが欠損した場合は0件になり、呼び出し側のログで気付ける）。
      def self.build_posting(card, category_hint, nuxt_attributes_by_job_id)
        title_link = card.at_css(TITLE_LINK_SELECTOR)
        return nil unless title_link

        job_id = job_id_from_href(title_link["href"])
        return nil unless job_id

        title = title_text(title_link)
        return nil if title.empty?

        build_posting_from_parts(card, category_hint, job_id, title, nuxt_attributes_by_job_id[job_id] || {})
      end

      def self.build_posting_from_parts(card, category_hint, job_id, title, nuxt_attributes)
        definition_lists = definition_lists_by_label(card)
        skills = link_texts(definition_lists[SKILL_LABEL])
        tags = build_tags(card)
        wage_match = MONTHLY_WAGE_RE.match(squish(definition_lists[WAGE_LABEL]&.at_css("dd")&.text))

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}/freelance/jobs/#{job_id}"),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(
            build_description(definition_lists, skills, tags, nuxt_attributes)
          ),
          category_hint: category_hint,
          reward: reward(wage_match),
          work_format: work_format(wage_match),
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: skills,
          client: "",
          tags: tags,
          posted_on: posted_on(nuxt_attributes[NUXT_PUBLICATION_DATE_KEY])
        )
      end

      def self.job_id_from_href(href)
        match = JOB_PATH_RE.match(href.to_s.strip)
        match && match[1]
      end

      # 案件名リンクの最初のテキストノードだけを使う（子の <span> はSEO接尾辞「東京都の案件・求人」）。
      # テキストは "〜エンジニア | " の形で終わるので、区切りの " | " を落とす。
      def self.title_text(title_link)
        first_text_node = title_link.children.find(&:text?)
        squish(first_text_node&.text).sub(TITLE_SUFFIX_SEPARATOR_RE, "")
      end

      # カード内の dl を「dt の見出し => dl 要素」のHashにする。
      # dt には <span class="iconify"> が先頭に入るが空要素なので squish すれば見出しだけになる。
      def self.definition_lists_by_label(card)
        card.css("dl").each_with_object({}) do |definition_list, definition_lists|
          label = squish(definition_list.at_css("dt")&.text)
          definition_lists[label] ||= definition_list unless label.empty?
        end
      end

      # dd 内のリンク（エリア・職種・スキル・業界）のテキスト一覧。dl が無ければ空配列。
      def self.link_texts(definition_list)
        return [] unless definition_list

        definition_list.css("dd a").map { |link| squish(link.text) }.reject(&:empty?)
      end

      # description は EngineerClassifier の判定テキストにそのまま入るので、技術名が載る
      # 業務内容（「開発言語：Go、Java、Ruby on Rails」のような技術スタック）・使用技術を必ず入れる。
      # タグの「リモート可」「長期案件」は REMOTE_RE / LONG_TERM_RE に当たり、memoとおすすめ度に効く。
      # 単価の生テキスト「月額 65 ～ 70 万円」は入れない（Classifier::SUSPICIOUS_RE の「月N万」に誤爆する）。
      def self.build_description(definition_lists, skills, tags, nuxt_attributes)
        duties = squish(nuxt_attributes[NUXT_DUTIES_KEY])
        required_skills = squish(definition_lists[REQUIRED_SKILLS_LABEL]&.at_css("dd")&.text)
        preferred_skills = squish(nuxt_attributes[NUXT_PREFERRED_SKILLS_KEY])
        occupations = link_texts(definition_lists[OCCUPATION_LABEL])
        industries = link_texts(definition_lists[INDUSTRY_LABEL])
        work_location = work_location_text(link_texts(definition_lists[AREA_LABEL]), nuxt_attributes)

        parts = []
        parts << "業務内容: #{duties}" unless duties.empty?
        parts << "必須スキル: #{required_skills}" unless required_skills.empty?
        parts << "歓迎スキル: #{preferred_skills}" unless preferred_skills.empty?
        parts << "使用技術: #{skills.join(" / ")}" unless skills.empty?
        parts << "職種: #{occupations.join("・")}" unless occupations.empty?
        parts << "勤務地: #{work_location}" unless work_location.empty?
        parts << "業界: #{industries.join("・")}" unless industries.empty?
        parts << tags.join("・") unless tags.empty?
        parts.join(" / ")
      end

      # 勤務地は HTML のエリア（都道府県）に、JSON の最寄駅があれば「（最寄駅: 飯田橋（東京都））」を添える。
      def self.work_location_text(areas, nuxt_attributes)
        nearest_stations = squish(nuxt_attributes[NUXT_NEAREST_STATIONS_KEY])
        location = areas.join("・")
        return location if nearest_stations.empty?
        return "最寄駅: #{nearest_stations}" if location.empty?

        "#{location}（最寄駅: #{nearest_stations}）"
      end

      # バッジ（New / Hot）を先頭に、こだわりタグ（長期案件 / リモート可 / フルリモート）を続ける。
      def self.build_tags(card)
        badges = card.css(BADGE_SELECTOR).map { |badge| squish(badge.text) }.reject(&:empty?)
        tags = card.css(TAG_SELECTOR).map { |tag| squish(tag.text) }.reject(&:empty?)
        badges + tags
      end

      # 「月額 65 ～ 70 万円」を万→円に換算して "650,000〜700,000円／月" にする。
      # EngineerClassifier.high_reward? は先頭の数値（650000 ≥ 300,000）で高単価を判定する。
      # 月額表記に一致しない（時給・非公開など未観測の表記）ときは "要確認"。
      def self.reward(wage_match)
        return "要確認" unless wage_match

        amounts = wage_match.captures.compact.map { |ten_thousand_yen| ten_thousand_yen.to_i * 10_000 }
        "#{amounts.map { |amount| FreelanceJobs.format_number(amount) }.join("〜")}円／月"
      end

      # 実データは全件が月額表記。一致しなければ契約形態を断定せず「業務委託（フリーランス）」にする。
      def self.work_format(wage_match)
        wage_match ? "月額制（業務委託）" : "業務委託（フリーランス）"
      end

      def self.posted_on(publication_date)
        text = publication_date.to_s.strip
        return nil unless text.match?(PUBLICATION_DATE_RE)

        Date.strptime(text, "%Y-%m-%d")
      rescue ArgumentError
        nil
      end

      # __NUXT_DATA__ から「案件ID => attributes（String値のHash）」を作る。
      # devalue 配列を全部展開するのではなく、type が "freelance_job" の Hash だけを拾い、
      # その attributes の値を1段だけ参照解決する（必要なのは文字列4項目だけなので十分）。
      # script が無い・JSONが壊れている・形式が変わったときは空Hashを返し、補完なしで続行する。
      def self.nuxt_job_attributes(document)
        script_node = document.at_css(NUXT_DATA_SELECTOR)
        return {} unless script_node

        entries = JSON.parse(script_node.text)
        return {} unless entries.is_a?(Array)

        entries.each_with_object({}) do |entry, attributes_by_job_id|
          next unless entry.is_a?(Hash) && devalue_value(entries, entry["type"]) == NUXT_JOB_TYPE

          job_id = devalue_value(entries, entry["id"]).to_s
          attributes = devalue_value(entries, entry["attributes"])
          next if job_id.empty? || !attributes.is_a?(Hash)

          # 同じ案件が別のフェッチキー（おすすめ枠など）にも入ることがあるので先勝ちにする。
          attributes_by_job_id[job_id] ||= resolve_string_attributes(entries, attributes)
        end
      rescue JSON::ParserError
        {}
      end

      # attributes Hash の値（インデックス参照）のうち、参照先が String のものだけを解決して返す。
      def self.resolve_string_attributes(entries, attributes)
        attributes.each_with_object({}) do |(attribute_name, value_index), resolved|
          value = devalue_value(entries, value_index)
          resolved[attribute_name] = value if value.is_a?(String)
        end
      end

      # devalue の参照を1段解決する。Integer は配列インデックス（負数は undefined）、
      # ["Reactive", idx] のようなラッパー配列はその参照先をたどる。それ以外はそのまま返す。
      # ラッパーが入れ子でも有限回で終わるよう、辿った回数を配列長で打ち切る。
      def self.devalue_value(entries, reference)
        current = reference
        entries.size.times do
          return nil if current.is_a?(Integer) && current.negative?

          current = entries[current] if current.is_a?(Integer)
          return current unless current.is_a?(Array) && NUXT_WRAPPER_TYPES.include?(current.first) && current[1].is_a?(Integer)

          current = current[1]
        end
        nil
      end

      def self.squish(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      private

      # 1スキルぶんのページ送り。カードが0件のページはページ終端（最終ページを超えた page は
      # HTTP 200 で本文「ありません」になる）またはセレクタ崩れなので、それ以上取得せず打ち切る。
      # SSR本文の「この条件の案件数N件」は常に0件（件数はクライアント側描画）なので終端判定には使わない。
      def fetch_target(target, fetch_failures)
        postings = []

        (1..@max_pages).each do |page_number|
          body = fetch_page_body(target[:skill_id], page_number, fetch_failures)
          break if body.nil?

          page_postings = self.class.parse(body, today: @today, category_hint: target[:category_hint])
          break if page_postings.empty?

          postings.concat(page_postings)
        end

        postings
      end

      # 散発的なHTTPエラーや通信層のタイムアウト・切断で1ページが取れなくても、スキル全体・取得元全体は
      # 落とさず、そのスキルのページ送りだけを打ち切って次のスキルへ進む。
      # rescueの範囲が@fetcher.getの1回だけなので、StandardErrorで受けてもパース側のバグは覆い隠さない。
      def fetch_page_body(skill_id, page_number, fetch_failures)
        get_with_single_retry(list_url(skill_id, page_number))
      rescue FreelanceJobs::AccessBlockedError
        raise
      rescue StandardError => error
        fetch_failures << error
        FreelanceJobs.logger.warn(
          "[FreelanceJobs::Sources::EngineerFactory] #{error.message} このスキルのページ送りを打ち切ります"
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
        FreelanceJobs.logger.warn("[FreelanceJobs::Sources::EngineerFactory] #{error.message} 1回だけ再取得します")
        @fetcher.get(url)
      end

      # 1ページ目はクエリ無し、2ページ目以降はサイト自身のページャと同じ ?page=<n> 形式。
      def list_url(skill_id, page_number)
        list_path = "#{BASE_URL}/freelance/jobs/skill/#{skill_id}"
        page_number == 1 ? list_path : "#{list_path}?page=#{page_number}"
      end
    end
  end
end
