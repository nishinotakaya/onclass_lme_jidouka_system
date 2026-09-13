# frozen_string_literal: true

require "nokogiri"
require "json"
require "date"
require "cgi"

module FreelanceJobs
  module Sources
    # フリーランスボード: 複数エージェントの案件を集約するアグリゲータ（Nuxt3 のSSRページ）。
    # 案件カードのHTMLではなく、`script#__NUXT_DATA__` に埋まっている devalue 形式のJSONを読む。
    # 一覧JSONに単価・スキル・業務内容・必須スキルまで全項目が揃うため、詳細ページは取得しない。
    #
    # devalue 形式の前提（壊れたらここを疑う）:
    #   - JSON全体はトップレベルの配列。Hash/Array の値はすべて「トップ配列のインデックス（Integer）」で、
    #     参照先が scalar ならそれが値。負のインデックスは undefined 等の特殊値なので nil として扱う
    #   - 先頭が型名の配列（["Reactive", 1] / ["Set"] 等）はラッパ。案件レコードの経路には出ないので解決しない
    #   - 案件一覧は「jobs」と「count」の両キーを持つ Hash（1ページに1個だけ存在。実体は
    #     root.data.getJobIndex）。その "jobs" は案件Hashへの参照の配列で、並びはDOMのカード順と一致する
    #   - 案件Hashの主要キー: id / display_title / name / detail / required / welcome / ai_summary /
    #     monthly_payment_f_num / monthly_payment_l_num / hourly_payment_f_num / hourly_payment_l_num /
    #     skill_ids（文字列IDの配列）+ skill_key_values（ID => {name}） / occupation_ids + occupation_key_values /
    #     prefecture_key_values / station_key_values / work_styles_key_values / agent_key_values（提供元） /
    #     generation_ai_project_flg / business_day_desc / first_published_at / closed_at
    #
    # URL設計（実測）: `/jobs?keyword=Ruby` が1ページ目、2ページ目以降は `&page=N`。30件/頁固定で
    # 並びは新着順（created_at の日付単位で降順）。sort パラメータは不要。
    # 提供元（agent_key_values の service_name）にはレバテックフリーランス・Findy Freelance 等、
    # 既に別の取得元で稼働中のエージェントが含まれ大量に重複するため、excluded_providers で捨てる。
    class FreelanceBoard
      SITE_NAME = "フリーランスボード"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://freelance-board.com"

      DEFAULT_SEARCH_TARGETS = [
        { keyword: "Ruby", hint: "Ruby" },
        { keyword: "TypeScript", hint: "TypeScript" },
        { keyword: "React", hint: "React" }
      ].freeze

      # 1キーワードあたりの取得ページ数。Rubyは30件/頁で約4〜5日分の新着に相当し、
      # 「毎朝の新着差分取り」には3ページで足りる（総件数は26,919件あるが全件は取らない）。
      MAX_PAGES = 3

      # 1回のバッチ実行でこの取得元が発行するHTTPリクエストの上限（サイトへの配慮）。
      # max_pages を大きくしても search_targets 数 × ページ数 がこれを超えないよう initialize で丸める。
      MAX_REQUESTS_PER_FETCH = 40

      # 既存の取得元と重複する提供元。比較は provider_key（小文字化＋空白除去）で行うため、
      # "TechStock" と "TECH STOCK"、"coconalaテック" と "coconalaテック " のような表記ゆれは吸収される。
      # サイト上の service_name は英字表記のものがある（Bizlink=ビズリンク / TechStock=TECH STOCK /
      # coconalaテック=ココナラテック）ので、実データの表記で列挙している。
      DEFAULT_EXCLUDED_PROVIDERS = [
        "レバテックフリーランス",
        "レバテッククリエイター",
        "Findy Freelance",
        "HiPro Tech",
        "フォスターフリーランス",
        "ポテパンフリーランス",
        "Bizlink",
        "TechStock",
        "coconalaテック",
        "ココナラテック"
      ].freeze

      NUXT_DATA_SELECTOR = "script#__NUXT_DATA__"

      # devalue の参照を辿る深さの上限。案件Hash → key_values → 要素Hash → name の3段で足りるが、
      # 循環参照が入っていても止まるよう余裕を持たせている。
      MAX_RESOLVE_DEPTH = 8

      # 生成AI活用案件のフラグ値（1=該当、2=非該当）。DOM の `div.tag.generation-ai-tag` の文言に合わせる。
      GENERATION_AI_PROJECT_FLAG = 1
      GENERATION_AI_TAG = "生成AI活用案件"

      # エージェントが業務内容（detail）の末尾に付ける定型の注意書きブロック。
      # 実測では ROSCA freelance の全案件が「========================」の行で挟んだ
      # 「※必ずお読みください※ … 弊社より直接スカウトを送信させていただきます …」を付けており、
      # このまま description に載せると EngineerClassifier の NON_DEV_RE（スカウト）に当たって
      # 開発案件なのに分類対象外になり、「実務経験1年以上」が難易度判定にも誤って効く。
      # 「=」8個以上だけの行から、次の同じ行（無ければ末尾）までを丸ごと落とす。
      AGENT_NOTICE_BLOCK_RE = /^[[:blank:]]*={8,}[[:blank:]]*$.*?(?:^[[:blank:]]*={8,}[[:blank:]]*$|\z)/m

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, max_pages: MAX_PAGES,
                     excluded_providers: DEFAULT_EXCLUDED_PROVIDERS)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @max_pages = clamp_pages_to_request_budget(max_pages, search_targets.size)
        @excluded_provider_keys = excluded_providers.map { |provider| self.class.provider_key(provider) }
      end

      # 通信あり。キーワードごとに一覧を1ページ目から max_pages ページ分たどる。
      # ページ単位の取得失敗は握りつぶして次のキーワードへ進むが、1件も取れずに失敗だけが
      # 残った場合は最初の失敗を送出し、ResearchServiceに「取得失敗」として記録させる
      # （黙って0件を返すと、サイト構造の崩れや全面的な障害に気付けなくなるため）。
      # Ruby一覧とReact一覧には同じ案件が現れるため、URLをキーに重複排除する。
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
      # excluded_providers はインスタンスの設定なので、ここでは提供元による除外は行わない
      # （fetch 側で行う）。today は全取得元共通のインターフェースとして受け取るが、
      # 締切の概念が無く posted_on もJSONの日時をそのまま使うため、この取得元では参照しない。
      def self.parse(body, today:, category_hint: nil)
        postings = {}

        job_records(body).each do |job|
          posting = build_posting(job, category_hint)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      # __NUXT_DATA__ から案件Hash（参照を解決済み）の配列を取り出す。script要素が無い・JSONが壊れている・
      # 期待キーが見つからない場合は例外にせず空配列を返す（サイト構造が変わったときは
      # 「0件になる」形で表面化させ、バッチ全体を落とさない）。
      def self.job_records(body)
        script_node = Nokogiri::HTML(body).at_css(NUXT_DATA_SELECTOR)
        return [] unless script_node

        top_level_values = JSON.parse(script_node.text)
        return [] unless top_level_values.is_a?(Array)

        job_references = job_index_references(top_level_values)
        job_references.map { |reference| resolve_reference(top_level_values, reference) }
                      .select { |job| job.is_a?(Hash) }
      rescue JSON::ParserError
        []
      end

      # 案件一覧Hash（"jobs" と "count" を両方持つ。1ページに1個だけ）を線形探索し、"jobs" の参照配列を返す。
      # root.data.getJobIndex と経路で辿るより、Nuxt側のstateキー名の変更に強い。
      def self.job_index_references(top_level_values)
        job_index = top_level_values.find do |value|
          value.is_a?(Hash) && value.key?("jobs") && value.key?("count")
        end
        return [] unless job_index

        job_references = resolve_scalar(top_level_values, job_index["jobs"])
        job_references.is_a?(Array) ? job_references : []
      end

      # devalue の参照（トップ配列のインデックス）を再帰的に実値へ展開する。
      # Hash/Array はその中の参照も解決し、scalar はそのまま返す。負のインデックス（undefined 等）と
      # 範囲外の参照は nil。深さ上限を超えたら nil にして循環参照でも止まるようにする。
      def self.resolve_reference(top_level_values, reference, depth = 0)
        return nil if depth > MAX_RESOLVE_DEPTH

        value = resolve_scalar(top_level_values, reference)
        case value
        when Hash
          value.transform_values { |inner_reference| resolve_reference(top_level_values, inner_reference, depth + 1) }
        when Array
          value.map { |inner_reference| resolve_reference(top_level_values, inner_reference, depth + 1) }
        else
          value
        end
      end

      # 参照を1段だけ解決する（Hash/Array の中身は参照のまま）。
      def self.resolve_scalar(top_level_values, reference)
        return nil unless reference.is_a?(Integer) && reference >= 0

        top_level_values[reference]
      end

      # 案件ID（詳細URLの識別子）と案件名は案件行として最低限必要な必須要素。
      # JSONのキー名が変わってどちらかが取れなくなった案件は、行を壊さないよう黙って除外する
      # （全件が欠損した場合は0件になり、呼び出し側のログで気付ける）。
      def self.build_posting(job, category_hint)
        job_id = positive_amount(job["id"])
        title = job_title(job)
        return nil if job_id.nil? || title.empty?

        skills = key_value_names(job, "skill_ids", "skill_key_values")
        monthly_amounts = amount_range(job, "monthly_payment_f_num", "monthly_payment_l_num")
        hourly_amounts = amount_range(job, "hourly_payment_f_num", "hourly_payment_l_num")

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}/jobs/detail/#{job_id}"),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(build_description(job, skills)),
          category_hint: category_hint,
          reward: build_reward(monthly_amounts, hourly_amounts),
          work_format: work_format(monthly_amounts, hourly_amounts),
          application_status: "-", # 応募数はJSONに含まれない
          deadline_text: "-",      # closed_at は全件nil。掲載期限は詳細ページのJSON-LDにしか無い
          deadline_on: nil,
          skills: skills,
          client: provider_name(job), # 提供元エージェント名（後段の重複排除用。シートには出ない）
          tags: build_tags(job),
          posted_on: parse_date(job["first_published_at"])
        )
      end

      # 表示名は display_title（サイト側で整えた見出し。DOMの h2 と同じ）。
      # 空なら name（エージェント側の原題「【Ruby on Rails】…」）にフォールバックする。
      def self.job_title(job)
        display_title = squish(job["display_title"])
        display_title.empty? ? squish(job["name"]) : display_title
      end

      # 提供元ラベル。agent_key_values は {"5" => {"service_name" => "FLEXY", ...}} の1要素Hash。
      # company_key_values の name（エージェント運営会社名）は提供元ラベルではないので使わない。
      def self.provider_name(job)
        agents = job["agent_key_values"]
        return "" unless agents.is_a?(Hash)

        agent = agents.values.first
        agent.is_a?(Hash) ? squish(agent["service_name"]) : ""
      end

      # 提供元の比較キー。小文字化して空白を全て除き、"TechStock" / "TECH STOCK" のような
      # 大小文字・空白の表記ゆれを吸収する。
      def self.provider_key(provider)
        squish(provider).downcase.gsub(/[[:space:]]/, "")
      end

      # description は EngineerClassifier の判定テキストにそのまま入り、RowBuilder が先頭160字を
      # 要約列に使う。そのため人が読む「概要」を先頭に置き、技術名が載る業務内容・必須スキル・
      # 使用技術・募集職種を続け、REMOTE_RE / LONG_TERM_RE に当たる勤務形態・稼働を末尾に置く。
      def self.build_description(job, skills)
        occupations = key_value_names(job, "occupation_ids", "occupation_key_values")
        work_location = work_location_text(job)
        work_style = first_key_value_name(job, "work_styles_key_values")

        parts = []
        append_labeled_part(parts, "概要", job["ai_summary"])
        append_labeled_part(parts, "業務内容", strip_agent_notice(job["detail"]))
        append_labeled_part(parts, "必須スキル", strip_agent_notice(job["required"]))
        append_labeled_part(parts, "歓迎スキル", strip_agent_notice(job["welcome"]))
        append_labeled_part(parts, "使用技術", skills.join(" / "))
        append_labeled_part(parts, "募集職種", occupations.join("・"))
        append_labeled_part(parts, "勤務地", work_location)
        append_labeled_part(parts, "勤務形態", work_style)
        append_labeled_part(parts, "稼働", job["business_day_desc"])
        parts.join(" / ")
      end

      def self.append_labeled_part(parts, label, value)
        text = squish(value)
        parts << "#{label}: #{text}" unless text.empty?
      end

      # エージェント定型の注意書きブロック（AGENT_NOTICE_BLOCK_RE）を本文から落とす。
      # 行構造で区切りを見るため squish 前の生テキストに適用する。
      def self.strip_agent_notice(text)
        text.to_s.gsub(AGENT_NOTICE_BLOCK_RE, " ")
      end

      # 勤務地は都道府県（prefecture_key_values）と最寄駅（station_key_values）の連結。駅は無い案件がある。
      def self.work_location_text(job)
        [first_key_value_name(job, "prefecture_key_values"), first_key_value_name(job, "station_key_values")]
          .reject(&:empty?).join(" ")
      end

      # IDの配列（例 skill_ids => ["4","130"]）を key_values（例 skill_key_values => {"4" => {"name" => "Ruby"}}）で
      # 名前に引く。IDの順序を保つため key_values を直接列挙せず、IDから引く。IDが nil/空なら []。
      def self.key_value_names(job, ids_key, key_values_key)
        ids = job[ids_key]
        key_values = job[key_values_key]
        return [] unless ids.is_a?(Array) && key_values.is_a?(Hash)

        ids.map { |id| entry_name(key_values[id.to_s]) }.reject(&:empty?)
      end

      # 1要素の key_values（都道府県・駅・勤務形態）から name を取り出す。無ければ空文字。
      def self.first_key_value_name(job, key_values_key)
        key_values = job[key_values_key]
        return "" unless key_values.is_a?(Hash)

        entry_name(key_values.values.first)
      end

      def self.entry_name(entry)
        entry.is_a?(Hash) ? squish(entry["name"]) : ""
      end

      # タグは勤務形態（フルリモート / 一部リモート可 / 常駐）と生成AI活用案件フラグ。
      def self.build_tags(job)
        tags = []
        work_style = first_key_value_name(job, "work_styles_key_values")
        tags << work_style unless work_style.empty?
        tags << GENERATION_AI_TAG if job["generation_ai_project_flg"] == GENERATION_AI_PROJECT_FLAG
        tags
      end

      # 単価の下限（*_f_num）と上限（*_l_num）を [下限, 上限] にする。同値なら1要素、片方だけでも1要素。
      # 実データは全件月額（payment_type_id=1）で、時給の2キーは存在するが nil。
      def self.amount_range(job, lower_key, upper_key)
        [positive_amount(job[lower_key]), positive_amount(job[upper_key])].compact.uniq
      end

      # "800,000円／月" / "1,050,000〜1,200,000円／月"（／は全角U+FF0F）。月額が無ければ時給、どちらも無ければ要確認。
      # EngineerClassifier.high_reward? がカンマを除いた先頭の数値を読むため、下限を先に置く。
      def self.build_reward(monthly_amounts, hourly_amounts)
        return "#{format_amount_range(monthly_amounts)}円／月" unless monthly_amounts.empty?
        return "#{format_amount_range(hourly_amounts)}円／時" unless hourly_amounts.empty?

        "要確認"
      end

      def self.format_amount_range(amounts)
        amounts.map { |amount| with_thousands_separator(amount) }.join("〜")
      end

      # 単価表記と同じ分岐で契約形態を決める。DOM上の契約表記は全件「業務委託(フリーランス)」だが、
      # 月額が出ていれば月額制、時給のみなら時間単価制に寄せる（他の取得元と揃える）。
      def self.work_format(monthly_amounts, hourly_amounts)
        return "月額制（業務委託）" unless monthly_amounts.empty?
        return "時間単価制" unless hourly_amounts.empty?

        "業務委託（フリーランス）"
      end

      # 金額・IDは数値で入る（例 monthly_payment_f_num => 800000）。nil・0・数値でない値はnilにして、
      # 呼び出し側の「値なし」分岐に寄せる。
      def self.positive_amount(value)
        return nil unless value.is_a?(Numeric) || value.to_s.match?(/\A\d+\z/)

        amount = value.to_i
        amount.positive? ? amount : nil
      end

      # 3桁区切り。末尾から3桁ごとの区切り位置（後ろに3の倍数桁が続く数字）にカンマを挿む。
      # ActiveSupportのnumber_with_delimiterは使わない（テストはRails無しの ruby -Itest で走るため）。
      def self.with_thousands_separator(amount)
        amount.to_s.gsub(/(\d)(?=(\d{3})+\z)/, '\1,')
      end

      # first_published_at は "2026-09-12T04:07:30.000+09:00" 形式。壊れた値・nilはnilにするだけで落とさない。
      def self.parse_date(text)
        Date.parse(text.to_s)
      rescue ArgumentError, TypeError
        nil
      end

      def self.squish(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      private

      # search_targets 数 × ページ数 が MAX_REQUESTS_PER_FETCH を超えないようページ数を丸める
      # （既定の3キーワードなら最大13ページ）。search_targets が空ならリクエストは発生しないのでそのまま。
      def clamp_pages_to_request_budget(max_pages, target_count)
        return max_pages if target_count.zero?

        [max_pages, MAX_REQUESTS_PER_FETCH / target_count].min
      end

      # 1キーワードぶんのページ送り。案件が0件のページはページ終端（またはJSON構造の崩れ）なので、
      # それ以上リクエストしても無駄になるため打ち切る。
      # 一覧の並びは created_at の日付単位でしか単調でないため、「取得済みの案件に当たったら打ち切る」
      # ような早期終了はせず、固定ページ数を取って SheetMerger のURL重複保持に任せる。
      def fetch_target(target, fetch_failures)
        postings = []

        (1..@max_pages).each do |page_number|
          body = fetch_page_body(target[:keyword], page_number, fetch_failures)
          break if body.nil?

          page_postings = self.class.parse(body, today: @today, category_hint: target[:hint])
          break if page_postings.empty?

          postings.concat(page_postings.reject { |posting| excluded_provider?(posting) })
        end

        postings
      end

      # 提供元（client に入れたラベル）が excluded_providers に含まれる案件は、既存の取得元と重複するので捨てる。
      def excluded_provider?(posting)
        @excluded_provider_keys.include?(self.class.provider_key(posting.client))
      end

      # 散発的なHTTPエラー・通信層のタイムアウト・切断は、1ページの失敗でキーワード全体・取得元全体を
      # 落とさず、そのキーワードのページ送りだけを打ち切って次のキーワードへ進む。
      # rescueの範囲が@fetcher.getの1回だけなので、StandardErrorで受けてもパース側のバグは覆い隠さない。
      def fetch_page_body(keyword, page_number, fetch_failures)
        get_with_single_retry(list_url(keyword, page_number))
      rescue FreelanceJobs::AccessBlockedError
        raise
      rescue StandardError => error
        fetch_failures << error
        FreelanceJobs.logger.warn(
          "[FreelanceJobs::Sources::FreelanceBoard] #{error.message} このキーワードのページ送りを打ち切ります"
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
        FreelanceJobs.logger.warn("[FreelanceJobs::Sources::FreelanceBoard] #{error.message} 1回だけ再取得します")
        @fetcher.get(url)
      end

      # 1ページ目は page パラメータ無し（`&page=1` の挙動は未検証のため実測済みの形に合わせる）。
      def list_url(keyword, page_number)
        url = "#{BASE_URL}/jobs?keyword=#{CGI.escape(keyword)}"
        page_number == 1 ? url : "#{url}&page=#{page_number}"
      end
    end
  end
end
