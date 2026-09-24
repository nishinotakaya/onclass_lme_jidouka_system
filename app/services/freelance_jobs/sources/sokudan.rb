# frozen_string_literal: true

require "nokogiri"
require "json"
require "date"

module FreelanceJobs
  module Sources
    # SOKUDAN（企業と直接契約するマッチング型。週1〜2日・リモートの副業案件が中心）。
    # 一覧は JSON API（/api/v2/top/projects）を読む。ページングが効く（実測。SSRの
    # `__NEXT_DATA__` はページングが効かなかったため、一覧はAPI・詳細はSSRの二本立てになっている）。
    # 詳細ページは従来どおり Next.js Pages Router の SSR ページで、案件データは
    # `script#__NEXT_DATA__`（type="application/json"）の JSON にのみ存在するため、
    # Findy Freelance と同じく埋め込みJSONを読む（HTMLパースはしない）。
    #
    # 取得の流れ（「一覧API（ページング）＋詳細ページJSON」型）:
    #   1. スキルidの組み合わせごとの一覧（/api/v2/top/projects?search_project[...][]=<id>&page=<n>）と
    #      全体新着一覧（/api/v2/top/projects?page=1、ページングしない）から、募集中かつ業務委託系の
    #      案件IDを集める（一覧には使用技術・本文が無い）
    #   2. createdAt 降順に並べ、上位 MAX_DETAIL_FETCHES 件だけ詳細（/top/projects/<id>）を取り、
    #      使用技術・本文・掲載日で一覧の結果を上書きする
    #
    # 一覧APIの前提（壊れたらここを疑う）:
    #   - GET /api/v2/top/projects。Cookie不要だが `x-requested-with: XMLHttpRequest` ヘッダが要る
    #     （無いと通常のSSRページが返る）
    #   - クエリ: `search_project[searchable_language_skill_ids][]=<id>` をスキルidの数だけ繰り返し、
    #     末尾に `page=<n>`（1始まり）。スキル未指定なら全体新着（`?page=1` のみ、ページングしない）
    #   - 応答本文はJSONそのもの（HTMLラッパーもscriptタグも無い）。`projectList` キーが案件の配列
    #   - projectList は必ず opened → closed の順（createdAt降順ではない）。page=2以降はclosedのみになる
    #     ことがあるので、そのページの生の projectList に state=="opened" が1件も無くなるまで、
    #     最大3ページ読み進める（打ち切り判定は契約形態フィルタ後ではなく生のprojectListで行う。
    #     詳しくは #skill_list_postings のコメントを参照）
    #   - 案件1件のキーは詳細ページのstaticProjectと共通: id(Integer) / title /
    #     state("opened"|"closed") / contractType / createdAt / minBudget.id・maxBudget.id
    #     （円の整数。label は "65万"）/ remoteType.label / projectAvailableTime.label /
    #     minWorkingHoursLabel / prefecture.label / tags[].label / professions[].label /
    #     corporation.name（未ログインでは "＊＊＊＊＊" にマスク）
    #   - スキルid: HTML=8 / CSS=1 / Ruby=3 / TypeScript=5 / React=15 / Ruby on Rails=19
    #
    # 詳細ページ（SSR。据え置き）の前提:
    #   - script#__NEXT_DATA__ は1ページに1個。中身はJSON全体（buildId・page・query等も含む）
    #   - props.pageProps.staticProject … 一覧要素と同じキーに加えて
    #     detail(本文全文、改行は "\r\n") / requiredSkills[].label（name は常に空）/
    #     applicationOpenAt / projectDetailStructuredData.jobPosting（JSON-LD。datePosted と
    #     baseSalary.value.unitText "MONTH"|"HOUR" を使う。validThrough は無い）
    #   - 詳細URLは必ず /top/projects/<id>。`/projects/<id>` は /login へ 302 するので使わない
    #
    # 注意（Cloudflare）: 全応答が server: cloudflare。日本の家庭IPからは全件 HTTP 200 だったが、
    # 本番（Heroku の US データセンターIP）では bot 判定で弾かれる可能性がある。
    # チャレンジ応答は HttpFetcher で FetchError になる（`cf-mitigated` ヘッダは AccessBlockedError の
    # 判定対象外）ため、初回本番実行後に Sidekiq ログで SOKUDAN の HTTP ステータスを確認すること。
    class Sokudan
      SITE_NAME = "SOKUDAN"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://sokudan.work"
      LIST_PATH = "/top/projects"
      # 一覧JSON APIのパス（詳細ページのLIST_PATHとは別物）。
      LIST_API_PATH = "/api/v2/top/projects"
      # 一覧APIに必須のヘッダ。無いとSSRの通常ページが返り projectList が読めない。
      LIST_REQUEST_HEADERS = { "x-requested-with" => "XMLHttpRequest" }.freeze
      # スキル一覧1本あたりのページ送り上限（サイトへの配慮。全体新着はページングしないので対象外）。
      MAX_LIST_PAGES_PER_TARGET = 3

      # スキルidの組み合わせ。skill_ids は searchable_language_skill_ids のクエリに配列順で渡す
      # （Ruby と Ruby on Rails は結果が一部重複するが、候補のURLキーによる重複除去で吸収される）。
      # HTML/CSS は特定の技術カテゴリに寄らないため category_hint は nil にし、分類（EngineerClassifier）に任せる。
      DEFAULT_SEARCH_TARGETS = [
        { skill_ids: [3, 19], category_hint: "Ruby" },
        { skill_ids: [5],     category_hint: "TypeScript" },
        { skill_ids: [15],    category_hint: "React" },
        { skill_ids: [8, 1],  category_hint: nil }
      ].freeze

      # 1回のバッチ実行でこの取得元が発行してよいHTTPリクエスト数の上限（サイトへの配慮）。
      REQUEST_BUDGET = 40
      # 詳細ページを取る上限。一覧は最悪ケースでスキル4グループ×最大3ページ＋全体新着1本＝13本、
      # 詳細27件で合計40（予算内）に収まる（detail_fetch_limit 参照）。
      MAX_DETAIL_FETCHES = 27
      # 全体新着一覧（/top/projects）由来の候補は、createdAt がこの日数以内のものだけ詳細を取る。
      # スラッグ一覧は投稿者のスキルタグ依存で新着を取りこぼす（例: 「物流DX×TypeScript」が
      # required_skills/TypeScript に載らない）ため全体新着を併用するが、40件全部を毎日取り直すと
      # 予算を食い潰すので「毎朝の新着差分取り」に絞る。SheetMerger が既存行をURLで保持するので積み上がる。
      LATEST_LOOKBACK_DAYS = 2

      OPENED_STATE = "opened"
      # contractType の4値。副業案件として採るのは業務委託系の2つだけ
      # （全体新着40件中20件が正社員のため、除外しないとシートが求人で埋まる）。
      CONTRACT_TYPE_LABELS = {
        "outsourcing" => "業務委託",
        "outsourcing_to_full_time" => "業務委託→正社員",
        "full_time" => "正社員",
        "fixed_term" => "契約社員・パート等"
      }.freeze
      SIDE_JOB_CONTRACT_TYPES = %w[outsourcing outsourcing_to_full_time].freeze

      # 未ログインでは corporation.name が "＊＊＊＊＊"（全角/半角アスタリスクのみ）にマスクされる。
      MASKED_CLIENT_NAME_RE = /\A[＊*]+\z/.freeze
      # JSON-LD baseSalary.value.unitText。"HOUR" のときだけ時間単価、それ以外（"MONTH"・欠損）は月額。
      HOURLY_UNIT_TEXT = "HOUR"
      HOURLY_UNIT_SUFFIX = "円／時"
      MONTHLY_UNIT_SUFFIX = "円／月"

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, include_latest_list: true,
                     max_detail_fetches: MAX_DETAIL_FETCHES, latest_lookback_days: LATEST_LOOKBACK_DAYS)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @include_latest_list = include_latest_list
        @max_detail_fetches = max_detail_fetches
        @latest_lookback_days = latest_lookback_days
      end

      # 通信あり。一覧で候補を集め、新しい順に詳細を取って上書きする。
      # 一覧1本・詳細1件の取得失敗は握りつぶして次へ進む（詳細に失敗した案件は一覧の結果をそのまま採用）が、
      # 1件も取れずに失敗だけが残った場合は最初の失敗を送出し、ResearchServiceに「取得失敗」として
      # 記録させる（黙って0件を返すと、サイト構造の崩れや全面的な障害に気付けなくなるため）。
      def fetch
        fetch_failures = []
        candidates = collect_candidates(fetch_failures)
        postings = fetch_details(candidates, fetch_failures)

        raise fetch_failures.first if postings.empty? && !fetch_failures.empty?

        postings
      end

      # 通信なし（テスト用）。一覧APIの応答本文（JSON）から、業務委託系の案件一覧を作る。
      # ページの写しに徹する（募集終了(closed)も含めて返す。除外するかは呼び出し側 fetch の判断。
      # Sources::CoconalaTech / Sources::Levtech と同じ流儀）。
      # 一覧には使用技術・本文が無いため skills は []、posted_on は createdAt になる。
      # todayは全取得元共通のインターフェースとして受け取るが、締切の概念が無いためここでは参照しない。
      def self.parse(body, today:, category_hint: nil)
        postings = {}

        project_list(body).each do |project|
          next unless side_job_project?(project)

          posting = build_posting(project, category_hint)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      # 通信なし（テスト用）。詳細1ページ分のHTML本文から全フィールド入りの JobPosting を1件作る。
      # state は application_status に反映するだけで、募集終了の除外は呼び出し側（fetch）が行う。
      # JSONが無い・必須キーが欠けている場合は nil（呼び出し側は一覧の結果にフォールバックする）。
      def self.parse_detail(body, today:, category_hint: nil)
        project = dig_nested_hash(next_data(body), "props", "pageProps", "staticProject")
        return nil unless project.is_a?(Hash)

        build_posting(project, category_hint)
      end

      # 一覧APIの応答本文（JSON）から案件配列を取り出す。JSONが壊れている・`projectList` キーが
      # 無い・期待キーが配列でない場合は例外にせず空配列を返す（サイト構造が変わったときは
      # 「0件になる」形で表面化させ、バッチ全体を落とさない）。
      def self.project_list(body)
        payload = JSON.parse(body)
        return [] unless payload.is_a?(Hash)

        project_list = payload["projectList"]
        project_list.is_a?(Array) ? project_list.select { |project| project.is_a?(Hash) } : []
      rescue JSON::ParserError
        []
      end

      def self.next_data(body)
        script_node = Nokogiri::HTML(body).at_css("script#__NEXT_DATA__")
        return nil unless script_node

        JSON.parse(script_node.text)
      rescue JSON::ParserError
        nil
      end

      # ネストしたHashを順に辿る。Hash#digを直接使うと、途中の値が配列や文字列だったときに
      # TypeError/NoMethodErrorで落ちてしまい「構造が変わったら0件」という方針が崩れるため、
      # 各段でHashであることを確かめながら降りる。
      def self.dig_nested_hash(payload, *keys)
        current_value = payload
        keys.each do |key|
          return nil unless current_value.is_a?(Hash)

          current_value = current_value[key]
        end
        current_value
      end

      # 業務委託系（正社員・契約社員は副業案件でないので除外）。stateでの絞り込みはしない
      # （一覧はページの写しに徹する。募集終了は application_status が CLOSED_STATUS になり、
      # 上位の ResearchService が closed_urls として既存行の削除に使う）。
      def self.side_job_project?(project)
        SIDE_JOB_CONTRACT_TYPES.include?(project["contractType"])
      end

      # 一覧要素と詳細の staticProject は同じキー名で、詳細が一覧の上位集合になっている。
      # そのため1つの組み立てで両方を扱い、詳細にしか無いキー（requiredSkills / detail /
      # projectDetailStructuredData）は欠けていれば空として組み立てる。
      def self.build_posting(project, category_hint)
        project_id = project["id"].to_s.strip
        title = squish(project["title"])
        # id（詳細URLの識別子）と title は案件行として最低限必要な必須要素。
        # JSONのキー名が変わってどちらかが取れなくなった要素は、行を壊さないよう黙って除外する。
        return nil unless project_id.match?(/\A\d+\z/) && !title.empty?

        skills = entry_labels(project["requiredSkills"])
        unit_suffix = reward_unit_suffix(project)
        reward = build_reward(project, unit_suffix)

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}#{LIST_PATH}/#{project_id}"),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(build_description(project, skills)),
          category_hint: category_hint,
          reward: reward,
          work_format: work_format(reward, unit_suffix),
          application_status: project["state"] == OPENED_STATE ? "募集中" : FreelanceJobs::JobPosting::CLOSED_STATUS,
          deadline_text: "-", # 締切の概念が無いサイト（JSON-LD にも validThrough が無い）
          deadline_on: nil,
          skills: skills,
          client: client_name(project),
          tags: build_tags(project),
          posted_on: posted_on(project)
        )
      end

      # description は EngineerClassifier の判定テキストにそのまま入るので、技術名が載る募集職種・
      # 必須スキルを先頭に置き、稼働（LONG_TERM_RE の「週N日」）・勤務形態（REMOTE_RE）を続ける。
      # 本文（detail）は全文を末尾に置く。技術名が本文にしか無い案件（スキルタグ未設定）の判定に効く。
      def self.build_description(project, skills)
        professions = entry_labels(project["professions"])
        available_time = nested_label(project, "projectAvailableTime")
        min_working_hours = project["minWorkingHoursLabel"].to_s.strip
        remote_type = nested_label(project, "remoteType")
        prefecture = nested_label(project, "prefecture")
        contract_type = contract_type_label(project)
        detail = project["detail"].to_s.strip

        parts = []
        parts << "募集職種: #{professions.join("・")}" unless professions.empty?
        parts << "必須スキル: #{skills.join(" / ")}" unless skills.empty?
        parts << "稼働: #{working_time_text(available_time, min_working_hours)}" unless available_time.empty?
        parts << "勤務形態: #{remote_type}" unless remote_type.empty?
        parts << "勤務地: #{prefecture}" unless prefecture.empty?
        parts << "契約形態: #{contract_type}" unless contract_type.empty?
        parts << "案件詳細: #{detail}" unless detail.empty?
        parts.join(" / ")
      end

      # "週2日（週16~23h）" に最低稼働時間 "週20h" を添えて "週2日（週16~23h）（週20h〜）" にする。
      def self.working_time_text(available_time, min_working_hours)
        min_working_hours.empty? ? available_time : "#{available_time}（#{min_working_hours}〜）"
      end

      # 単価は minBudget.id / maxBudget.id が円の整数（label は "65万"）。
      # EngineerClassifier.high_reward? がカンマを除いた先頭の数値（下限）を読むため3桁区切りで出す。
      # 下限が無い・0 の案件は単価非公開とみなし "要確認" にする。
      def self.build_reward(project, unit_suffix)
        min_budget = positive_amount(dig_nested_hash(project, "minBudget", "id"))
        max_budget = positive_amount(dig_nested_hash(project, "maxBudget", "id"))
        return "要確認" unless min_budget

        amounts = [min_budget, max_budget].compact.uniq.map { |amount| with_thousands_separator(amount) }
        "#{amounts.join("〜")}#{unit_suffix}"
      end

      # 単位は JSON-LD の baseSalary.value.unitText で決める（実測は全件 "MONTH"。サイト表示
      # 「報酬65万〜95万」も月額）。一覧には JSON-LD が無いので月額を既定にする。
      def self.reward_unit_suffix(project)
        unit_text = dig_nested_hash(
          project, "projectDetailStructuredData", "jobPosting", "baseSalary", "value", "unitText"
        )
        unit_text.to_s.upcase == HOURLY_UNIT_TEXT ? HOURLY_UNIT_SUFFIX : MONTHLY_UNIT_SUFFIX
      end

      # 単価表記と同じ分岐で契約形態を決める（単価非公開なら区分だけを示す）。
      def self.work_format(reward, unit_suffix)
        return "業務委託（フリーランス）" if reward == "要確認"

        unit_suffix == HOURLY_UNIT_SUFFIX ? "時間単価制" : "月額制（業務委託）"
      end

      # 契約形態ラベルを先頭に置き、サイトのタグ（"フリーランス歓迎" / "フルリモート" / "高単価" 等）を続ける。
      def self.build_tags(project)
        contract_type = contract_type_label(project)
        tag_labels = entry_labels(project["tags"])
        contract_type.empty? ? tag_labels : [contract_type, *tag_labels]
      end

      # 未知の contractType 値はラベル表に無いので生の値をそのまま出す（黙って落とさない）。
      def self.contract_type_label(project)
        contract_type = project["contractType"].to_s.strip
        CONTRACT_TYPE_LABELS.fetch(contract_type, contract_type)
      end

      # 発注企業名。未ログインでは "＊＊＊＊＊" にマスクされるため、その場合は非公開として "" にする。
      def self.client_name(project)
        name = squish(dig_nested_hash(project, "corporation", "name"))
        name.match?(MASKED_CLIENT_NAME_RE) ? "" : name
      end

      # 掲載日は JSON-LD の datePosted（"2026-09-10"）を正とし、無ければ募集開始日時 applicationOpenAt、
      # それも無ければ作成日時 createdAt（一覧にはこれしか無い）の順にフォールバックする。
      def self.posted_on(project)
        date_posted = dig_nested_hash(project, "projectDetailStructuredData", "jobPosting", "datePosted")
        [date_posted, project["applicationOpenAt"], project["createdAt"]].each do |text|
          date = parse_date(text)
          return date if date
        end
        nil
      end

      # [{ "label" => ... }] の配列から label だけを取り出す（name は空文字のことが多いので使わない）。
      # requiredSkills には "Ruby on Railsでの開発経験 " のように末尾空白付きが混ざるので strip する。
      def self.entry_labels(entries)
        return [] unless entries.is_a?(Array)

        entries.map { |entry| entry.is_a?(Hash) ? squish(entry["label"]) : "" }.reject(&:empty?).uniq
      end

      # { "label" => ... } 形式のネストしたキーから label を安全に取り出す（キーごと欠けていても落ちない）。
      def self.nested_label(project, key)
        squish(dig_nested_hash(project, key, "label"))
      end

      # 金額は数値で入る（例 minBudget.id => 650000）。nil・0・数値でない値はnilにして、
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

      # 日時は "2026-09-10T19:13:39.825+09:00" / "2026-09-10" 形式。壊れた値・nilはnilにするだけで落とさない。
      def self.parse_date(text)
        Date.parse(text.to_s)
      rescue ArgumentError, TypeError
        nil
      end

      def self.squish(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      private

      # 一覧を順に読み、URLキーで候補を集める（同一案件は最初に出た一覧の category_hint を保持。
      # 並びは search_targets → 全体新着なので、スキル一覧由来の hint が優先される）。
      # 全体新着一覧にしか無い案件は createdAt が直近 latest_lookback_days 日のものだけ採る。
      def collect_candidates(fetch_failures)
        candidates = {}

        @search_targets.each do |target|
          skill_list_postings(target[:skill_ids], target[:category_hint], fetch_failures).each do |posting|
            candidates[posting.url] ||= posting
          end
        end

        if @include_latest_list
          latest_list_postings(fetch_failures).each do |posting|
            next if candidates.key?(posting.url) || !recently_created?(posting)

            candidates[posting.url] = posting
          end
        end

        candidates.values
      end

      # スキル一覧はページ送りが効くので、1ページ目から最大 MAX_LIST_PAGES_PER_TARGET ページまで
      # 読み進める。打ち切り判定は「そのページの生の projectList に state=="opened" が1件も無いか」
      # で行う（契約形態フィルタ後の postings で判定すると、opened だが業務委託系でない案件
      # （正社員等）しか無いページで postings が空になり、まだ次ページに opened が残っているのに
      # 誤って打ち切ってしまうため）。
      def skill_list_postings(skill_ids, category_hint, fetch_failures)
        postings = []

        (1..MAX_LIST_PAGES_PER_TARGET).each do |page_number|
          body = fetch_list_body(skill_list_page_url(skill_ids, page_number), fetch_failures)
          break if body.nil?

          raw_project_list = self.class.project_list(body)
          postings.concat(self.class.parse(body, today: @today, category_hint: category_hint))
          break unless raw_project_list.any? { |project| project["state"] == OPENED_STATE }
        end

        postings
      end

      # 全体新着一覧はページングしない（1ページ目のみ）。
      def latest_list_postings(fetch_failures)
        body = fetch_list_body(latest_list_page_url, fetch_failures)
        return [] if body.nil?

        self.class.parse(body, today: @today, category_hint: nil)
      end

      # 一覧APIの取得。x-requested-with ヘッダを付けないとSSRの通常ページが返ってしまう。
      def fetch_list_body(url, fetch_failures)
        fetch_body(url, fetch_failures, "この一覧をスキップします", headers: LIST_REQUEST_HEADERS)
      end

      # 一覧の posted_on は createdAt 由来。日付が読めない案件は新着かどうか判断できないので採らない。
      def recently_created?(posting)
        posting.posted_on && posting.posted_on >= @today - @latest_lookback_days
      end

      # 候補を新しい順（createdAt 降順、同日は一覧の出現順）に並べ、予算内の件数だけ詳細を取る。
      # 予算から溢れた古い候補と、詳細取得に失敗した候補は一覧の結果（skills 空・本文なし）をそのまま採用する。
      # 詳細で募集終了（state が opened 以外）になっていた案件は、一覧取得〜詳細取得の間に締め切られた
      # ものなので落とす。
      def fetch_details(candidates, fetch_failures)
        sorted_candidates = candidates.each_with_index.sort_by do |posting, index|
          [posting.posted_on ? -posting.posted_on.jd : 0, index]
        end.map(&:first)

        detail_limit = detail_fetch_limit
        sorted_candidates.each_with_index.filter_map do |posting, index|
          next posting if index >= detail_limit

          detail_posting = fetch_detail_posting(posting, fetch_failures)
          next posting if detail_posting.nil?

          detail_posting.application_status == "募集中" ? detail_posting : nil
        end
      end

      # 詳細に使えるリクエスト数。一覧の本数（最悪ケース＝全ページ opened が続いた場合）を
      # 差し引いて REQUEST_BUDGET を超えないようにする。
      def detail_fetch_limit
        list_request_count = @search_targets.size * MAX_LIST_PAGES_PER_TARGET + (@include_latest_list ? 1 : 0)
        [@max_detail_fetches, REQUEST_BUDGET - list_request_count].min.clamp(0..)
      end

      # 詳細1件の取得＋解析。取得に失敗した・JSONが読めなかった場合は nil を返し、呼び出し側が一覧の結果を使う。
      def fetch_detail_posting(posting, fetch_failures)
        body = fetch_body(posting.url, fetch_failures, "この案件は一覧の内容で登録します")
        return nil if body.nil?

        self.class.parse_detail(body, today: @today, category_hint: posting.category_hint)
      end

      # 1回だけ取り直しても失敗したURLは、失敗を記録して nil を返す（その単位だけ打ち切る）。
      # rescueの範囲が@fetcher.getの1回だけなので、StandardErrorで受けてもパース側のバグは覆い隠さない
      # （HTTPエラーのFetchErrorだけでなく、通信層のタイムアウト・切断も同じ扱いにしたいため広く受ける）。
      # headersは一覧APIのみ渡す（詳細ページ取得は従来どおりヘッダなし）。
      def fetch_body(url, fetch_failures, skip_message, headers: {})
        get_with_single_retry(url, headers)
      rescue FreelanceJobs::AccessBlockedError
        raise
      rescue StandardError => error
        fetch_failures << error
        FreelanceJobs.logger.warn("[FreelanceJobs::Sources::Sokudan] #{error.message} #{skip_message}")
        nil
      end

      # 散発的な失敗は1回だけ取り直す。HttpFetcherが2回目以降のリクエスト前に
      # REQUEST_INTERVAL秒 sleep するため、ここで追加のsleepは要らない。
      # WAFのアクセス制限は取り直しても解消しないので、AccessBlockedErrorはそのまま送出する。
      def get_with_single_retry(url, headers)
        @fetcher.get(url, headers: headers)
      rescue FreelanceJobs::AccessBlockedError
        raise
      rescue StandardError => error
        FreelanceJobs.logger.warn("[FreelanceJobs::Sources::Sokudan] #{error.message} 1回だけ再取得します")
        @fetcher.get(url, headers: headers)
      end

      # スキル一覧APIのURL。searchable_language_skill_ids を skill_ids の配列順に繰り返し、末尾に page を付ける。
      def skill_list_page_url(skill_ids, page_number)
        query = skill_ids.map { |skill_id| "search_project[searchable_language_skill_ids][]=#{skill_id}" }.join("&")
        "#{BASE_URL}#{LIST_API_PATH}?#{query}&page=#{page_number}"
      end

      # 全体新着一覧APIのURL（スキル指定なし）。
      def latest_list_page_url(page_number = 1)
        "#{BASE_URL}#{LIST_API_PATH}?page=#{page_number}"
      end
    end
  end
end
