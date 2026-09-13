# frozen_string_literal: true

require "json"

module FreelanceJobs
  module Sources
    # クラウドテック（クラウドワークス運営のフリーランスエージェント。tech.crowdworks.jp）。
    # 既存の Crowdworks（crowdworks.jp のクラウドソーシング）とは別サイトなので SITE_NAME も分けている。
    #
    # 一覧ページ /job_offers は Vite 製 SPA の空シェル（<div id="app"> のみで案件リンクが1本も無い）
    # のため、HTMLは読まず、SPAが叩いている公開JSON API を直接呼ぶ。
    #   GET /api/v1/users/job_offers?skill_ids[]=<id>&openonly=true&order=newest_first&page=<n>
    # Cookie・CSRFトークン不要（2026-09-13 に curl で 200 を確認）。レスポンスはページ情報を持たない
    # 「案件Hashの裸の配列」で、1ページ20件固定。
    #
    # JSON構造の前提（壊れたらここを疑う。キーは実データ42件で全件存在・nilゼロ）:
    #   id (Integer)                 … 案件ID。詳細URLは /job_offers/<id>（sitemap・canonicalと同じ正規形。
    #                                  SPA内リンクの /job_offers/<職種略号>/<id> は使わない）
    #   title (String)               … 例 "【Ruby/週5日/一部リモート/恵比寿】…業務案件"
    #   maxUnitPrice (Integer)       … 月額単価の上限（円）。一覧UIの並び替えラベルが「月額単価」なので月額
    #   open (Boolean)               … 募集中か。openonly=true を付けるので通常すべて true
    #   new (Boolean)                … 新着バッジ
    #   description (String)         … 本文（改行入り。■期待するミッション／■開発環境／■求めるスキル 等）
    #   officeLocation (String)      … 駅名 または "フルリモート"
    #   requiredWorkingDays (Integer)… 週の稼働日数（3/4/5）
    #   workStyle (String)           … "remote_work" / "partial_remote_work" / "office_work" の3値
    #   occupation { id, name }      … 募集職種
    #   skills [{ id, name }]        … 使用技術
    #   appeals [{ id, description }]… こだわり条件（"服装自由" "長期案件" 等）
    #
    # ★絞り込みは全文検索（word=）ではなく skill_ids[] のタクソノミー絞り込みを使う。
    #   word=React は本文に "FW: React" と一文あるだけの Delphi 案件まで拾い（実測 10/20 がノイズ）、
    #   EngineerClassifier が description 一致 +1 点で誤分類してしまうため。
    # ★skill_ids[] に渡すIDは、案件JSONの skills[].id（22=Ruby, 357=TypeScript 等）ではなく
    #   /api/v1/users/top/job_offer_search/master_skills が返す**別のID体系**である点に注意。
    #   案件側のIDを渡すと HTTP 404 になる（2026-09-13 実測）。マスターIDの Ruby は Rails のみの案件も、
    #   React は Next.js のみの案件も含むので、技術ごとに1つのIDで足りる。
    class Crowdtech
      SITE_NAME = "クラウドテック"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://tech.crowdworks.jp"
      LIST_API_PATH = "/api/v1/users/job_offers"

      # Accept を付けずに呼んだ場合の挙動は未検証（付ければ確実に JSON が返る）ので必ず付ける。
      REQUEST_HEADERS = { "Accept" => "application/json" }.freeze

      # master_skill_id の出どころは /api/v1/users/top/job_offer_search/master_skills
      # （2026-09-13 時点: 3=Ruby / 15=TypeScript / 103=React。他に 1=Java / 4=Python / 6=Go /
      #   11=Node.js / 123=Vue.js / 315=Amazon Web Service）。壊れたら同エンドポイントから再導出できる。
      # 実測の該当件数（募集中のみ）は Ruby 19件 / TypeScript 20件以上 / React 20件以上。
      DEFAULT_SEARCH_TARGETS = [
        { master_skill_id: 3,   hint: "Ruby" },
        { master_skill_id: 15,  hint: "TypeScript" },
        { master_skill_id: 103, hint: "React" }
      ].freeze

      # APIの1ページあたりの件数（固定）。返却件数がこれに満たなければ最終ページとみなす。
      PAGE_SIZE = 20
      # 1絞り込みあたりの最大ページ数。新着順（order=newest_first）なので先頭ページだけで新着を拾える。
      # 3絞り込み×2ページ＝最大6リクエスト/バッチ。
      MAX_PAGES = 2

      # 存在しないページ番号（および該当0件の絞り込み）は HTTP 404 ＋ body "[]" で返る（実測）。
      # HttpFetcher は非200を FetchError("HTTP <status> <url>") として送出するので、
      # メッセージ先頭のステータスで404を見分ける（HttpFetcher がステータスを公開していないため）。
      NOT_FOUND_MESSAGE_RE = /\AHTTP 404 /

      # workStyle は3値固定（実測42件）。未知の値が来たら訳さずそのまま出す。
      WORK_STYLE_LABELS = {
        "remote_work" => "フルリモート",
        "partial_remote_work" => "一部リモート",
        "office_work" => "常駐（出社）"
      }.freeze

      OPEN_STATUS = "募集中"
      CLOSED_STATUS = "募集終了"

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, max_pages: MAX_PAGES)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @max_pages = max_pages
      end

      # 通信あり。絞り込みごとに一覧APIを1ページ目から max_pages ページ分たどり、URLキーで重複排除する
      # （Ruby と React の両方に載る案件があるため重複排除は必須）。
      # ページ単位の取得失敗は握りつぶして次の絞り込みへ進むが、1件も取れずに失敗だけが残った場合は
      # 最初の失敗を送出し、ResearchService に「取得失敗」として記録させる
      # （黙って0件を返すと、APIやマスターIDの変更に気付けなくなるため）。
      def fetch
        postings = {}
        fetch_failures = []

        @search_targets.each do |target|
          fetch_target(target, fetch_failures).each { |posting| postings[posting.url] ||= posting }
        end

        raise fetch_failures.first if postings.empty? && !fetch_failures.empty?

        postings.values
      end

      # 通信なし（テスト用）。一覧API 1ページ分のJSON本文から案件一覧を作る。
      # today は全取得元共通のインターフェースとして受け取るが、クラウドテックは締切の概念が無く
      # 掲載日も一覧JSONに含まれないため、この取得元では参照しない。
      def self.parse(body, today:, category_hint: nil)
        build_postings(job_offer_entries(body), category_hint)
      end

      # レスポンス本文（JSON配列）を案件Hashの配列にする。配列でない・要素がHashでない・JSONが壊れている
      # 場合は例外にせず空配列を返す（構造が変わったときは「0件になる」形で表面化させ、バッチ全体を落とさない）。
      def self.job_offer_entries(body)
        payload = JSON.parse(body)
        payload.is_a?(Array) ? payload.select { |entry| entry.is_a?(Hash) } : []
      rescue JSON::ParserError, TypeError
        []
      end

      def self.build_postings(job_offers, category_hint)
        postings = {}

        job_offers.each do |job_offer|
          posting = build_posting(job_offer, category_hint)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      # 案件1件をJobPostingに組み立てる。id（詳細URLの識別子）と title は案件行として最低限必要な
      # 必須要素なので、キー名が変わってどちらかが取れなくなった要素は行を壊さないよう黙って除外する
      # （全件が欠損した場合は0件になり、呼び出し側のログで気付ける）。
      def self.build_posting(job_offer, category_hint)
        job_offer_id = positive_amount(job_offer["id"])
        title = squish(job_offer["title"])
        return nil if job_offer_id.nil? || title.empty?

        skills = entry_values(job_offer["skills"], "name")
        max_unit_price = positive_amount(job_offer["maxUnitPrice"])
        work_style_label = work_style_label(job_offer["workStyle"])

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}/job_offers/#{job_offer_id}"),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(
            build_description(job_offer, skills, work_style_label)
          ),
          category_hint: category_hint,
          reward: build_reward(max_unit_price),
          work_format: work_format(max_unit_price),
          application_status: application_status(job_offer["open"]),
          deadline_text: "-", # 応募締切の概念が無いサイト
          deadline_on: nil,
          skills: skills,
          client: "",         # 詳細ページの JSON-LD hiringOrganization も "社名非公開" で発注企業名は取れない
          tags: build_tags(job_offer, work_style_label),
          posted_on: nil      # 一覧JSONに掲載日が無い（詳細ページの JSON-LD datePosted は1件1リクエストになるので取らない）
        )
      end

      # description は EngineerClassifier の判定テキストにそのまま入り、RowBuilder がシート用に先頭160文字で
      # 切るため、技術名が載る募集職種・使用技術と、稼働（LONG_TERM_RE「週N日」）・勤務形態
      # （REMOTE_RE「リモート」）を本文より前に置く。本文は改行入りなので normalize_description で畳まれる。
      def self.build_description(job_offer, skills, work_style_label)
        occupation_name = nested_value(job_offer["occupation"], "name")
        working_days = positive_amount(job_offer["requiredWorkingDays"])
        office_location = squish(job_offer["officeLocation"])
        body_text = job_offer["description"].to_s.strip

        parts = []
        parts << "募集職種: #{occupation_name}" unless occupation_name.empty?
        parts << "使用技術: #{skills.join(" / ")}" unless skills.empty?
        parts << "稼働: 週#{working_days}日" if working_days
        parts << "勤務形態: #{work_style_label}" unless work_style_label.empty?
        parts << "勤務地: #{office_location}" unless office_location.empty?
        parts << body_text unless body_text.empty?
        parts.join(" / ")
      end

      # 新着バッジ（new）はこだわり条件に含まれないため、true のときだけ先頭に "NEW" を足す。
      # 勤務形態もタグに入れておく（シート上でリモート可否を一目で分かるようにするため）。
      def self.build_tags(job_offer, work_style_label)
        appeal_descriptions = entry_values(job_offer["appeals"], "description")
        tags = work_style_label.empty? ? appeal_descriptions : [work_style_label, *appeal_descriptions]
        job_offer["new"] == true ? ["NEW", *tags] : tags
      end

      # 月額上限のみのサイトなので「〜1,000,000円／月」の形にする（EngineerClassifier.high_reward? は
      # カンマを除いた最初の数値を月額として300,000円の閾値で判定する）。金額が無い・0 なら "要確認"。
      def self.build_reward(max_unit_price)
        return "要確認" unless max_unit_price

        "〜#{FreelanceJobs.format_number(max_unit_price)}円／月"
      end

      # 時給表記は存在しないサイトなので、月額があれば月額制、無ければ汎用の業務委託に寄せる。
      def self.work_format(max_unit_price)
        max_unit_price ? "月額制（業務委託）" : "業務委託（フリーランス）"
      end

      # open は Boolean。キーごと欠けていた（nil）場合は判定材料が無いものとして "-" にする。
      def self.application_status(open_flag)
        case open_flag
        when true then OPEN_STATUS
        when false then CLOSED_STATUS
        else "-"
        end
      end

      def self.work_style_label(work_style)
        work_style_text = squish(work_style)
        WORK_STYLE_LABELS.fetch(work_style_text, work_style_text)
      end

      # [{ "name" => ... }] / [{ "description" => ... }] の配列から値だけを取り出す
      # （配列でない・要素がHashでない・値が空なら捨てる）。
      def self.entry_values(entries, key)
        return [] unless entries.is_a?(Array)

        entries.map { |entry| entry.is_a?(Hash) ? squish(entry[key]) : "" }.reject(&:empty?)
      end

      # { "name" => ... } 形式のネストしたHashから値を安全に取り出す（キーごと欠けていても落ちない）。
      def self.nested_value(nested_hash, key)
        nested_hash.is_a?(Hash) ? squish(nested_hash[key]) : ""
      end

      # ID・金額・日数は数値で入る（例 maxUnitPrice => 1000000）。nil・0・数値でない値は nil にして、
      # 呼び出し側の「値なし」分岐に寄せる。
      def self.positive_amount(value)
        return nil unless value.is_a?(Numeric) || value.to_s.match?(/\A\d+\z/)

        amount = value.to_i
        amount.positive? ? amount : nil
      end

      # 表示用の値は前後の空白・改行を畳んでから使う（ノーブレークスペースも畳めるよう [[:space:]]）。
      def self.squish(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      # HttpFetcher が非200で送出する FetchError のうち、404（ページ無し・該当0件）だけを見分ける。
      def self.not_found_error?(error)
        error.is_a?(FreelanceJobs::FetchError) && error.message.match?(NOT_FOUND_MESSAGE_RE)
      end

      private

      # 1絞り込みぶんのページ送り。返却件数が PAGE_SIZE 未満なら最終ページなので、それ以上リクエストしない。
      # ちょうど20件で終わった場合は次ページが 404 になり、fetch_page_body 側で終端として nil が返る。
      def fetch_target(target, fetch_failures)
        postings = []

        (1..@max_pages).each do |page_number|
          body = fetch_page_body(target[:master_skill_id], page_number, fetch_failures)
          break if body.nil?

          job_offers = self.class.job_offer_entries(body)
          break if job_offers.empty?

          postings.concat(self.class.build_postings(job_offers, target[:hint]))
          break if job_offers.size < PAGE_SIZE
        end

        postings
      end

      # 2ページ目以降の 404 は「そのページが無い」＝ページ送りの終端なので失敗には数えない。
      # 1ページ目の 404 は「該当0件」だが、Ruby/TypeScript/React で0件になることは考えにくく、
      # マスタースキルIDの失効を疑うべき状況なので失敗として記録する（全絞り込みが失敗すれば fetch が送出する）。
      # rescue の範囲が @fetcher.get だけなので、StandardError で受けてもパース側のバグは覆い隠さない
      # （HTTPエラーだけでなく、通信層のタイムアウト・切断も同じ扱いにしたいため広く受ける）。
      def fetch_page_body(master_skill_id, page_number, fetch_failures)
        get_with_single_retry(list_url(master_skill_id, page_number))
      rescue FreelanceJobs::AccessBlockedError
        raise
      rescue StandardError => error
        return nil if page_number > 1 && self.class.not_found_error?(error)

        fetch_failures << error
        FreelanceJobs.logger.warn(
          "[FreelanceJobs::Sources::Crowdtech] #{error.message} この絞り込みのページ送りを打ち切ります"
        )
        nil
      end

      # 散発的な通信失敗は1回だけ取り直す。HttpFetcher が2回目以降のリクエスト前に REQUEST_INTERVAL 秒
      # sleep するため、ここで追加の sleep は要らない。
      # 404 はページが無いことを示す決定的な応答なので取り直さない。
      # WAF のアクセス制限は取り直しても解消しないので、AccessBlockedError はそのまま送出する。
      def get_with_single_retry(url)
        @fetcher.get(url, headers: REQUEST_HEADERS)
      rescue FreelanceJobs::AccessBlockedError
        raise
      rescue StandardError => error
        raise if self.class.not_found_error?(error)

        FreelanceJobs.logger.warn("[FreelanceJobs::Sources::Crowdtech] #{error.message} 1回だけ再取得します")
        @fetcher.get(url, headers: REQUEST_HEADERS)
      end

      # "https://tech.crowdworks.jp/api/v1/users/job_offers?skill_ids%5B%5D=3&openonly=true&order=newest_first&page=1"
      # 角括弧はブラウザの URLSearchParams と同じくパーセントエンコード済み（%5B%5D）で書く（この形で200を確認）。
      # openonly=true は募集中のみ、order=newest_first は新着順（既定は max_salary_desc なので明示必須）。
      def list_url(master_skill_id, page_number)
        "#{BASE_URL}#{LIST_API_PATH}?skill_ids%5B%5D=#{master_skill_id}&openonly=true&order=newest_first&page=#{page_number}"
      end
    end
  end
end
