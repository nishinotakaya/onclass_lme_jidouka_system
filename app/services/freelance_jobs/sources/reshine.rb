# frozen_string_literal: true

require "date"
require "json"

module FreelanceJobs
  module Sources
    # re:shine（https://www.re-shine.jp/）はVite製SPAでログイン必須のため、HTMLは読まず、
    # Firebase(identitytoolkit)でサインインしてidTokenを取得したうえで api.re-shine.jp の
    # JSON一覧APIを叩く。
    #
    # ⚠ 秘密情報の扱い: ENVに入れたメールアドレス・パスワードや、サインインで得たidTokenは
    #   例外メッセージ・ログに一切出さない（漏れるとアカウント乗っ取りに直結するため）。
    #   post_jsonのリクエスト本文をinspect等でログに出さないこと。
    class Reshine
      SITE_NAME = "re:shine"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      # 一覧APIの最大取得ページ数。空配列が返れば手前で打ち切るため、通常はここまで到達しない。
      MAX_PAGES = 10

      # Firebase Web APIキーはサイトの公開JS（Firebase Web SDK初期化コード）に埋め込まれた
      # 公開識別子であり秘密情報ではないが、コミット前の秘密情報ガード（gitleaksのgcp-api-key
      # 規則）に当たるためコードには置かず、ENV（RESHINE_FIREBASE_API_KEY）で渡す。

      SIGN_IN_URL = "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword"
      LIST_API_URL = "https://api.re-shine.jp/projects"
      BASE_PROJECT_URL = "https://www.re-shine.jp/projects"

      MISSING_CREDENTIALS_MESSAGE = "RESHINE_EMAIL / RESHINE_PASSWORD / RESHINE_FIREBASE_API_KEY が未設定です"

      OPEN_STATUS = "募集中"
      WORK_FORMAT = "日額制（業務委託）"

      # remote_workの3値。descriptionの「リモート: <表記>」とtagsの「リモート<表記>」の両方で使う。
      REMOTE_WORK_LABELS = {
        "possible" => "可",
        "maybe" => "相談",
        "impossible" => "不可"
      }.freeze

      def initialize(fetcher:, today:)
        @fetcher = fetcher
        @today = today
      end

      # 通信あり。ENVの認証情報でFirebaseにサインインし（fetch1回につき1回だけ）、
      # 一覧APIをpage=1から空配列が返るまで（最大MAX_PAGESページ）たどる。
      # 全ページの結果をURLキーで重複排除して返す。
      def fetch
        email = env_credential("RESHINE_EMAIL")
        password = env_credential("RESHINE_PASSWORD")
        firebase_api_key = env_credential("RESHINE_FIREBASE_API_KEY")
        if email.nil? || password.nil? || firebase_api_key.nil?
          raise FreelanceJobs::FetchError, MISSING_CREDENTIALS_MESSAGE
        end

        id_token = sign_in(email, password)
        fetch_projects(id_token)
      end

      # 通信なし（テスト用）。一覧API 1ページ分のJSON本文から案件一覧を作る。
      # todayは全取得元共通のインターフェースとして受け取るが、掲載日は published_at から取れるため
      # この取得元では参照しない。
      def self.parse(body, today:, category_hint: nil)
        build_postings(project_entries(body), category_hint)
      end

      # レスポンス本文（JSON配列）を案件Hashの配列にする。配列でない・要素がHashでない・JSONが壊れている
      # 場合は例外にせず空配列を返す（構造が変わったときは「0件になる」形で表面化させ、バッチ全体を落とさない）。
      def self.project_entries(body)
        payload = JSON.parse(body)
        payload.is_a?(Array) ? payload.select { |entry| entry.is_a?(Hash) } : []
      rescue JSON::ParserError, TypeError
        []
      end

      def self.build_postings(projects, category_hint)
        postings = {}

        projects.each do |project|
          posting = build_posting(project, category_hint)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      # 非公開（status != "public"）・未掲載（published != true）の案件は除外する。
      # labelとnameは詳細URL・案件名として最低限必要な必須要素なので、欠けた案件は黙って除外する。
      def self.build_posting(project, category_hint)
        return nil unless project["status"] == "public" && project["published"] == true

        label = project["label"].to_s.strip
        title = squish(project["name"])
        return nil if label.empty? || title.empty?

        required_skills = skill_names(project["required_skills"])
        desired_skills = skill_names(project["desired_skills"])
        remote_work_label = remote_work_label(project["remote_work"])

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: build_url(label),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(
            build_description(project, required_skills, desired_skills, remote_work_label)
          ),
          category_hint: category_hint,
          reward: build_reward(project["min_daily_price"], project["max_daily_price"]),
          work_format: WORK_FORMAT,
          application_status: OPEN_STATUS,
          deadline_text: "-", # 応募締切の概念が無いサイト
          deadline_on: nil,
          skills: required_skills,
          client: nested_value(project["corporation"], "name"),
          tags: build_tags(project, remote_work_label),
          posted_on: parse_date(project["published_at"])
        )
      end

      # 案件詳細URLは /projects/<label>/ の形（末尾スラッシュ付きで統一する）。
      # JobPosting.normalize_urlはscheme/hostの正規化とクエリ除去のために末尾スラッシュを落とすので、
      # その後に付け直す（Sources::PeBank.normalize_job_urlと同じ流儀）。
      def self.build_url(label)
        normalized_url = FreelanceJobs::JobPosting.normalize_url("#{BASE_PROJECT_URL}/#{label}/")
        return normalized_url if normalized_url.empty? || normalized_url.end_with?("/")

        "#{normalized_url}/"
      end

      # description は EngineerClassifier の判定テキストにそのまま入るため、職種・スキル・稼働日数・
      # リモート可否・勤務地を本文より前に置く。本文は改行入りなので normalize_description で畳まれる。
      def self.build_description(project, required_skills, desired_skills, remote_work_label)
        job_class_name = nested_value(project["job_class"], "name")
        location = squish(project["location"])
        body_text = project["description"].to_s.strip

        "職種: #{job_class_name} / " \
          "必須スキル: #{required_skills.join(" / ")} / " \
          "歓迎スキル: #{desired_skills.join(" / ")} / " \
          "稼働: 週#{project["min_working_days"]}〜#{project["max_working_days"]}日 / " \
          "リモート: #{remote_work_label} / " \
          "勤務地: #{location} / " \
          "本文: #{body_text}"
      end

      # 自社開発・正社員転換相談可は該当するときだけ付ける。リモート表記は「リモート可」のように
      # description用ラベルの前に「リモート」を足すだけ（両者で表記を揃えるため）。
      def self.build_tags(project, remote_work_label)
        tags = remote_work_label.empty? ? [] : ["リモート#{remote_work_label}"]
        tags << "自社開発" if project["in_house_type"] == "in_house"
        tags << "正社員転換相談可" if project["transition_recruitment_type"] == "can_be_considered"
        tags
      end

      # 「50,000〜70,000円／日」の形にする。min_daily_priceが無ければ金額不明として"要確認"にする。
      def self.build_reward(min_daily_price, max_daily_price)
        return "要確認" unless min_daily_price

        "#{FreelanceJobs.format_number(min_daily_price)}〜#{FreelanceJobs.format_number(max_daily_price)}円／日"
      end

      def self.remote_work_label(remote_work)
        text = squish(remote_work)
        REMOTE_WORK_LABELS.fetch(text, text)
      end

      # [{ "name" => ... }] の配列から値だけを取り出す（配列でない・要素がHashでない・値が空なら捨てる）。
      def self.skill_names(skills)
        return [] unless skills.is_a?(Array)

        skills.map { |skill| skill.is_a?(Hash) ? squish(skill["name"]) : "" }.reject(&:empty?)
      end

      # { "name" => ... } 形式のネストしたHashから値を安全に取り出す（キーごと欠けていても落ちない）。
      def self.nested_value(nested_hash, key)
        nested_hash.is_a?(Hash) ? squish(nested_hash[key]) : ""
      end

      # published_at（例 "2026-09-01T00:00:00.000Z"）をDateにする。壊れた値・空はnilにする。
      def self.parse_date(text)
        stripped_text = text.to_s.strip
        return nil if stripped_text.empty?

        Date.parse(stripped_text)
      rescue ArgumentError, TypeError
        nil
      end

      def self.squish(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      private

      # 一覧APIをpage=1から空配列が返るまでたどる（最大MAX_PAGESページ）。
      def fetch_projects(id_token)
        postings = {}
        headers = { "Authorization" => "Bearer #{id_token}", "Accept" => "application/json" }

        (1..MAX_PAGES).each do |page_number|
          body = @fetcher.get(list_url(page_number), headers: headers)
          entries = self.class.project_entries(body)
          break if entries.empty?

          self.class.build_postings(entries, nil).each { |posting| postings[posting.url] ||= posting }
        end

        postings.values
      end

      def list_url(page_number)
        "#{LIST_API_URL}?page=#{page_number}"
      end

      # Firebaseにサインインしてidトークンを取り出す（fetch1回につき1回だけ呼ぶ）。
      # 失敗時はpost_jsonが投げた例外（メッセージ先頭が"HTTP <status> ..."）からステータスだけを
      # 取り出して訳し直す。email・password・元の例外メッセージ（idTokenやURLを含みうる）は
      # 一切含めない。
      def sign_in(email, password)
        response_body = @fetcher.post_json(
          sign_in_url,
          { "email" => email, "password" => password, "returnSecureToken" => true }
        )
        JSON.parse(response_body)["idToken"]
      rescue FreelanceJobs::FetchError => error
        raise FreelanceJobs::FetchError, "re:shine のサインインに失敗しました（HTTP #{sign_in_failure_status(error)}）"
      end

      def sign_in_failure_status(error)
        error.message[/\AHTTP (\d+)/, 1] || "不明"
      end

      def sign_in_url
        "#{SIGN_IN_URL}?key=#{env_credential("RESHINE_FIREBASE_API_KEY")}"
      end

      # ENV値の空白・空文字・nilをまとめてnil扱いにする（認証情報の未設定判定に使う）。
      def env_credential(key)
        value = ENV[key]
        return nil if value.nil? || value.strip.empty?

        value
      end
    end
  end
end
