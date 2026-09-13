# frozen_string_literal: true

require "nokogiri"
require "json"
require "date"

module FreelanceJobs
  module Sources
    # Findy Freelance: Next.js の SSR ページ。案件カードのHTMLをパースするのではなく、
    # `script#__NEXT_DATA__`（type="application/json"）に埋まっている生JSONを読む。
    # 案件配列は props.pageProps.initialWorkList にあり、1ページ目の10件が入っている。
    #
    # HTML/JSON構造の前提（壊れたらここを疑う）:
    #   - script#__NEXT_DATA__ は1ページに1個。中身はJSON全体（buildId・page・query等も含む）
    #   - props.pageProps.initialWorkList … 案件の配列（1ページ10件）
    #   - props.pageProps.slug / name / initialTotalPagesCount … 言語スラッグ・表示名・総ページ数
    #   - 案件詳細URLは id ではなく workHash から組み立てる（HTML内の <a href="/works/_fhNZS9onGdfx"> と一致）。
    #     workHash は `_` 始まり・`_` 終わりがあり得る英数字13文字なので、正規表現で絞らずそのまま連結する
    #
    # ページングは使えない（実測）: `?page=2` はHTTP 200だがSSRが無視して1ページ目と同一の
    # initialWorkList を返し、`/works/languages/ruby/2` は308リダイレクトになる。2ページ目以降は
    # クライアント側のGraphQL取得のため、ここでは1ページ目だけを取る。並びは openedAt 降順
    # （新着順）なので「毎朝の新着差分取り」用途として成立する。1一覧=1リクエスト。
    class FindyFreelance
      SITE_NAME = "Findy Freelance"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://freelance.findy-code.io"

      # 一覧ページのパス。ReactはFindy上「言語」ではなく「スキル」扱いのため
      # /works/languages/react は存在せず、/works/skills/react になる（sitemap.xmlで確認済み）。
      DEFAULT_SEARCH_TARGETS = [
        { path: "/works/languages/ruby", hint: "Ruby" },
        { path: "/works/languages/typescript", hint: "TypeScript" },
        { path: "/works/skills/react", hint: "React" }
      ].freeze

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
      end

      # 通信あり。一覧ごとに1ページ目だけを取得する（合計 search_targets 件のリクエスト）。
      # Ruby一覧とReact一覧には同じ案件が現れるため、URLをキーに重複排除する。
      def fetch
        postings = {}

        @search_targets.each do |target|
          body = @fetcher.get(list_url(target[:path]))
          self.class.parse(body, today: @today, category_hint: target[:hint]).each do |posting|
            postings[posting.url] ||= posting
          end
        end

        postings.values
      end

      # 通信なし（テスト用）。一覧1ページ分のHTML本文から案件一覧を作る。
      # todayは全取得元共通のインターフェースとして受け取るが、Findyは締切の概念が無く
      # posted_onもJSONの日時をそのまま使うため、この取得元では参照しない。
      def self.parse(body, today:, category_hint: nil)
        postings = {}

        work_list(body).each do |work|
          posting = build_posting(work, category_hint)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      # __NEXT_DATA__ から案件配列を取り出す。script要素が無い・JSONが壊れている・
      # 期待キーが配列でない場合は例外にせず空配列を返す（サイト構造が変わったときは
      # 「0件になる」形で表面化させ、バッチ全体を落とさない）。
      def self.work_list(body)
        script_node = Nokogiri::HTML(body).at_css("script#__NEXT_DATA__")
        return [] unless script_node

        work_list = dig_nested_hash(JSON.parse(script_node.text), "props", "pageProps", "initialWorkList")
        work_list.is_a?(Array) ? work_list.select { |work| work.is_a?(Hash) } : []
      rescue JSON::ParserError
        []
      end

      # ネストしたHashを順に辿る。Hash#digを直接使うと、途中の値が配列や文字列だったときに
      # TypeError/NoMethodErrorで落ちてしまい「構造が変わったら0件」という上記の方針が崩れるため、
      # 各段でHashであることを確かめながら降りる。
      def self.dig_nested_hash(payload, *keys)
        current_value = payload
        keys.each do |key|
          return nil unless current_value.is_a?(Hash)

          current_value = current_value[key]
        end
        current_value
      end

      def self.build_posting(work, category_hint)
        work_hash = work["workHash"].to_s.strip
        title = work["title"].to_s.gsub(/\s+/, " ").strip
        # workHash（詳細URLの識別子）とtitleは案件行として最低限必要な必須要素。
        # JSONのキー名が変わってどちらかが取れなくなったカードは、行を壊さないよう黙って除外する。
        return nil if work_hash.empty? || title.empty?

        skills = build_skills(work)
        max_monthly_wage = positive_amount(work["maxMonthlyWage"])
        max_hourly_wage = positive_amount(work["maxHourlyWage"])

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}/works/#{work_hash}"),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(build_description(work, skills)),
          category_hint: category_hint,
          reward: build_reward(max_monthly_wage, max_hourly_wage),
          work_format: work_format(max_monthly_wage, max_hourly_wage),
          application_status: "-", # 応募数・提案数はJSONに含まれない
          deadline_text: "-",      # 応募締切の概念が無いサイト
          deadline_on: nil,
          skills: skills,
          client: "",              # company に name キーが無く、発注企業名は非公開
          tags: build_tags(work),
          posted_on: parse_date(work["openedAt"])
        )
      end

      # description は EngineerClassifier の判定テキストにそのまま入るので、技術名が載る
      # 使用技術・募集職種を先に置く。稼働日数は LONG_TERM_RE（週\s*[1-5]\s*日）、
      # 勤務形態は REMOTE_RE（リモート）に当たり、memoの「長期・継続あり」「リモート可」になる。
      #
      # company.profile.businessAbstract（事業内容）は意図的に含めない。
      # 「法人営業の新規開拓を効率化するWebサービス」のような事業内容が NON_DEV_RE の「営業」に
      # 誤爆し、案件が分類されず丸ごと落ちるため（実測でRuby一覧の分類成功が9/10→10/10に改善）。
      def self.build_description(work, skills)
        job_type_name = nested_name(work, "jobType")
        working_days = working_days_text(work)
        remote_work_name = nested_name(work, "remoteWork")
        participation_benefits = work["participationBenefits"].to_s.strip

        parts = []
        parts << "募集職種: #{job_type_name}" unless job_type_name.empty?
        parts << "使用技術: #{skills.join(" / ")}" unless skills.empty?
        parts << "稼働: #{working_days}" unless working_days.empty?
        parts << "勤務形態: #{remote_work_name}" unless remote_work_name.empty?
        parts << "参画メリット: #{participation_benefits}" unless participation_benefits.empty?
        parts.join(" / ")
      end

      # 稼働日数は minDaysPerWeek / maxDaysPerWeek の2キー。同値なら「週5日」、
      # 幅があれば「週4〜5日」、どちらも取れなければ空文字（description から省く）。
      def self.working_days_text(work)
        days_per_week = [positive_amount(work["minDaysPerWeek"]), positive_amount(work["maxDaysPerWeek"])].compact.uniq
        return "" if days_per_week.empty?

        "週#{days_per_week.join("〜")}日"
      end

      # developmentLanguages（言語）と developmentSkills（フレームワーク・インフラ）を連結する。
      # どちらも [{ "id" =>, "name" =>, "__typename" => }] の配列で、developmentSkills は空配列のことがある。
      # 言語とスキルに同じ名前が入る場合に備えて uniq する。
      def self.build_skills(work)
        (entry_names(work["developmentLanguages"]) + entry_names(work["developmentSkills"])).uniq
      end

      # 案件特徴（workCharacteristics）をタグにする。新着フラグ isNewOpened は特徴タグに
      # 含まれないため、trueのときだけ先頭に "NEW" を足す。
      def self.build_tags(work)
        characteristic_names = entry_names(work["workCharacteristics"])
        work["isNewOpened"] ? ["NEW", *characteristic_names] : characteristic_names
      end

      # [{ "name" => ... }] の配列から name だけを取り出す（要素がHashでない・nameが空なら捨てる）。
      def self.entry_names(entries)
        return [] unless entries.is_a?(Array)

        entries.map { |entry| entry.is_a?(Hash) ? entry["name"].to_s.strip : "" }.reject(&:empty?)
      end

      # { "name" => ... } 形式のネストしたキーから name を安全に取り出す（キーごと欠けていても落ちない）。
      def self.nested_name(work, key)
        value = work[key]
        value.is_a?(Hash) ? value["name"].to_s.strip : ""
      end

      # 報酬は必ず月額（maxMonthlyWage）を優先する。
      # EngineerClassifier.high_reward? は /時給|時間単価|時間報酬/ に一致しない表記を月額とみなして
      # 閾値300,000円で判定するため、「〜8,000円／時」を入れると高単価判定が必ず落ちる。
      # Findyは maxMonthlyWage == maxHourlyWage * 160 で両方持ち、サイト表示も月額なので月額を採る。
      def self.build_reward(max_monthly_wage, max_hourly_wage)
        return "〜#{with_thousands_separator(max_monthly_wage)}円／月" if max_monthly_wage
        return "〜#{with_thousands_separator(max_hourly_wage)}円／時" if max_hourly_wage

        "要確認"
      end

      # 単価表記と同じ分岐で契約形態を決める（月額が出ていれば月額制、時給のみなら時間単価制）。
      def self.work_format(max_monthly_wage, max_hourly_wage)
        return "月額制（業務委託）" if max_monthly_wage
        return "時間単価制" if max_hourly_wage

        "業務委託（フリーランス）"
      end

      # 金額・日数は数値で入る（例 maxMonthlyWage => 1280000）。nil・0・数値でない値はnilにして、
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

      # openedAt は "2026-09-11T16:02:32+09:00" 形式。壊れた値・nilはnilにするだけで落とさない。
      def self.parse_date(text)
        Date.parse(text.to_s)
      rescue ArgumentError, TypeError
        nil
      end

      def list_url(path)
        "#{BASE_URL}#{path}"
      end
      private :list_url
    end
  end
end
