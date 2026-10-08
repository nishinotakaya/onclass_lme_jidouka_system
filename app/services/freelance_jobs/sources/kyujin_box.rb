# frozen_string_literal: true

require "cgi"
require "date"
require "json"
require "nokogiri"

module FreelanceJobs
  module Sources
    # 求人ボックス: 正社員を含む求人の検索一覧（パス型URL）をパースする。
    # 北海道タブで「正社員でもよいのでRubyをできるだけ多く」集めるための取得元（2026-10-08、社長要求）。
    # 業務委託3取得元ではRubyが120行中1件しか取れなかったのに対し、
    # 「Rubyエンジニアの仕事-北海道」だけで2,296件ある（2026-10-08実測）。
    #
    # robots.txt について: /rd/ /jb/ /api/ 等は禁止、Crawl-delay 1。
    # 一覧のパス型URLと `?pg=N` は許可されているため、詳細ページ（/jb/）は取得しない。
    # 一覧カードの data-func-show-arg に求人情報のJSONが丸ごと入っているので、それだけで足りる。
    #
    # ページ送りは `?pg=N`（1頁24件）。2ページ目以降の404とカード0件の頁はページ終端として打ち切る
    # （1ページ目の失敗は障害として例外のまま）。
    class KyujinBox
      # 取得元の識別名（ResearchService・除外設定が参照する）。シートの「掲載サイト」列には使わず、
      # カードの siteName（Green・ビズリーチ等）を載せる。求人ボックス直掲載の行だけこの名前になる。
      SITE_NAME = "求人ボックス"
      # 掲載サイト列に SITE_NAME 以外の値（転載元サイト名）が入ることを ResearchService に伝える印。
      # 印が無いと、それらの行が「取得失敗サイトの行」として期限切れでも永久に残ってしまう。
      def self.reports_original_site_names? = true

      # siteName 末尾の「 - 登録エントリー」（前後空白ゆれ含む）。掲載サイト名としては不要な文言。
      REGISTRATION_ENTRY_SUFFIX = /\s*-\s*登録エントリー\s*\z/
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      # 「求人ボックス.com」のpunycode。
      BASE_URL = "https://xn--pckua2a7gp15o89zb.com"

      DEFAULT_SEARCH_TARGETS = [
        { path_keyword: "Rubyエンジニアの仕事-北海道", hint: "Ruby" }
      ].freeze

      # 30頁×24件=720件。間隔1.5秒で約45秒。
      MAX_PAGES = 30

      CARD_SELECTOR = "section.p-result_card"
      # 求人JSONを持つ要素。カード内の最初の1つ（タイトルリンク a.p-result_title_link）を使う。
      JOB_DATA_SELECTOR = "[data-func-show-arg]"
      JOB_DATA_ATTRIBUTE = "data-func-show-arg"

      # uniqueId が "l" で始まる求人は他サイト（Green等）からの転載で、求人ボックス側に詳細ページが無い。
      AGGREGATED_ID_PREFIX = "l"
      # 技術タグとみなす条件。「未経験OK」のような日本語混じりの待遇タグを除くため、英数字記号だけのタグに限る。
      TECH_TAG_PATTERN = %r{\A[A-Za-z0-9.+#/ -]*[A-Za-z][A-Za-z0-9.+#/ -]*\z}.freeze

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, max_pages: MAX_PAGES)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @max_pages = max_pages
      end

      # 通信あり。キーワード×ページ数ぶん一覧を取得し、URLキーで重複排除する（先に出た方を残す）。
      def fetch
        postings_by_url = {}

        @search_targets.each do |search_target|
          collect_target_postings(search_target, postings_by_url)
        end

        postings_by_url.values
      end

      def collect_target_postings(search_target, postings_by_url)
        (1..@max_pages).each do |page_number|
          begin
            body = @fetcher.get(search_url(search_target[:path_keyword], page_number))
          rescue FreelanceJobs::FetchError => fetch_error
            break if end_of_pages?(fetch_error, page_number)

            raise
          end
          page_postings = self.class.parse(body, today: @today, category_hint: search_target[:hint])
          break if page_postings.empty?

          page_postings.each { |posting| postings_by_url[posting.url] ||= posting }
        end
      end
      private :collect_target_postings

      # 2ページ目以降の 404（"HTTP 404 <url>"）は範囲外ページ＝終端。1ページ目の 404 は本当の障害。
      def end_of_pages?(fetch_error, page_number)
        page_number > 1 && fetch_error.message.match?(/\AHTTP 404 /)
      end
      private :end_of_pages?

      # 一覧URL。CGI.escape はスペースを + にするが、キーワードにスペースは無い（%XX のみになる）。
      def search_url(path_keyword, page_number)
        url = "#{BASE_URL}/#{CGI.escape(path_keyword)}"
        page_number > 1 ? "#{url}?pg=#{page_number}" : url
      end
      private :search_url

      # 通信なし（テスト用）。一覧HTML本文から案件一覧を作る。
      def self.parse(body, today:, category_hint: nil)
        document = Nokogiri::HTML(body)
        postings = {}

        document.css(CARD_SELECTOR).each do |card|
          job = read_job(card)
          next unless job

          posting = build_posting(job, category_hint)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      # カードの data-func-show-arg（JSON）の "json" キー（さらにJSON文字列）を求人Hashにする。
      # 属性が無い・JSONが壊れている・Hashでないカードは nil（呼び出し側で読み飛ばす）。
      def self.read_job(card)
        attribute = card.at_css(JOB_DATA_SELECTOR)&.[](JOB_DATA_ATTRIBUTE)
        return nil if attribute.nil? || attribute.empty?

        job = JSON.parse(JSON.parse(attribute)["json"].to_s)
        job.is_a?(Hash) ? job : nil
      rescue JSON::ParserError, TypeError
        nil
      end

      def self.build_posting(job, category_hint)
        url = job_url(job)
        title = normalize_text(job["title"])
        return nil if url.nil? || title.empty?

        company = normalize_text(job["company"])
        # 一覧では社名が見えないと、どの会社の求人か判別できないため案件名の先頭に付ける。
        titled_with_company = company.empty? ? title : "【#{company}】#{title}"

        feature_tags = Array(job["allFeatureTags"]).map { |tag| normalize_text(tag) }.reject(&:empty?)

        FreelanceJobs::JobPosting.new(
          site: original_site_name(job, company),
          url: FreelanceJobs::JobPosting.normalize_url(url),
          title: titled_with_company,
          description: FreelanceJobs::JobPosting.normalize_description(build_description(job, feature_tags)),
          category_hint: category_hint,
          reward: text_or_default(job["payment"]),
          work_format: text_or_default(job["employType"]),
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: feature_tags.grep(TECH_TAG_PATTERN),
          client: company,
          tags: feature_tags,
          posted_on: parse_date(job["updatedAt"])
        )
      end

      # 掲載サイト名。社長要求で「求人ボックス」ではなく Green 等の転載元サイト名を出す。
      # siteName が空、または直掲載で siteName が会社名と同じ（サイト名ではなく社名が入っている）場合は求人ボックス。
      def self.original_site_name(job, company)
        site_name = normalize_text(job["siteName"]).sub(REGISTRATION_ENTRY_SUFFIX, "").strip
        return SITE_NAME if site_name.empty?

        direct_listing = !job["uniqueId"].to_s.strip.start_with?(AGGREGATED_ID_PREFIX)
        direct_listing && site_name == company ? SITE_NAME : site_name
      end

      # 案件URL。直掲載（uniqueId が l 始まりでない）は求人ボックスの詳細URL、
      # 転載（l 始まり）は掲載元URLをそのまま使う。掲載元URLが空なら nil。
      # SheetMerger は normalize_url でクエリを落としたURLを行キーにするため、
      # クエリでしか求人を区別できない掲載元（ビズリーチ等）は JobPosting.normalize_url 側で識別クエリを残す。
      def self.job_url(job)
        unique_id = job["uniqueId"].to_s.strip
        return nil if unique_id.empty?
        return "#{BASE_URL}/jb/#{unique_id}" unless unique_id.start_with?(AGGREGATED_ID_PREFIX)

        source_url = job["url"].to_s.strip
        source_url.empty? ? nil : source_url
      end

      # 北海道分類器は `勤務地: …` を「/ または改行まで」で読むため、勤務地の値に / を入れず、
      # 勤務地・雇用形態を末尾に置く（勤務地の後ろに長文があっても切れるが、値自体の途中切れを避ける）。
      def self.build_description(job, feature_tags)
        yaml_head = normalize_text(job["firstYamlHead"])
        yaml_content = normalize_text(job["firstYamlContent"])
        yaml_part = if yaml_content.empty?
                      nil
                    else
                      yaml_head.empty? ? yaml_content : "#{yaml_head}: #{yaml_content}"
                    end

        [
          yaml_part,
          labeled("職種", job["jobType"]),
          labeled("会社", job["company"]),
          labeled("掲載元", job["siteName"]),
          feature_tags.empty? ? nil : "特徴: #{feature_tags.join('・')}",
          labeled("勤務地", normalize_text(job["workArea"]).tr("/", "・")),
          labeled("雇用形態", job["employType"])
        ].compact.join(" / ")
      end

      def self.labeled(label, value)
        text = normalize_text(value)
        text.empty? ? nil : "#{label}: #{text}"
      end

      def self.text_or_default(value)
        text = normalize_text(value)
        text.empty? ? "要確認" : text
      end

      # "2026-10-08 12:15:53.000" → 日付部分。パース不能は nil。
      def self.parse_date(value)
        match = value.to_s.match(/(\d{4})-(\d{2})-(\d{2})/)
        match ? Date.new(*match.captures.map(&:to_i)) : nil
      rescue ArgumentError
        nil
      end

      def self.normalize_text(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      private_class_method :read_job, :build_posting, :original_site_name, :job_url, :build_description, :labeled,
                           :text_or_default, :parse_date, :normalize_text
    end
  end
end
