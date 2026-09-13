# frozen_string_literal: true

require "nokogiri"
require "date"

module FreelanceJobs
  module Sources
    # フリーランスHub: 複数エージェントの案件を集約するサイト。スキル別一覧
    # （/project/skill/<スキルID>/?order=created_at&page=<ページ>）をHTMLパースする。
    # Nuxt3 のSSR出力で、1ページ40件ぶんのカードマークアップ（PC版のみ）が生HTMLに含まれる。
    # 一覧のJSON-LDは FAQPage / BreadcrumbList だけで案件データは無く、window.__NUXT__ の
    # ペイロードは devalue 形式で壊れやすいため、DOMから項目を取り出す。
    # 一覧カードだけで案件名・単価・使用技術・作業内容・勤務地・提供元・掲載日が全て取れるため、
    # 詳細ページ（/project/detail/<ID>/）は取得しない。
    #
    # HTML構造の前提（壊れたらここを疑う。2026-09-13 実データ280件で確認）:
    #   - div.ProjectCard … カード1件。id="ProjectListPc_ProjectCard_<数値ID>"
    #   - カードに案件詳細への <a href> は無い。id の数値から /project/detail/<ID>/ を組み立てる
    #   - h3.ProjectCard_Title … 案件名
    #   - div.ProjectCard_SummaryItem--money p … 単価（"650,000円/月" / "800,000 〜 900,000円/月"、数値は<strong>）
    #   - div.ProjectCard_SummaryItem--skill span.TagLink … 使用技術
    #   - div.ProjectCard_SummaryItem--occupation span.TagLink … 募集職種（無いカードあり）
    #   - div.ProjectCard_SummaryItem--location span / --station span … 都道府県・最寄駅（無いカードあり）
    #   - div.ProjectCard_DetailText … 作業内容（全件 "作業内容 " で始まる）
    #   - div.ProjectCard_Tags a.ProjectCard_HotTags … "フルリモート" / "リモート可" / "オンライン商談OK"
    #   - ul.ProjectCard__Status__List li.ProjectCard__Status__Item … "NEW" / "注目"
    #   - p.ProjectCard__Footer__Info span … 1番目が掲載日（"5日前・" / "2ヶ月前・"）、2番目が "提供元: <ラベル>"
    #
    # 注意1: robots.txt が /project/search/ を禁止している。HotTags の <a href> はその配下
    #   （例 /project/search/?characteristic=20）を指すため、href は絶対に辿らず表示文言だけを使う。
    #   /project/skill/ と /project/detail/ は許可されている。
    # 注意2: 並び替えのクエリ名は `order`（`?sort=created_at` はSSRに無視されおすすめ順になる）。
    # 注意3: div.ProjectCard_SummaryItem--contract（"業務委託(フリーランス)"）はレバテッククリエイター
    #   提供のカードで要素ごと欠けるため、work_format は単価の単位で決める。
    # 注意4: 応募状況・応募締切はサイトに無いため application_status / deadline_text は "-"、
    #   deadline_on は nil で固定する。
    class FreelanceHub
      SITE_NAME = "フリーランスHub"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://freelance-hub.jp"

      # スキルIDはサイトの /project/skill/<ID>/ に実在するものだけを並べている
      # （Ruby=8 / TypeScript=409 / React=359）。Rails=80 は Ruby=8 と半数以上が重複するため既定に入れない。
      # 3一覧の間にも同じ案件が少数現れるが、URLキーの重複除去で吸収される。
      DEFAULT_SEARCH_TARGETS = [
        { skill_id: 8,   category_hint: "Ruby" },
        { skill_id: 409, category_hint: "TypeScript" },
        { skill_id: 359, category_hint: "React" }
      ].freeze

      # 1スキルあたりの取得ページ数。order=created_at で新着順に並ぶため、先頭数ページで
      # 新着案件を拾える（1頁40件。Ruby は3頁で約1ヶ月分、TypeScript/React は約1〜2週間分）。
      # 3スキル×3頁 = 9リクエスト／1バッチ。
      MAX_PAGES = 3

      # 集約サイトのため、既に個別の取得元として実装済みのエージェントが提供する案件は重複する。
      # サイト側の提供元ラベル（フッターの /agent/detail/ 一覧の正式表記）で完全一致させて捨てる。
      # ラベルの全角括弧・全角コロンはサイトの表記そのまま（例 "ココナラテック（旧：フリエン/furien）"）。
      # "クラウドワークス テック" / "ランサーズエージェント（Lancers Argent）" はエージェント事業で、
      # 既存の CrowdWorks / ランサーズ（公開案件）とは別物なので除外しない。
      DEFAULT_EXCLUDED_PROVIDERS = [
        "レバテックフリーランス",
        "レバテッククリエイター",
        "ココナラテック（旧：フリエン/furien）",
        "ビズリンク",
        "HiPro Tech（ハイプロテック）",
        "Findy Freelance",
        "フォスターフリーランス"
      ].freeze

      CARD_SELECTOR = "div.ProjectCard"
      TITLE_SELECTOR = "h3.ProjectCard_Title"
      MONEY_SELECTOR = "div.ProjectCard_SummaryItem--money p"
      SKILL_SELECTOR = "div.ProjectCard_SummaryItem--skill span.TagLink"
      OCCUPATION_SELECTOR = "div.ProjectCard_SummaryItem--occupation span.TagLink"
      LOCATION_SELECTOR = "div.ProjectCard_SummaryItem--location span"
      STATION_SELECTOR = "div.ProjectCard_SummaryItem--station span"
      DETAIL_TEXT_SELECTOR = "div.ProjectCard_DetailText"
      HOT_TAG_SELECTOR = "div.ProjectCard_Tags a.ProjectCard_HotTags"
      STATUS_TAG_SELECTOR = "ul.ProjectCard__Status__List li.ProjectCard__Status__Item"
      FOOTER_INFO_SELECTOR = "p.ProjectCard__Footer__Info span"

      # カードの id 属性から案件IDを取り出す。数値でないIDは案件として扱わない。
      CARD_ID_RE = /\AProjectListPc_ProjectCard_(\d+)\z/.freeze

      # 作業内容の先頭に付く見出し語。description では "作業内容: …" に組み立て直す。
      DETAIL_TEXT_HEADING = "作業内容"

      # フッター2番目の span に付く提供元の接頭辞（"提供元: Midworks"）。
      PROVIDER_PREFIX_RE = /\A提供元:\s*/.freeze

      # 掲載日は "N日前・" / "Nヶ月前・" の相対表記のみ（末尾の "・" は区切り文字）。
      # 想定外の表記なら posted_on は nil にする。
      DAYS_AGO_RE = /\A(\d+)日前/.freeze
      MONTHS_AGO_RE = /\A(\d+)ヶ月前/.freeze

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, max_pages: MAX_PAGES,
                     excluded_providers: DEFAULT_EXCLUDED_PROVIDERS)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @max_pages = max_pages
        @excluded_providers = excluded_providers
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
      # 提供元による除外はここでは行わない（fetch 側で行う）。ページ終端の判定を
      # 「除外前のカード数」で行いたいためで、除外対象ばかりのページでページ送りが止まるのを防ぐ。
      def self.parse(body, today:, category_hint: nil)
        document = Nokogiri::HTML(body)
        postings = {}

        document.css(CARD_SELECTOR).each do |card|
          posting = build_posting(card, today, category_hint)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      # 案件IDと案件名は表示・重複除去の両方に必須なので、欠けたカードは黙って捨てる
      # （セレクタが変わって全カードが欠損した場合は0件になり、呼び出し側のログで気付ける）。
      def self.build_posting(card, today, category_hint)
        project_id = project_id(card)
        return nil unless project_id

        title = squish(card.at_css(TITLE_SELECTOR)&.text)
        return nil if title.empty?

        build_posting_from_parts(card, today, category_hint, project_id, title)
      end

      def self.build_posting_from_parts(card, today, category_hint, project_id, title)
        skills = tag_texts(card, SKILL_SELECTOR)
        hot_tags = tag_texts(card, HOT_TAG_SELECTOR)
        posted_text, provider_text = footer_texts(card)
        reward_text = reward(squish(card.at_css(MONEY_SELECTOR)&.text))

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}/project/detail/#{project_id}/"),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(
            build_description(card, skills, hot_tags)
          ),
          category_hint: category_hint,
          reward: reward_text,
          work_format: work_format(reward_text),
          application_status: "-",
          deadline_text: "-",
          deadline_on: nil,
          skills: skills,
          client: provider_text.sub(PROVIDER_PREFIX_RE, ""),
          tags: tag_texts(card, STATUS_TAG_SELECTOR) + hot_tags,
          posted_on: posted_on(posted_text, today)
        )
      end

      def self.project_id(card)
        match = CARD_ID_RE.match(card["id"].to_s)
        match && match[1]
      end

      def self.tag_texts(card, selector)
        card.css(selector).map { |tag| squish(tag.text) }.reject(&:empty?)
      end

      # フッターの span は [掲載日, 提供元] の順で必ず2個ある。欠けても落とさず空文字で扱う。
      def self.footer_texts(card)
        texts = card.css(FOOTER_INFO_SELECTOR).map { |info| squish(info.text) }
        [texts[0].to_s, texts[1].to_s]
      end

      # 作業内容・募集職種・使用技術・勤務地・リモート等タグを description に入れる。
      # EngineerClassifierはtitle+description+skillsを連結したテキストで判定するため、
      # 技術名とリモート可否が本文に出ていると分類精度が上がる。
      def self.build_description(card, skills, hot_tags)
        detail_text = squish(card.at_css(DETAIL_TEXT_SELECTOR)&.text).sub(/\A#{DETAIL_TEXT_HEADING}\s*/, "")
        occupations = tag_texts(card, OCCUPATION_SELECTOR)
        work_location = [
          squish(card.at_css(LOCATION_SELECTOR)&.text),
          squish(card.at_css(STATION_SELECTOR)&.text)
        ].reject(&:empty?).join(" ")

        parts = []
        parts << "#{DETAIL_TEXT_HEADING}: #{detail_text}" unless detail_text.empty?
        parts << "募集職種: #{occupations.join(" / ")}" unless occupations.empty?
        parts << "使用技術: #{skills.join(" / ")}" unless skills.empty?
        parts << "勤務地: #{work_location}" unless work_location.empty?
        parts << hot_tags.join("・") unless hot_tags.empty?
        parts.join(" / ")
      end

      # 単価は "650,000円/月" のように半角スラッシュで単位が付く。他の取得元（レバテック・ビズリンク）と
      # 表記を揃えるため全角の "／"（U+FF0F）に置き換えて "650,000円／月" にする。
      # EngineerClassifier.high_reward? がカンマを除いた先頭の数値を読むため、桁区切りはそのまま残す。
      def self.reward(money_text)
        money_text.empty? ? "要確認" : money_text.tr("/", "／")
      end

      # 実データは全件「円／月」だが、将来の時間単価表記に備えて単位で分岐させる。
      def self.work_format(reward_text)
        if reward_text.include?("／時")
          "時間単価制"
        elsif reward_text.include?("／月")
          "月額制（業務委託）"
        else
          "業務委託（フリーランス）"
        end
      end

      # "N日前" は today から N 日引く。"Nヶ月前" は Date#<< で N ヶ月引く
      # （一覧の「5日前」が詳細ページの最終更新日と一致することを実データで確認済み）。
      def self.posted_on(posted_text, today)
        if (match = DAYS_AGO_RE.match(posted_text))
          today - match[1].to_i
        elsif (match = MONTHS_AGO_RE.match(posted_text))
          today << match[1].to_i
        end
      end

      def self.squish(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      private

      # 1スキルぶんのページ送り。カードが0件のページはページ終端（またはセレクタ崩れ）なので、
      # それ以上リクエストしても無駄になるため打ち切る。提供元による除外はページ終端の判定後に行う。
      def fetch_target(target, fetch_failures)
        postings = []

        (1..@max_pages).each do |page_number|
          body = fetch_page_body(target[:skill_id], page_number, fetch_failures)
          break if body.nil?

          page_postings = self.class.parse(body, today: @today, category_hint: target[:category_hint])
          break if page_postings.empty?

          postings.concat(page_postings.reject { |posting| excluded_provider?(posting.client) })
        end

        postings
      end

      # 提供元ラベルが除外リストに完全一致する案件は、既存の取得元と重複するため捨てる。
      def excluded_provider?(provider_label)
        @excluded_providers.include?(provider_label)
      end

      # 散発的なHTTPエラーや通信層のタイムアウト・切断で1ページが取れなくても、スキル全体・
      # 取得元全体を落とさず、そのスキルのページ送りだけを打ち切って次のスキルへ進む。
      # rescueの範囲が@fetcher.getの1回だけなので、StandardErrorで受けてもパース側のバグは覆い隠さない。
      def fetch_page_body(skill_id, page_number, fetch_failures)
        get_with_single_retry(list_url(skill_id, page_number))
      rescue FreelanceJobs::AccessBlockedError
        raise
      rescue StandardError => error
        fetch_failures << error
        FreelanceJobs.logger.warn(
          "[FreelanceJobs::Sources::FreelanceHub] #{error.message} このスキルのページ送りを打ち切ります"
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
        FreelanceJobs.logger.warn("[FreelanceJobs::Sources::FreelanceHub] #{error.message} 1回だけ再取得します")
        @fetcher.get(url)
      end

      # 並び替えのクエリ名は `order`。`sort` はSSRに無視されるので使わない（注意2参照）。
      def list_url(skill_id, page_number)
        "#{BASE_URL}/project/skill/#{skill_id}/?order=created_at&page=#{page_number}"
      end
    end
  end
end
