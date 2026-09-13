# frozen_string_literal: true

require "nokogiri"
require "date"
require "uri"

module FreelanceJobs
  module Sources
    # シューマツワーカー（副業特化のマッチングサイト）: スキル別一覧（/projects?skills=<slug>）で
    # 募集中カードの案件パスだけを集め、案件ごとに詳細ページ（/projects/<id>）を取得してHTMLパースする。
    # Rails の完全SSR出力で、WAF・CAPTCHA・リダイレクトは無い（2026-09-13計測）。
    #
    # ★2段階取得にしている理由: 一覧カードの値は報酬「-420,000円/月 (...」・スキル「Next.js/Githu...」の
    #   ように**全項目が「…」で省略されている**うえ、掲載日・発注企業名・案件本文が一覧に無い。
    #   一覧だけで JobPosting を作ると reward/skills が壊れるため、必ず詳細を取る。
    # ★トップページの ?q= は検索ではなくLP（?q=Ruby と ?q=TypeScript で HTML 差分ゼロ）なので使わない。
    #
    # 注意1: 一覧の並びは「募集中（ID降順）→ 募集終了（ID降順）」。募集終了カードが1枚でも
    #   出たページより後に募集中は無いので、そこでページ送りを打ち切る。
    # 注意2: 範囲外のページ番号は HTTP 200 でカード0件（本文「該当する案件の募集はございません」）。
    # 注意3: 詳細ページ下部に関連案件カード（div.project-section-related-project 配下の
    #   div.project-list-item）があるため、一覧パースは .projects-main-body 配下に限定する。
    # 注意4: 詳細の JSON-LD（@type JobPosting）は description に生の改行が入っており JSON.parse で
    #   落ちる（Ruby 3.2.3 + json 2.13.2 で実測）。発注企業名だけ正規表現で拾い、他は DOM から取る。
    class ShuuumatuWorker
      SITE_NAME = "シューマツワーカー"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://shuuumatu-worker.jp"
      LIST_PATH = "/projects"

      # スキルslugはトップページのナビに列挙されているもの。slug ごとに結果集合は別物
      # （実測: ruby 8件 / rubyonrails 10件 / typescript・react・nextjs 各12件超）。
      # 同じ案件が複数slugに出るためURLキーの重複除去は必須。
      # reactnative は未確認のため入れていない（入れる場合の hint は "React"）。
      DEFAULT_SEARCH_TARGETS = [
        { skill_slug: "ruby",        category_hint: "Ruby" },
        { skill_slug: "rubyonrails", category_hint: "Ruby" },
        { skill_slug: "typescript",  category_hint: "TypeScript" },
        { skill_slug: "react",       category_hint: "React" },
        { skill_slug: "nextjs",      category_hint: "React" }
      ].freeze

      # 1スキルあたりの最大ページ数。12件/頁で募集中が先頭に並ぶため実質1ページで足りるが、
      # 募集中が12件を超えたときの取りこぼし防止として2ページ目まで許す（安全側の上限）。
      MAX_PAGES = 2
      # 詳細ページの最大取得件数。一覧 ≤ 5スキル×2頁 = 10 と合わせて 40 に収める。
      MAX_DETAIL_REQUESTS = 30
      # 1回の fetch でこの取得元が発行するHTTPリクエストの上限（再取得ぶんも数える）。
      # 一覧・詳細の上限とは別に、ここで物理的に打ち止めにしてサイトへの負荷を保証する。
      REQUEST_BUDGET = 40

      # --- 一覧ページ ---
      LIST_CARD_SELECTOR = ".projects-main-body div.project-list-item"
      LIST_CARD_LINK_SELECTOR = "a.project-list-item-link[href]"
      # 募集終了カードはカード自身の class に付く（併せて子要素 div.project-list-item-head__closed
      # に "募集終了" のテキストがある）。募集中カードにはどちらも無い。
      CLOSED_CARD_CLASS = "project-list-item--closed"
      # 次ページリンク。1ページしか無いときは nav.pagination 自体が無い。
      NEXT_PAGE_SELECTOR = "nav.pagination li.next a[href]"
      # 案件詳細への相対パス（例 "/projects/18881"）。この形だけを案件として扱う。
      JOB_PATH_RE = %r{\A/projects/\d+\z}.freeze

      # --- 詳細ページ ---
      DETAIL_ROOT_SELECTOR = "div.project-main"
      OG_URL_SELECTOR = 'meta[property="og:url"][content]'
      TITLE_SELECTOR = "h1.project-main-head__title"
      # 職種は pc 版がフル文字列、sp 版は「…」で省略されるため必ず pc 版を読む。
      JOB_TYPE_SELECTOR = "div.project-main-head__jobtype.pc"
      PUBLISHED_SELECTOR = "p.project-main-head-published"
      # 募集状態は CTA のテキストで判定する（詳細には closed 系の class が無い）。
      # 募集中: <a class="entry-btn">エントリーしてみる</a> / 募集終了: <button disabled>公開終了</button>
      CTA_SELECTOR = "div.project-main-body-cta-top"
      OPEN_STATUS = "募集中"
      EXPIRED_STATUS = "公開終了"
      # 条件ブロック（報酬 / 稼働時間 / 働き方 / 関連スキル）。ラベルは <p>、値は <div>。
      CONDITION_ITEM_SELECTOR = "div.project-main-body-condition-item"
      CONDITION_LABEL_SELECTOR = ".project-main-body-condition-item__label"
      CONDITION_TEXT_SELECTOR = ".project-main-body-condition-item__text"
      # 関連スキルは <a class="...skill-list__item">Next.js</a> の連なり。__text をそのまま
      # .text すると "Next.jsGithubBigQuery…" と連結されるので必ず個々の要素を読む。
      SKILL_ITEM_SELECTOR = ".project-main-body-condition-item-skill-list__item"
      REWARD_LABEL = "報酬"
      WORKING_HOURS_LABEL = "稼働時間"
      WORK_STYLE_LABEL = "働き方"
      # 本文セクション（案件内容 / 必要条件 / 歓迎条件（無い案件あり） / 求める人物像 / 契約形態）。
      DETAIL_SECTION_SELECTOR = "div.project-main-body-detail"
      DETAIL_LABEL_SELECTOR = ".project-main-body-detail__label"
      DETAIL_TEXT_SELECTOR = ".project-main-body-detail__text"
      DESCRIPTION_SECTION_LABELS = %w[案件内容 必要条件 歓迎条件].freeze
      JSON_LD_SELECTOR = 'script[type="application/ld+json"]'

      # 掲載日は "公開日: 2026年09月01日" 表記。
      POSTED_ON_RE = /(\d{4})年(\d{1,2})月(\d{1,2})日/.freeze
      # JSON-LD の hiringOrganization.name だけを文字列として抜く（注意4のとおり JSON.parse は使えない）。
      HIRING_ORGANIZATION_NAME_RE = /"hiringOrganization"\s*:\s*\{[^}]*?"name"\s*:\s*"([^"]*)"/m.freeze

      # 報酬は自由記述（例 "-420,000円/月 (ご経験やスキルにより要相談) ※時間単価：4,500円/時程度"、
      # "112,500〜150,000円/月 ※目安…"、"-月額単価320,000〜400,000円 (…) -時間単価4,000〜5,000円/時程度"）。
      # 先に範囲表記を探し、無ければ最初の金額を取る。先頭の "-" は箇条書き記号なので
      # 範囲の区切りにハイフンは含めない。
      AMOUNT_PATTERN = /\d{1,3}(?:,\d{3})+|\d+/.source
      REWARD_RANGE_RE = /(#{AMOUNT_PATTERN})(?:円)?\s*[〜~～]\s*(#{AMOUNT_PATTERN})\s*円/.freeze
      REWARD_SINGLE_RE = /(#{AMOUNT_PATTERN})\s*円/.freeze
      # 金額の直後が "/時" なら時間単価。サイトは月額が基本で、時給は「※時間単価：…」の補足として
      # 後ろに出るため、最初の金額の単位だけを見ればよい。
      HOURLY_UNIT_RE = %r{\A\s*[/／]\s*時}.freeze
      # 「働き方」「稼働時間」の先頭に付く箇条書きハイフン（"-フルリモート"）。
      LEADING_BULLET_RE = /\A[-−]\s*/.freeze

      # 一覧カード1枚ぶんの情報（詳細パスと募集終了フラグだけ持つ）。
      ListCard = Struct.new(:path, :closed, keyword_init: true)

      # 詳細ページから取り出した条件・本文の入れ物。description と tags が同じ値を使うため
      # DOM からの抽出を1か所に集約して持ち回る。
      DetailFields = Struct.new(:job_type, :sections, :conditions, :skills, keyword_init: true)

      # include_expired: 募集終了（公開終了）の案件をシートに載せるか。既定は false
      # （応募できない案件でシートが埋まるのを避ける）。
      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS, max_pages: MAX_PAGES,
                     max_detail_requests: MAX_DETAIL_REQUESTS, request_budget: REQUEST_BUDGET,
                     include_expired: false)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @max_pages = max_pages
        @max_detail_requests = max_detail_requests
        @request_budget = request_budget
        @include_expired = include_expired
        @request_count = 0
      end

      # 通信あり。スキルごとに一覧を読んで募集中カードの案件パスを集め（スキル横断でユニーク化）、
      # 先頭から max_detail_requests 件まで詳細ページを取得して JobPosting にする。
      # 1ページ・1案件の取得失敗は握りつぶして先へ進むが、1件も取れずに失敗だけが残った場合は
      # 最初の失敗を送出し、ResearchServiceに「取得失敗」として記録させる
      # （黙って0件を返すと、サイト構造の崩れや全面的な障害に気付けなくなるため）。
      def fetch
        fetch_failures = []
        open_paths_with_hints = collect_open_detail_paths(fetch_failures)
        postings = fetch_detail_postings(open_paths_with_hints, fetch_failures)

        raise fetch_failures.first if postings.empty? && !fetch_failures.empty?

        postings.values
      end

      # 通信なし（テスト用）。**詳細1ページ分**のHTML本文から案件を作る（0件または1件）。
      # 募集終了でも application_status: "公開終了" として**そのまま返す**（ページの写しに徹する）。
      # 採用するかどうかは fetch の include_expired 側で判断する。
      # detail_url: og:url が無いページ向けの予備（fetch が取得URLを渡す）。
      def self.parse(body, today:, category_hint: nil, detail_url: nil)
        document = Nokogiri::HTML(body)
        detail_root = document.at_css(DETAIL_ROOT_SELECTOR)
        return [] unless detail_root

        posting = build_posting(document, detail_root, category_hint, detail_url)
        posting ? [posting] : []
      end

      # 通信なし。一覧1ページ分のHTML本文から、カードの並び順どおりに ListCard を作る。
      # 案件パスの形でないカード（セレクタ崩れ・別種リンク）は黙って捨てる。
      def self.parse_list(body)
        document = Nokogiri::HTML(body)

        document.css(LIST_CARD_SELECTOR).filter_map do |card|
          path = card.at_css(LIST_CARD_LINK_SELECTOR)&.[]("href").to_s.strip
          next nil unless path.match?(JOB_PATH_RE)

          ListCard.new(path: path, closed: card["class"].to_s.split.include?(CLOSED_CARD_CLASS))
        end
      end

      # 通信なし。一覧ページの次ページリンク（相対パス）。無ければ nil。
      def self.next_page_path(body)
        href = Nokogiri::HTML(body).at_css(NEXT_PAGE_SELECTOR)&.[]("href").to_s.strip
        href.empty? ? nil : href
      end

      # 詳細1ページを JobPosting に組み立てる。案件URLと案件名のどちらかが取れないページは
      # 詳細ページではない（または構造が変わった）と判断して nil を返す。
      def self.build_posting(document, detail_root, category_hint, detail_url)
        url = detail_page_url(document, detail_url)
        return nil if url.empty?

        title = squish_text(detail_root.at_css(TITLE_SELECTOR)&.text)
        # タイトルが空のまま行を作るとシート上で案件を識別できなくなるため、ここで落とす。
        return nil if title.empty?

        detail_fields = extract_detail_fields(detail_root)
        reward = build_reward(detail_fields.conditions[REWARD_LABEL])
        status = application_status(detail_root)

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: url,
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(build_description(detail_fields)),
          category_hint: category_hint,
          reward: reward,
          work_format: work_format(reward),
          application_status: status,
          # 応募締切の概念が無いサイトなので固定値にする。
          deadline_text: "-",
          deadline_on: nil,
          skills: detail_fields.skills,
          client: hiring_organization_name(document),
          tags: build_tags(status, detail_fields),
          posted_on: parse_posted_on(detail_root)
        )
      end

      # og:url（"https://shuuumatu-worker.jp/projects/18881"）を優先し、無ければ取得URLを使う。
      def self.detail_page_url(document, detail_url)
        og_url = document.at_css(OG_URL_SELECTOR)&.[]("content").to_s.strip
        FreelanceJobs::JobPosting.normalize_url(og_url.empty? ? detail_url : og_url)
      end

      def self.extract_detail_fields(detail_root)
        DetailFields.new(
          job_type: squish_text(detail_root.at_css(JOB_TYPE_SELECTOR)&.text),
          sections: detail_sections(detail_root),
          conditions: condition_items(detail_root),
          skills: detail_root.css(SKILL_ITEM_SELECTOR).map { |item| squish_text(item.text) }.reject(&:empty?)
        )
      end

      # 条件ブロックを「ラベル => 値」の Hash にする（"報酬" / "稼働時間" / "働き方" / "関連スキル"）。
      # 報酬の値には <br> と注記の <div> が入れ子になっているので、改行を空白に置き換えて1行にする。
      def self.condition_items(detail_root)
        detail_root.css(CONDITION_ITEM_SELECTOR).each_with_object({}) do |item, conditions|
          label = squish_text(item.at_css(CONDITION_LABEL_SELECTOR)&.text)
          value = text_with_line_breaks(item.at_css(CONDITION_TEXT_SELECTOR))
          conditions[label] = value unless label.empty? || value.empty?
        end
      end

      # 本文セクションを「見出し => 本文」の Hash にする。
      def self.detail_sections(detail_root)
        detail_root.css(DETAIL_SECTION_SELECTOR).each_with_object({}) do |section, sections|
          label = squish_text(section.at_css(DETAIL_LABEL_SELECTOR)&.text)
          content = text_with_line_breaks(section.at_css(DETAIL_TEXT_SELECTOR))
          sections[label] = content unless label.empty? || content.empty?
        end
      end

      # EngineerClassifier は title + description + skills を連結したテキストで判定するため、
      # 技術名が多数出る本文（案件内容・必要条件・歓迎条件）に加え、職種・使用技術・稼働時間・
      # 働き方（"フルリモート" → リモート可）も description に入れる。tags は分類器に渡らない。
      def self.build_description(detail_fields)
        parts = DESCRIPTION_SECTION_LABELS.filter_map do |label|
          detail_fields.sections[label] && "#{label}: #{detail_fields.sections[label]}"
        end
        parts << "募集職種: #{detail_fields.job_type}" unless detail_fields.job_type.empty?
        parts << "使用技術: #{detail_fields.skills.join(" / ")}" unless detail_fields.skills.empty?
        working_hours = strip_leading_bullet(detail_fields.conditions[WORKING_HOURS_LABEL])
        parts << "#{WORKING_HOURS_LABEL}: #{working_hours}" unless working_hours.empty?
        work_style = strip_leading_bullet(detail_fields.conditions[WORK_STYLE_LABEL])
        parts << "#{WORK_STYLE_LABEL}: #{work_style}" unless work_style.empty?
        parts.join(" / ")
      end

      def self.build_tags(status, detail_fields)
        [status, strip_leading_bullet(detail_fields.conditions[WORK_STYLE_LABEL]), detail_fields.job_type]
          .reject { |tag| tag.empty? || tag == "-" }
      end

      # 報酬の自由記述から "420,000円／月" / "112,500〜150,000円／月" / "4,500円／時" の形を作る。
      # 金額が読めなければ "要確認"。桁区切りのカンマは残す（Classifier.first_reward_amount が delete(",") する）。
      def self.build_reward(reward_text)
        text = reward_text.to_s
        matched = REWARD_RANGE_RE.match(text) || REWARD_SINGLE_RE.match(text)
        return "要確認" unless matched

        amount = matched[2] ? "#{matched[1]}〜#{matched[2]}" : matched[1]
        unit = matched.post_match.match?(HOURLY_UNIT_RE) ? "／時" : "／月"
        "#{amount}円#{unit}"
      end

      def self.work_format(reward)
        if reward.include?("／時")
          "時間単価制"
        elsif reward.include?("／月")
          "月額制（業務委託）"
        else
          "業務委託（フリーランス）"
        end
      end

      # CTA テキストが "公開終了" なら募集終了、"エントリー…" なら募集中。
      # CTA が無いページは判定材料が無いものとして "-" にする（include_expired の除外対象にはしない）。
      def self.application_status(detail_root)
        cta_text = squish_text(detail_root.at_css(CTA_SELECTOR)&.text)
        return EXPIRED_STATUS if cta_text.include?(EXPIRED_STATUS)
        return OPEN_STATUS if cta_text.include?("エントリー")

        "-"
      end

      def self.recruitment_expired?(posting)
        posting.application_status == EXPIRED_STATUS
      end

      # "公開日: 2026年09月01日" → Date。日付が読めないページは掲載日なし(nil)として扱う。
      def self.parse_posted_on(detail_root)
        matched = POSTED_ON_RE.match(squish_text(detail_root.at_css(PUBLISHED_SELECTOR)&.text))
        return nil unless matched

        Date.new(matched[1].to_i, matched[2].to_i, matched[3].to_i)
      rescue ArgumentError
        # "2026年02月31日" のような存在しない日付が入っていた場合の保険。
        nil
      end

      # 発注企業名は DOM に出ないため JSON-LD の hiringOrganization.name を正規表現で拾う
      # （"Wello株式会社" / "株式会社 pluszero " / "個人事業主"）。取れなければ ""。
      def self.hiring_organization_name(document)
        document.css(JSON_LD_SELECTOR).each do |script|
          matched = HIRING_ORGANIZATION_NAME_RE.match(script.text)
          return squish_text(matched[1]) if matched
        end
        ""
      end

      def self.strip_leading_bullet(text)
        squish_text(text).sub(LEADING_BULLET_RE, "")
      end

      # 本文は "\r<br>" 区切りで、そのまま .text すると行が連結して読めなくなる。
      # テキスト化の前に <br> を半角スペースのテキストノードへ置き換えて区切りを残す。
      def self.text_with_line_breaks(node)
        return "" unless node

        copied_node = node.dup
        copied_node.css("br").each do |line_break|
          line_break.replace(Nokogiri::XML::Text.new(" ", copied_node.document))
        end
        squish_text(copied_node.text)
      end

      # Nokogiriのtextは前後に改行・インデントが入るため、表示用の値は必ず空白を畳んでから使う。
      def self.squish_text(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      private

      # 全スキルの一覧を読み、募集中カードの案件パスを出現順にユニーク化する。
      # 値は最初に見つけたスキルの category_hint（同じ案件が複数スキルに出た場合は先勝ち）。
      def collect_open_detail_paths(fetch_failures)
        @search_targets.each_with_object({}) do |target, paths_with_hints|
          collect_open_paths_for_target(target, fetch_failures).each do |path|
            paths_with_hints[path] ||= target[:category_hint]
          end
        end
      end

      # 1スキルぶんのページ送り。次のいずれかで打ち切る:
      #   カード0件（末尾超え・セレクタ崩れ）/ 募集終了カードあり（以降に募集中は無い）/
      #   次ページリンク無し / max_pages 到達 / 取得失敗 / リクエスト予算切れ
      def collect_open_paths_for_target(target, fetch_failures)
        open_paths = []
        page_url = list_url(target[:skill_slug])

        @max_pages.times do
          body = fetch_body(page_url, fetch_failures, "このスキルのページ送り")
          break if body.nil?

          cards = self.class.parse_list(body)
          break if cards.empty?

          open_paths.concat(cards.reject(&:closed).map(&:path))
          break if cards.any?(&:closed)

          next_path = self.class.next_page_path(body)
          break if next_path.nil?

          page_url = absolute_url(next_path)
        end

        open_paths
      end

      # 募集中の案件パスを先頭から max_detail_requests 件まで詳細取得して JobPosting にする。
      def fetch_detail_postings(open_paths_with_hints, fetch_failures)
        postings = {}

        open_paths_with_hints.first(@max_detail_requests).each do |path, category_hint|
          detail_url = absolute_url(path)
          body = fetch_body(detail_url, fetch_failures, "この案件の詳細")
          next if body.nil?

          self.class.parse(body, today: @today, category_hint: category_hint, detail_url: detail_url).each do |posting|
            next if !@include_expired && self.class.recruitment_expired?(posting)

            postings[posting.url] ||= posting
          end
        end

        postings
      end

      # 1URLの取得。散発的なHTTPエラーは1回だけ取り直し、それでも失敗したら fetch_failures に積んで
      # nil を返す（呼び出し側はその単位＝スキルのページ送り／その案件の詳細だけを打ち切る）。
      # rescueの範囲が@fetcher.getの1回だけなので、StandardErrorで受けてもパース側のバグは覆い隠さない
      # （HTTPエラーのFetchErrorだけでなく、通信層のタイムアウト・切断も同じ扱いにしたいため広く受ける）。
      # WAFのアクセス制限は取り直しても解消しないので、AccessBlockedErrorはそのまま送出する。
      def fetch_body(url, fetch_failures, unit_description)
        return nil unless request_budget_remaining?(url)

        get_with_single_retry(url)
      rescue FreelanceJobs::AccessBlockedError
        raise
      rescue StandardError => error
        fetch_failures << error
        FreelanceJobs.logger.warn(
          "[FreelanceJobs::Sources::ShuuumatuWorker] #{error.message} #{unit_description}を打ち切ります"
        )
        nil
      end

      # HttpFetcherが2回目以降のリクエスト前に REQUEST_INTERVAL 秒 sleep するため、ここで追加のsleepは要らない。
      # 予算が尽きていれば取り直さずにそのまま失敗させる。
      def get_with_single_retry(url)
        counted_get(url)
      rescue FreelanceJobs::AccessBlockedError
        raise
      rescue StandardError => error
        raise unless request_budget_remaining?(url)

        FreelanceJobs.logger.warn("[FreelanceJobs::Sources::ShuuumatuWorker] #{error.message} 1回だけ再取得します")
        counted_get(url)
      end

      def counted_get(url)
        @request_count += 1
        @fetcher.get(url)
      end

      # 予算切れは障害ではなく設計上の打ち止めなので fetch_failures には積まず、警告ログだけ残す。
      def request_budget_remaining?(url)
        return true if @request_count < @request_budget

        FreelanceJobs.logger.warn(
          "[FreelanceJobs::Sources::ShuuumatuWorker] リクエスト上限 #{@request_budget} 回に達したため取得を打ち切ります #{url}"
        )
        false
      end

      # "https://shuuumatu-worker.jp/projects?skills=typescript"。2ページ目以降のURLはサイトが
      # 吐く次ページリンク（"/projects?page=2&skills=typescript"）をそのまま辿る。
      def list_url(skill_slug)
        "#{BASE_URL}#{LIST_PATH}?skills=#{skill_slug}"
      end

      def absolute_url(path)
        URI.join(BASE_URL, path).to_s
      end
    end
  end
end
