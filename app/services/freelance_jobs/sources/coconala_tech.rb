# frozen_string_literal: true

require "nokogiri"
require "date"

module FreelanceJobs
  module Sources
    # ココナラテック（tech.coconala.com）: スキルIDで絞り込んだ求人・案件一覧をHTMLパースする。
    # Next.js App Router のSSRで、カードのDOM（バッジ / h2 / ul>li×3 / 使用技術 / 募集本文）が
    # サーバー側で完全に出力されているため、一覧ページだけで必要なフィールドが全て埋まる。
    #
    # 同じ「ココナラ」でも FreelanceJobs::Sources::Coconala（coconala.com/requests の
    # クラウドソーシング公開依頼）とは別サービス・別ドメインのため、SITE_NAMEも別にしてある。
    #
    # 詳細ページ(/job-postings/{UUID})にはJSON-LD(JobPosting)があり実務経験年数まで取れるが、
    # 全件叩くと 30件×3スキル＝90リクエストになる割に一覧へ足せる情報がほぼ無いため取得しない。
    # その結果 EngineerClassifier の難易度判定は「中級（実務経験あり）」に寄るが、これは許容する。
    class CoconalaTech
      SITE_NAME = "ココナラテック"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://tech.coconala.com"

      # skillIds は実HTTPで特定した値。<title>（「Rubyの求人・案件一覧 |…」）と結果集合が
      # スキルごとに変わることを確認済み。実測の該当件数は Ruby 267件 / TypeScript 590件 /
      # React 687件（うち大半が募集終了。募集中はそれぞれ 5 / 22 / 23件程度）。
      DEFAULT_SEARCH_TARGETS = [
        { skill_id: 4,  hint: "Ruby" },
        { skill_id: 31, hint: "TypeScript" },
        { skill_id: 64, hint: "React" }
      ].freeze

      # 1ページ30件。このサイトは「募集中が先頭・募集終了が後ろ」に並ぶため、通常は
      # 1スキル1ページで募集中案件を取り切れる（下の fetch の打ち切り条件を参照）。
      # max_pages は募集中案件が30件を超えて増えた場合の保険＝安全上限としてのみ効く。
      DEFAULT_MAX_PAGES = 2

      # 一覧カードのアンカー。1案件につきPC用(tw-flex)とSP用(tablet:tw-hidden)の2本が出力され、
      # 中身は完全に同一なので href をキーに重複排除する。
      CARD_ANCHOR_SELECTOR = 'a[href^="/job-postings/"]'
      # href は "/job-postings/{UUID}"（数値IDではなくUUIDv7）。パンくず等の他のアンカーを
      # 拾わないよう、UUIDの形をしたパスだけを案件として扱う。
      JOB_PATH_RE = %r{\A/job-postings/([0-9A-Za-z-]+)\z}
      # バッジ（"NEW" / "リモート可" / "募集終了"）。アンカー内にちょうど1個。
      BADGE_CONTAINER_SELECTOR = "div.tw-mb-2"

      # サイト側のバッジ文言（CLOSED_BADGE）と、シートに出す応募状況（CLOSED_STATUS / OPEN_STATUS）は
      # たまたま同じ文字列だが由来が違うので定数を分けてある（バッジ文言が変わっても表示側は動く）。
      CLOSED_BADGE = "募集終了"
      CLOSED_STATUS = FreelanceJobs::JobPosting::CLOSED_STATUS
      OPEN_STATUS = "募集中"

      # ul>li の必要項目数。0:単価 / 1:勤務地 / 2:職種+契約形態 の順で固定されている。
      # li のclassは3つとも同一(tw-flex tw-items-start tw-gap-2)でアイコンSVGしか違わないため、
      # class では区別できず**出現順でしか判定できない**。実測180カード全てが
      # 「ちょうど3項目・同じ順序」だったことがこの対応付けの根拠。
      REQUIRED_LIST_ITEM_COUNT = 3
      REWARD_ITEM_INDEX = 0
      WORK_LOCATION_ITEM_INDEX = 1
      OCCUPATION_ITEM_INDEX = 2

      # 使用技術のspanは `<span>Ruby<!-- -->・</span>` の形で、最後の要素だけ区切りが付かない。
      SKILL_DELIMITER_SUFFIX_RE = /[・、]\z/

      # UUIDv7から掲載日を導出するときの異常値ガード（サイト開設前・未来日を弾く）。
      EARLIEST_POSTED_ON = Date.new(2015, 1, 1).freeze

      def initialize(fetcher:, today:, search_targets: DEFAULT_SEARCH_TARGETS,
                     max_pages: DEFAULT_MAX_PAGES, include_closed: false)
        @fetcher = fetcher
        @today = today
        @search_targets = search_targets
        @max_pages = max_pages
        @include_closed = include_closed
      end

      # 通信あり。スキルごとに一覧ページを取得し、URLキーで重複排除する。
      # TypeScriptとReactの検索結果には同じ案件が現れる（実測で確認）ため重複排除は必須。
      def fetch
        postings = {}

        @search_targets.each do |target|
          collect_skill_postings(target).each { |posting| postings[posting.url] ||= posting }
        end

        postings.values
      end

      # 通信なし（テスト用）。一覧1ページ分のHTML本文から案件一覧を作る。
      # 募集終了の案件も application_status: "募集終了" として**そのまま返す**（ページの写しに徹する）。
      # 募集終了を採用するかどうかの方針は fetch の include_closed 側で判断する。
      def self.parse(body, today:, category_hint: nil)
        document = Nokogiri::HTML(body)
        postings = {}

        document.css(CARD_ANCHOR_SELECTOR).each do |anchor|
          posting = build_posting(anchor, today, category_hint)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      # アンカー1件をJobPostingに組み立てる。案件カードに必要な要素
      # （UUIDのhref / h2 / li×3 のul）が揃わないものは一覧カードではない
      # （またはDOM構造が変わった）と判断してnilを返し、呼び出し側で除外する。
      # 黙って値を欠いたJobPostingを作るより、対応付けが成り立たないカードは落とす方が安全。
      def self.build_posting(anchor, today, category_hint)
        job_id = job_id_from(anchor["href"])
        return nil unless job_id

        title_element = anchor.at_css("h2")
        return nil unless title_element

        list_items = summary_list_items(anchor)
        return nil if list_items.size < REQUIRED_LIST_ITEM_COUNT

        reward_text = list_items[REWARD_ITEM_INDEX]
        skills = skills_from(anchor)
        tags = badge_labels(anchor)

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url("#{BASE_URL}/job-postings/#{job_id}"),
          title: title_element.text.gsub(/\s+/, " ").strip,
          description: FreelanceJobs::JobPosting.normalize_description(build_description(anchor, list_items, skills)),
          category_hint: category_hint,
          reward: reward_text.empty? ? "要確認" : reward_text,
          work_format: work_format(reward_text),
          application_status: tags.include?(CLOSED_BADGE) ? CLOSED_STATUS : OPEN_STATUS,
          # 応募締切はサイト全体に存在しない（詳細ページにも無い）。
          deadline_text: "-",
          deadline_on: nil,
          skills: skills,
          # 一覧にも詳細ページのJSON-LD(hiringOrganization.name)にも発注者名は無く「社名非公開」固定。
          client: "",
          tags: tags,
          posted_on: parse_posted_on(job_id, today: today)
        )
      end

      def self.recruitment_closed?(posting)
        posting.application_status == CLOSED_STATUS
      end

      # href から案件ID（UUID）を取り出す。UUIDの形をしていないパスはnilを返す。
      def self.job_id_from(href)
        match = JOB_PATH_RE.match(href.to_s.strip)
        match && match[1]
      end

      # ul（アンカー内にちょうど1個）直下のliのテキスト。入れ子のul対策で「>」で直下に限定する。
      def self.summary_list_items(anchor)
        list = anchor.at_css("ul")
        return [] unless list

        list.css("> li").map { |list_item| list_item.text.gsub(/\s+/, " ").strip }
      end

      # 使用技術。ulの次の兄弟divに入っている（実測180カード全てで存在）。
      # 先頭にアイコン用のspan（中身がSVG）が混ざるので除外し、区切りの「・」を落とす。
      def self.skills_from(anchor)
        skills_block = anchor.at_css("ul")&.next_element
        return [] unless skills_block

        skills_block.css("span")
                    .reject { |span_node| span_node.at_css("svg") }
                    .map { |span_node| span_node.text.gsub(/\s+/, " ").strip.sub(SKILL_DELIMITER_SUFFIX_RE, "").strip }
                    .reject(&:empty?)
      end

      # バッジのラベル一覧。例: ["リモート可"] / ["募集終了"] / ["NEW", "リモート可"] / []（バッジ無し）。
      def self.badge_labels(anchor)
        anchor.css("#{BADGE_CONTAINER_SELECTOR} span").map { |span_node| span_node.text.strip }.reject(&:empty?)
      end

      # 「職種・契約形態 / 勤務地 / 使用技術 / 募集本文」を連結する。
      # 募集本文のpはCSS(tw-line-clamp-2)で見た目が省略されているだけで、DOMには
      # 【作業内容】【開発環境】まで全文が入っている。技術名がここに濃く出るため、
      # EngineerClassifier の判定精度はこのpを含めるかどうかで決まる。
      def self.build_description(anchor, list_items, skills)
        parts = [list_items[OCCUPATION_ITEM_INDEX], list_items[WORK_LOCATION_ITEM_INDEX]]
        parts << "使用技術: #{skills.join(" / ")}" unless skills.empty?
        parts << anchor.at_css("p")&.text.to_s
        parts.reject { |part| part.nil? || part.strip.empty? }.join(" / ")
      end

      # 契約形態は単価の単位で判定する。このサイトの区切りは**半角スラッシュ**（"円/月"）で、
      # レバテックの全角（"円／月"）とは異なる。将来どちらで出力されてもよいように全角を
      # 半角に寄せてから判定する。実測180件は全て「/月」だが、時給表記の分岐も残しておく。
      def self.work_format(reward_text)
        normalized_reward = reward_text.to_s.tr("／", "/")

        if normalized_reward.include?("/時")
          "時間単価制"
        elsif normalized_reward.include?("/月")
          "月額制（業務委託）"
        else
          "業務委託（フリーランス）"
        end
      end

      # 掲載日をUUIDから導出する。一覧HTMLに掲載日は出ないが、案件IDがUUIDv7（先頭48bitが
      # ミリ秒タイムスタンプ）なので追加リクエスト0回で取れる。
      # 「先頭48bitのミリ秒をUTCとして読んだ値が、そのままJST（日本時間）の壁時計」になる。
      # 詳細ページのNext.jsペイロードにある createdAt（"2026-09-10T04:03:37+09:00" のように
      # +09:00付き）と実データ11件で秒まで完全一致することを確認済み。つまりサイト側がJSTの
      # 壁時計でUUIDを採番している。localtimeで読むと9時間ずれるので必ずUTC固定で読む。
      #
      # ★照合先を間違えないこと。同じ詳細ページのJSON-LDにある datePosted は、この瞬間を
      # **UTCの日付**に直した値なので、JST9時より前に採番された案件では1日前になる
      # （実データ35件中25件がズレた）。掲載日として採るべきは createdAt 側のJST日付で、
      # 他ソース（ココナラ公開依頼・ビズリンク等）の掲載日もサイト表示のJST日付に揃えてある。
      #
      # これはサイトの実装詳細への依存なので、採番方式が変わったら（バージョンnibbleが7でない、
      # または日付が範囲外になり）nilを返すだけで壊れないようにガードしてある。
      def self.parse_posted_on(job_id, today:)
        normalized_id = job_id.to_s.delete("-")
        # 13桁目(index 12)がUUIDのバージョン。7以外はタイムスタンプを含まないので採用しない。
        return nil unless normalized_id.length == 32 && normalized_id[12] == "7"

        # 先頭48bit（12桁の16進数）がUnix時間のミリ秒。秒への切り捨てで日付は変わらない。
        milliseconds = normalized_id[0, 12].to_i(16)
        posted_on = Time.at(milliseconds / 1000).utc.to_date
        return nil unless posted_on.between?(EARLIEST_POSTED_ON, today + 1)

        posted_on
      end

      # 1スキル分の案件を、ページを進めながら集める。
      #
      # ★ページングの打ち切り条件
      # このサイトは「募集中の案件が先頭、募集終了が後ろ」に並ぶ（3スキル全ページで検証済み）。
      # したがって1ページ内に募集終了カードが1件でも現れたら、以降のページは全件が募集終了なので
      # そのスキルの取得を打ち切ってよい。この規則により通常は1スキル1リクエストで済む。
      # 「募集中のみ」に絞るURLクエリは実HTTPで3候補試したが全て無効（クライアント側フィルタと
      # みなす）だったため、バッジから自前で判定している。
      def collect_skill_postings(target)
        adopted_postings = []

        (1..@max_pages).each do |page_number|
          body = @fetcher.get(list_url(target[:skill_id], page_number))
          page_postings = self.class.parse(body, today: @today, category_hint: target[:hint])
          # カードが1件も取れないページは末尾を超えている（またはDOMが変わった）とみなす。
          break if page_postings.empty?

          closed_postings, open_postings = page_postings.partition do |posting|
            self.class.recruitment_closed?(posting)
          end
          adopted_postings.concat(@include_closed ? page_postings : open_postings)

          break unless closed_postings.empty?
        end

        adopted_postings
      end

      # 一覧URL。ブラウザで開くURLと同じ形（1ページ目もpage=1を明示）にしておく。
      def list_url(skill_id, page_number)
        "#{BASE_URL}/job-postings?skillIds=#{skill_id}&page=#{page_number}"
      end
      private :collect_skill_postings, :list_url
    end
  end
end
