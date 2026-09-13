# frozen_string_literal: true

require "nokogiri"

module FreelanceJobs
  module Sources
    # TECH STOCK: スキル別の案件一覧ページ（article.project-article-card）をHTMLパースする。
    # 完全SSRで、案件名・単価・勤務地・業務内容はすべてHTMLテキストとして入っている
    # （__NEXT_DATA__ のようなJSON埋め込みは無く、Vueは検索条件UIとお気に入りボタンだけ）。
    #
    # HTML構造の前提（壊れたらここを疑う。2026-09-12の実データ4一覧80カードで確認）:
    #   - カードは article.project-article-card。1ページ20件固定
    #   - 詳細リンクは絶対URL "https://tech-stock.com/projects/details/0136868/"。
    #     1カードに同じhrefのaが2本ある（案件名リンクと「詳細を見る」ボタン）
    #   - 案件名 h2.project-article-card-header__title の子spanは定型句「のエンジニア求人・案件」だけ
    #   - 単価は div.project-article-card-header__price の em（"〜90"）と span（"万円/月(税別)"）に分かれる
    #   - 職種・勤務地・契約形態・言語は「アイコンimg + テキストspan」の組。1ページ内で
    #     id属性が重複している不正HTMLのため、#id セレクタは使わず img の alt値で引く
    #   - 掲載日・応募締切は一覧に一切無い（一覧の ld+json は BreadcrumbList のみ）。
    #     詳細ページの ld+json には datePosted があるが、1件1リクエストになるため採用しない
    #   - 発注企業名は詳細ページでも "社名非公開" なので client は常に ""
    #
    # 一覧には募集終了案件が大量に混ざる（クエリ無しの実測で重複排除後69件中50件＝72%が募集終了。
    # 1ページ目は「募集中→募集終了」の順、2ページ目以降は全件が募集終了）。シートには
    # application_status 列が出るだけでフィルタが無く、そのままでは応募できない行で埋まる。
    # そこでサイト自身のナビゲーションリンクが使う募集中フィルタ ?onlyRecruiting=true を付けて取得する。
    # 実測でクエリ有り19件＝クエリ無しの募集中19件と完全一致し、取りこぼしは無い。
    # robots.txt の Disallow は /wp-admin/ と ?sort= / ?careerdefinitionvalues= /
    # ?lowerlimitstockingunitprice= / ?utm_source= / *maintenance* のみで、onlyRecruiting は対象外。
    # 過去案件から相場を見たい場合だけ only_recruiting: false を渡せば従来どおり全件取れる。
    # ページングはパス末尾の /2/ 形式で、クエリはその後ろに付く（サイトのページャhrefと同形式）。
    class TechStock
      SITE_NAME = "TECH STOCK"
      # ResearchService が HttpFetcher の間隔として参照するため、全ソースが持つ必要がある。
      REQUEST_INTERVAL = 1.5
      BASE_URL = "https://tech-stock.com"

      # 取得対象のスキル別一覧。category_hint は EngineerClassifier のカテゴリ名と一致させる。
      # 4集合の重なりは実測でごく僅か（募集中フィルタ込みで合計22件→重複排除後19件）なので、
      # 4本とも取っても無駄打ちにはならない。重複はURLキーで除去される。
      # 各行のコメントは 2026-09-12 の実測件数（募集中のみ / 募集終了を含む全件）。
      DEFAULT_SKILL_TARGETS = [
        { path: "skill/language_ruby", category_hint: "Ruby" },              # 8件 / 153件
        { path: "skill/framework_ruby-on-rails", category_hint: "Ruby" },    # 3件 /  85件
        { path: "skill/language_typescript", category_hint: "TypeScript" },  # 6件 / 162件
        { path: "skill/framework_react", category_hint: "React" }            # 5件 / 242件
      ].freeze

      # 1スキルあたりに辿る一覧ページ数の上限。既定1ページ（=4スキル＝4リクエスト）。
      # 募集中フィルタ込みの実測は ruby 8件 / rails 3件 / typescript 6件 / react 5件 と
      # いずれも1ページ（20件）に収まるため、既定では2ページ目を取りに行っても空振りになる。
      DEFAULT_MAX_PAGES = 1

      # サイトのナビゲーションが使う募集中フィルタ。これを付けると募集終了案件が一覧から消える。
      ONLY_RECRUITING_QUERY = "onlyRecruiting=true"

      CARD_SELECTOR = "article.project-article-card"
      DETAIL_LINK_SELECTOR = 'a[href*="/projects/details/"]'
      TITLE_SELECTOR = "h2.project-article-card-header__title"
      PRICE_SELECTOR = "div.project-article-card-header__price"
      # 一覧の project-info-table は80カード全件が「業務内容」1行のみ（他ラベルは一覧に出ない）。
      WORK_CONTENT_SELECTOR = "div.project-info-table__content"
      CHIP_SELECTOR = "span.project-article-card-header__chip"
      CLOSED_CHIP_SELECTOR = "span.project-article-card-header__chip--closed"
      REMOTE_CHIP_SELECTOR = "span.project-article-card-header__chip--remote"
      # パンくずの現在地。一覧ページが「どのスキルの一覧か」を示す唯一の安定した表示名
      # （h1は "Rubyのフリーランスエンジニア案件・求人一覧" と "Reactのフリーランスのエンジニア案件一覧" の
      #  2種類の接尾辞があり、機械的に剥がしにくい）。実測値は "Ruby" / "TypeScript" / "React" / "Ruby on Rails"。
      BREADCRUMB_SKILL_SELECTOR = "span.post-projects.current-item"
      PAGINATION_LINK_SELECTOR = "ul.pagination li a"

      # 単価の単位表記。サイト表記は半角の "/月" だが、全角の "／月" に変わっても拾えるようにしておく。
      MONTHLY_UNIT_RE = %r{/月|／月}.freeze
      HOURLY_UNIT_RE = %r{/時|／時}.freeze
      # 単価emの範囲・上限表記（"〜90" / "70〜90"）から数値だけを取り出す。小数表記（"7.5"）にも備える。
      AMOUNT_RE = /\d+(?:\.\d+)?/.freeze
      # 「〜90万円」のような上限表記かどうか。波ダッシュ・全角チルダ・半角チルダの3種を見る。
      UPPER_LIMIT_PREFIXES = ["〜", "～", "~"].freeze
      MAN_YEN_IN_YEN = 10_000

      # 言語spanに現れうる言語名の全集合（フッタsitemap「スキルから探す」の45件。
      # 全一覧ページに埋め込まれており、実データから抽出した表示名そのまま）。
      # 全角丸括弧に注意（Objective-C（iOS） / Swift（iOS） / Android（Java））。
      # サイト側に言語が追加されるとここが不足し、split_languages が警告を出して打ち切る。
      LANGUAGE_VOCABULARY = %w[
        Java PHP Scala Ruby Python Go言語 Perl Hack Elixir JavaScript
        XML HTML5 HTML CSS3 TypeScript CoffeeScript Objective-C（iOS） Swift（iOS） Android（Java） Kotlin
        VB.NET C# VBA VB VC++ ASP BASIC C言語 C++ Delphi
        PL/SQL Pro*C COBOL RPG PL/I YPS-COBOL JCL R言語 FORTRAN SQL
        Shell アセンブラ ストアドプロシージャ Actionscript Haskell
      ].freeze

      # 長い名前から順に突き合わせるための辞書。Java と JavaScript、HTML と HTML5、
      # VB と VB.NET、C言語 と C# と C++ の食い合いを「最長一致」で解決する。
      LANGUAGE_VOCABULARY_BY_LENGTH = LANGUAGE_VOCABULARY.sort_by { |name| -name.length }.freeze

      def initialize(fetcher:, today:, skill_targets: DEFAULT_SKILL_TARGETS, max_pages: DEFAULT_MAX_PAGES,
                     only_recruiting: true)
        @fetcher = fetcher
        @today = today
        @skill_targets = skill_targets
        @max_pages = max_pages
        @only_recruiting = only_recruiting
      end

      # 通信あり。スキルごとに一覧を1ページ目から最大 max_pages ページ分たどる。
      # 同じ案件が複数のスキル一覧に出るため、URLをキーに重複排除する。
      def fetch
        postings = {}

        @skill_targets.each do |skill_target|
          fetch_skill_target(skill_target).each { |posting| postings[posting.url] ||= posting }
        end

        postings.values
      end

      # 通信なし（テスト用）。一覧1ページ分のHTML本文から案件一覧を作る。
      # todayは全取得元共通のインターフェースとして受け取るが、TECH STOCKの一覧には
      # 掲載日も応募締切も無いため、この取得元では参照しない。
      def self.parse(body, today:, category_hint: nil)
        document = Nokogiri::HTML(body)
        skill_label = breadcrumb_skill_label(document)
        postings = {}

        document.css(CARD_SELECTOR).each do |card|
          posting = build_posting(card, category_hint, skill_label)
          next unless posting

          postings[posting.url] ||= posting
        end

        postings.values
      end

      # ページャの数字リンクから最終ページ番号を読む。表示は「1 2 3 … 6 7 8 次へ>」の形で、
      # 先頭3ページと末尾3ページだけが並ぶため、数字リンクの最大値がそのまま最終ページになる
      # （末尾の「次へ>」はテキストが数字でないので除外される）。ページャが無ければ1ページ。
      def self.last_page_number(body)
        page_numbers = Nokogiri::HTML(body).css(PAGINATION_LINK_SELECTOR)
                                          .map { |link| link.text.strip }
                                          .select { |text| text.match?(/\A\d+\z/) }
                                          .map(&:to_i)
        page_numbers.empty? ? 1 : page_numbers.max
      end

      # 案件URLと案件名は「行としての最低条件」かつ重複排除のキーなので、どちらかが取れない
      # カードは行を壊さないよう黙って捨てる。セレクタが変わって全カードが欠損した場合は
      # 0件になり、ResearchServiceのログ（取得件数）で気付ける。
      def self.build_posting(card, category_hint, skill_label)
        detail_page_url = detail_page_url(card)
        return nil if detail_page_url.empty?

        title = card_title(card)
        return nil if title.empty?

        price_node = card.at_css(PRICE_SELECTOR)
        amount_text = squish(price_node&.at_css("em")&.text)
        unit_text = squish(price_node&.at_css("span")&.text)
        languages = split_languages(attribute_text(card, "言語"))

        FreelanceJobs::JobPosting.new(
          site: SITE_NAME,
          url: FreelanceJobs::JobPosting.normalize_url(detail_page_url),
          title: title,
          description: FreelanceJobs::JobPosting.normalize_description(build_description(card, languages)),
          category_hint: category_hint,
          reward: build_reward(amount_text, unit_text),
          work_format: work_format(unit_text),
          application_status: card.at_css(CLOSED_CHIP_SELECTOR) ? "募集終了" : "募集中",
          deadline_text: "-", # 一覧に応募締切の記載が無いサイト
          deadline_on: nil,
          skills: build_skills(skill_label, languages),
          client: "", # 発注企業名は詳細ページでも "社名非公開"
          tags: build_tags(card),
          posted_on: nil # 一覧に掲載日の記載が無いサイト
        )
      end

      # 詳細リンクは実データでは絶対URL。将来相対パスに変わっても拾えるよう、
      # スキームが無いときだけ BASE_URL を前置する。
      def self.detail_page_url(card)
        link_node = card.at_css(DETAIL_LINK_SELECTOR)
        return "" unless link_node

        href = link_node["href"].to_s.strip
        return "" if href.empty?

        href.start_with?("http") ? href : "#{BASE_URL}#{href}"
      end

      # 案件名の子spanは定型句「のエンジニア求人・案件」のみ。除去しないと全案件の末尾に付く。
      # 複製してから除去するので、呼び出し元のDOM（tags等の後続処理）は壊さない。
      def self.card_title(card)
        title_node = card.at_css(TITLE_SELECTOR)
        return "" unless title_node

        title_without_suffix = title_node.dup
        title_without_suffix.css("span").each(&:remove)
        squish(title_without_suffix.text)
      end

      # 属性行（職種・勤務地・契約形態・言語・月額報酬）は「アイコンimg + テキストspan」の組。
      # id属性が1ページ内で重複している不正HTMLなので、#id ではなく img の alt値で引く。
      def self.attribute_text(card, alt_text)
        squish(card.at_css(%(img[alt="#{alt_text}"] + span))&.text)
      end

      # 言語spanは区切り文字がまったく無い連結文字列（実例 "PHPJavaRubyPythonGo言語JavaScript"）。
      # そのまま skills に入れると EngineerClassifier の単語境界正規表現
      # （(?<![A-Za-z])Ruby(?![A-Za-z]) 等）が一致せず、Ruby案件がRubyと判定されない。
      # そこでサイトの語彙辞書を使い、左から最長一致で切り出す。
      def self.split_languages(languages_text)
        remaining_text = languages_text.gsub(/[[:space:]]/, "")
        languages = []

        until remaining_text.empty?
          matched_name = LANGUAGE_VOCABULARY_BY_LENGTH.find { |name| remaining_text.start_with?(name) }
          # 辞書に無い言語が増えると、そこから先を切り出せない。黙って捨てると語彙の不足に
          # 気付けないため、残りを警告に出して打ち切る（LANGUAGE_VOCABULARYに追記すれば直る）。
          unless matched_name
            FreelanceJobs.logger.warn(
              "[FreelanceJobs::Sources::TechStock] 言語辞書に無い表記です: #{remaining_text.inspect}"
            )
            break
          end

          languages << matched_name
          remaining_text = remaining_text[matched_name.length..]
        end

        languages
      end

      # 言語spanにはフレームワーク名（React / Ruby on Rails）が入らず、React一覧では
      # 言語spanごと存在しないカードも多い（実測20件中9件）。skillsに "React" が1つも出ないと
      # 分類がcategory_hint頼みの弱い判定になるため、一覧ページ自身のパンくずから取った
      # スキル表示名を必ず先頭に足す。
      def self.build_skills(skill_label, languages)
        ([skill_label] + languages).reject(&:empty?).uniq
      end

      def self.breadcrumb_skill_label(document)
        squish(document.at_css(BREADCRUMB_SKILL_SELECTOR)&.text)
      end

      # descriptionはEngineerClassifierの判定テキストにそのまま入るので、技術名が載る
      # 業務内容・職種・使用言語を先に置く。勤務地とリモートチップに「リモート」「在宅」が
      # 入るため、REMOTE_RE（memoの「リモート可」）もここで効く。
      def self.build_description(card, languages)
        work_content = squish(card.at_css(WORK_CONTENT_SELECTOR)&.text)
        job_category = attribute_text(card, "職種")
        work_location = attribute_text(card, "勤務地")
        contract_type = attribute_text(card, "契約形態")
        remote_labels = card.css(REMOTE_CHIP_SELECTOR).map { |chip| squish(chip.text) }.reject(&:empty?)

        parts = []
        parts << "業務内容: #{work_content}" unless work_content.empty?
        parts << "職種: #{job_category}" unless job_category.empty?
        parts << "使用言語: #{languages.join(" / ")}" unless languages.empty?
        parts << "勤務地: #{work_location}" unless work_location.empty?
        parts << remote_labels.join(" / ") unless remote_labels.empty?
        parts << "契約形態: #{contract_type}" unless contract_type.empty?
        parts.join(" / ")
      end

      # チップは実測3種（__chip--remote "一部リモート"/"フルリモート"、__chip--closed "募集終了"、
      # __chip--new "New"）。DOM順のまま入れるので、募集終了案件は先頭が "募集終了" になる。
      def self.build_tags(card)
        card.css(CHIP_SELECTOR).map { |chip| squish(chip.text) }.reject(&:empty?)
      end

      # 単価は em="〜90" と span="万円/月(税別)" の2要素に分かれている。万円表記のままだと
      # 読みにくいうえ他サイトと桁が揃わないため、円に直して "〜900,000円／月（税別）" にする。
      # EngineerClassifier.high_reward? はこの文字列から先頭の900000を読み、月額30万円以上を
      # 高単価と判定する（"時給"等の語を含まない表記なので月額の閾値が適用される）。
      def self.build_reward(amount_text, unit_text)
        amounts = amount_text.scan(AMOUNT_RE)
        # emが空＝単価非公開。levtechに合わせて表示用の "要確認" を入れる（nilは入れない）。
        return "要確認" if amounts.empty?

        # 0.1万円単位の端数に備えて一度Floatにするが、8.3 * 10_000 が 82999.99… になる
        # 浮動小数点誤差があるため、to_i（切り捨て）ではなくroundで整数化する。
        formatted_amounts = amounts.map do |amount|
          FreelanceJobs.format_number((amount.to_f * MAN_YEN_IN_YEN).round)
        end

        "#{upper_limit_prefix(amount_text)}#{formatted_amounts.join("〜")}#{reward_unit(unit_text)}#{tax_note(unit_text)}"
      end

      def self.upper_limit_prefix(amount_text)
        amount_text.start_with?(*UPPER_LIMIT_PREFIXES) ? "〜" : ""
      end

      def self.reward_unit(unit_text)
        return "円／月" if unit_text.match?(MONTHLY_UNIT_RE)
        return "円／時" if unit_text.match?(HOURLY_UNIT_RE)

        "円"
      end

      def self.tax_note(unit_text)
        return "（税別）" if unit_text.include?("税別")
        return "（税込）" if unit_text.include?("税込")

        ""
      end

      # 規約どおり単価の単位表記で判定する。実測は全件が月額（"万円/月(税別)"）だが、
      # 時給表記が現れても拾えるようにしておく。契約形態span（"業務委託（フリーランス）"／"派遣"）は
      # 単価ではなく契約の種類なのでdescription側に回す。
      def self.work_format(unit_text)
        return "月額制（業務委託）" if unit_text.match?(MONTHLY_UNIT_RE)
        return "時間単価制" if unit_text.match?(HOURLY_UNIT_RE)

        "業務委託（フリーランス）"
      end

      def self.squish(text)
        text.to_s.gsub(/[[:space:]]+/, " ").strip
      end

      private

      # 1スキルぶんのページ送り。1ページ目は必ず取り、2ページ目以降は max_pages と
      # ページャが示す最終ページ番号の小さい方まで進む。
      def fetch_skill_target(skill_target)
        first_page_body = @fetcher.get(list_url(skill_target[:path], 1))
        postings = parse_page(first_page_body, skill_target)
        return postings if @max_pages <= 1

        # 存在しないページ番号を叩くと404になるため、ページャから読んだ最終ページで上限を抑える。
        last_page_number = [self.class.last_page_number(first_page_body), @max_pages].min

        (2..last_page_number).each do |page_number|
          page_postings = parse_page(@fetcher.get(list_url(skill_target[:path], page_number)), skill_target)
          # 0件のページはページ終端かセレクタ崩れ。それ以上リクエストしても無駄なので打ち切る。
          break if page_postings.empty?

          postings.concat(page_postings)
        end

        postings
      end

      def parse_page(body, skill_target)
        self.class.parse(body, today: @today, category_hint: skill_target[:category_hint])
      end

      # 2ページ目以降はパス末尾に "/2/" を足す形式（末尾スラッシュ必須）。募集中フィルタは
      # その後ろにクエリとして付く（サイトのページャhrefが "/projects/2/?onlyRecruiting=true" 形式）。
      # robots.txt が禁止しているクエリ（sort / careerdefinitionvalues /
      # lowerlimitstockingunitprice / utm_source）は付けない。
      def list_url(path, page_number)
        page_path = page_number <= 1 ? "" : "#{page_number}/"
        query = @only_recruiting ? "?#{ONLY_RECRUITING_QUERY}" : ""

        "#{BASE_URL}/projects/#{path}/#{page_path}#{query}"
      end
    end
  end
end
