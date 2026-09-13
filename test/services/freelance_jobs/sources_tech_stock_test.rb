# frozen_string_literal: true
# test/services/freelance_jobs/sources_tech_stock_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesTechStockTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 12)
  RUBY_FIXTURE = "tech_stock_ruby.html"
  REACT_FIXTURE = "tech_stock_react.html"

  def parse_fixture(name, category_hint:)
    FreelanceJobs::Sources::TechStock.parse(read_fixture(name), today: TODAY, category_hint: category_hint)
  end

  # --- 一覧1ページ分の件数（TECH STOCKは20件/頁固定） ---

  def test_parse_fixture_returns_twenty_postings
    assert_equal 20, parse_fixture(RUBY_FIXTURE, category_hint: "Ruby").size
  end

  # --- 1件目の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture(RUBY_FIXTURE, category_hint: "Ruby").first

    assert_equal "TECH STOCK", first.site
    assert_equal "https://tech-stock.com/projects/details/0136868", first.url
    assert_equal "【Ruby/Rails】既存システム保守開発｜自動車", first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "〜900,000円／月（税別）", first.reward
    assert_equal "月額制（業務委託）", first.work_format
    assert_equal "募集中", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_equal ["Ruby"], first.skills
    assert_equal "", first.client
    assert_equal ["一部リモート"], first.tags
    assert_nil first.posted_on
    assert_equal(
      "業務内容: ・既存システム保守開発によるユーザー価値向上PJ推進 ・最新技術/開発ツールを活用したアーキ設計及び実装 " \
      "・障害検知/対策を含む運用保守プロセス最適化 / 職種: システムエンジニア・プログラマー / IT / 使用言語: Ruby / " \
      "勤務地: 東京都港区 / 品川駅 / 一部リモート / 契約形態: 業務委託（フリーランス）",
      first.description
    )
  end

  # --- URL正規化（絶対URL・末尾スラッシュなし・クエリなし） ---

  def test_urls_are_normalized_absolute_without_trailing_slash_or_query
    parse_fixture(RUBY_FIXTURE, category_hint: "Ruby").each do |posting|
      assert_match %r{\Ahttps://tech-stock\.com/projects/details/\d+\z}, posting.url,
                   "末尾スラッシュなし・クエリなしの正規化されたURLのはず"
    end
  end

  # --- category_hint が引数どおり全件に伝わる ---

  def test_category_hint_is_propagated_to_every_posting
    postings = parse_fixture(REACT_FIXTURE, category_hint: "React")

    assert(postings.all? { |posting| posting.category_hint == "React" },
           "全件のcategory_hintが引数のReactになるはず")
  end

  # --- 案件名の定型句（子span「のエンジニア求人・案件」）を落とす ---

  def test_titles_drop_boilerplate_suffix_span
    postings = parse_fixture(RUBY_FIXTURE, category_hint: "Ruby")

    refute(postings.any? { |posting| posting.title.include?("のエンジニア求人・案件") },
           "案件名の末尾に付く定型句は除去されるはず")
    refute(postings.any? { |posting| posting.title.empty? })
  end

  # --- 言語span（区切り文字なしの連結文字列）を辞書で分割する ---

  def test_skills_split_concatenated_language_span
    fourth = parse_fixture(RUBY_FIXTURE, category_hint: "Ruby")[3]

    # 言語span生値は "PHPJavaRubyPythonGo言語JavaScript"。先頭はパンくず由来の "Ruby"。
    assert_equal ["Ruby", "PHP", "Java", "Python", "Go言語", "JavaScript"], fourth.skills
  end

  # 語彙（LANGUAGE_VOCABULARY）が不足すると未知語で分割が打ち切られ、分類が静かに劣化する。
  # フィクスチャに出る全カードの言語spanが「1文字も余さず消化できる」ことで語彙不足を検知する。
  def test_language_vocabulary_consumes_every_concatenated_language_span
    [RUBY_FIXTURE, REACT_FIXTURE].each do |fixture_name|
      Nokogiri::HTML(read_fixture(fixture_name)).css("article.project-article-card").each do |card|
        languages_text = card.at_css('img[alt="言語"] + span')&.text.to_s.gsub(/[[:space:]]/, "")
        next if languages_text.empty?

        split_languages = FreelanceJobs::Sources::TechStock.split_languages(languages_text)

        assert_equal languages_text, split_languages.join,
                     "#{fixture_name}: 言語span #{languages_text.inspect} を辞書で全部分割できるはず"
      end
    end
  end

  # --- 言語spanが無いカードでもパンくず由来のスキル名が残る（React一覧は20件中9件が該当） ---

  def test_skills_fall_back_to_breadcrumb_skill_when_language_span_is_missing
    third = parse_fixture(REACT_FIXTURE, category_hint: "React")[2]

    assert_equal "https://tech-stock.com/projects/details/0144309", third.url
    assert_equal ["React"], third.skills, "言語spanが無くてもパンくずの「React」がskillsに入るはず"
    assert_equal "〜1,080,000円／月（税別）", third.reward
  end

  # --- 募集終了チップのある案件は application_status と tags に出す（除外はしない） ---

  def test_closed_posting_keeps_status_and_tags
    closed = parse_fixture(REACT_FIXTURE, category_hint: "React")
             .find { |posting| posting.url.end_with?("/0146733") }

    refute_nil closed
    assert_equal "募集終了", closed.application_status
    assert_equal ["募集終了", "一部リモート"], closed.tags
  end

  # --- 単価: 万円表記を円に直す（範囲表記・小数・単価非公開・時給表記） ---

  def test_reward_converts_range_of_man_yen_to_yen
    postings = parse_card_fragment(amount: "70〜90", unit: "万円/月(税別)")

    assert_equal "700,000〜900,000円／月（税別）", postings.first.reward
    assert_equal "月額制（業務委託）", postings.first.work_format
  end

  # 8.3 * 10_000 は浮動小数点で 82999.99… になる。切り捨てると1円ずれるのでroundしている。
  def test_reward_rounds_fractional_man_yen_without_losing_one_yen
    postings = parse_card_fragment(amount: "〜8.3", unit: "万円/月(税別)")

    assert_equal "〜83,000円／月（税別）", postings.first.reward
  end

  def test_reward_is_unconfirmed_when_price_is_missing
    postings = parse_card_fragment(amount: "", unit: "")

    assert_equal "要確認", postings.first.reward
    assert_equal "業務委託（フリーランス）", postings.first.work_format
  end

  def test_work_format_is_hourly_when_unit_is_per_hour
    postings = parse_card_fragment(amount: "〜0.75", unit: "万円/時(税別)")

    assert_equal "〜7,500円／時（税別）", postings.first.reward
    assert_equal "時間単価制", postings.first.work_format
  end

  # --- 必須要素（詳細リンク・案件名）が欠けたカードは黙って除外する ---

  def test_parse_skips_card_without_detail_link
    fragment = <<~HTML
      <article class="project-article-card">
        <h2 class="project-article-card-header__title">リンクなし<span>のエンジニア求人・案件</span></h2>
      </article>
    HTML

    assert_equal [], FreelanceJobs::Sources::TechStock.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_skips_card_without_title
    fragment = <<~HTML
      <article class="project-article-card">
        <a href="https://tech-stock.com/projects/details/0000001/">詳細を見る</a>
      </article>
    HTML

    assert_equal [], FreelanceJobs::Sources::TechStock.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  # --- ページャから最終ページ番号を読む ---

  def test_last_page_number_reads_maximum_numeric_pagination_link
    # Ruby一覧は153件=8ページ、React一覧は242件=13ページ（「次へ>」は数字でないため無視される）。
    assert_equal 8, FreelanceJobs::Sources::TechStock.last_page_number(read_fixture(RUBY_FIXTURE))
    assert_equal 13, FreelanceJobs::Sources::TechStock.last_page_number(read_fixture(REACT_FIXTURE))
    assert_equal 1, FreelanceJobs::Sources::TechStock.last_page_number(wrap_html("<div>ページャなし</div>")),
                 "ページャが無い一覧は1ページとみなすはず"
  end

  # --- fetch（通信なし・Fakeフェッチャー） ---

  # どのURLでも同じbodyを返すFakeフェッチャー（呼び出されたURLを記録する）。
  class RecordingFetcher
    def initialize(body:)
      @body = body
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      @body
    end
  end

  def test_fetch_requests_each_skill_target_once_and_deduplicates_urls
    fetcher = RecordingFetcher.new(body: read_fixture(RUBY_FIXTURE))
    skill_targets = [
      { path: "skill/language_ruby", category_hint: "Ruby" },
      { path: "skill/framework_ruby-on-rails", category_hint: "Ruby" }
    ]
    source = FreelanceJobs::Sources::TechStock.new(fetcher: fetcher, today: TODAY, skill_targets: skill_targets)

    postings = source.fetch

    assert_equal ["https://tech-stock.com/projects/skill/language_ruby/?onlyRecruiting=true",
                  "https://tech-stock.com/projects/skill/framework_ruby-on-rails/?onlyRecruiting=true"],
                 fetcher.requested_urls, "既定のmax_pagesは1なのでスキルごとに1リクエストのはず"
    assert_equal 20, postings.size, "2スキルとも同じ案件が返っても重複排除され20件のままのはず"
  end

  # robots.txt が Disallow にしているクエリ（サイトの絞り込みUIが使うもの）。
  DISALLOWED_QUERY_RE = /sort=|careerdefinitionvalues=|lowerlimitstockingunitprice=|utm_source=/

  # 一覧の72%が募集終了案件なので、サイト自身のナビが使う募集中フィルタを既定で付ける。
  def test_fetch_appends_only_recruiting_query_and_no_disallowed_query
    fetcher = RecordingFetcher.new(body: read_fixture(RUBY_FIXTURE))
    source = FreelanceJobs::Sources::TechStock.new(fetcher: fetcher, today: TODAY, max_pages: 3)
    source.fetch

    assert(fetcher.requested_urls.all? { |url| url.end_with?("/?onlyRecruiting=true") },
           "一覧URLは末尾スラッシュ＋募集中フィルタのはず")
    refute(fetcher.requested_urls.any? { |url| url.match?(DISALLOWED_QUERY_RE) },
           "robots.txtが禁止しているクエリを付けてはいけない")
  end

  # 過去案件から相場を見たいときのために、募集中フィルタは外せるようにしてある。
  def test_fetch_omits_query_when_only_recruiting_is_disabled
    fetcher = RecordingFetcher.new(body: read_fixture(RUBY_FIXTURE))
    skill_targets = [{ path: "skill/language_ruby", category_hint: "Ruby" }]
    source = FreelanceJobs::Sources::TechStock.new(fetcher: fetcher, today: TODAY,
                                                   skill_targets: skill_targets, only_recruiting: false)

    source.fetch

    assert_equal ["https://tech-stock.com/projects/skill/language_ruby/"], fetcher.requested_urls
  end

  def test_fetch_follows_pages_up_to_max_pages
    fetcher = RecordingFetcher.new(body: read_fixture(RUBY_FIXTURE))
    skill_targets = [{ path: "skill/language_ruby", category_hint: "Ruby" }]
    source = FreelanceJobs::Sources::TechStock.new(fetcher: fetcher, today: TODAY,
                                                   skill_targets: skill_targets, max_pages: 3)

    source.fetch

    assert_equal ["https://tech-stock.com/projects/skill/language_ruby/?onlyRecruiting=true",
                  "https://tech-stock.com/projects/skill/language_ruby/2/?onlyRecruiting=true",
                  "https://tech-stock.com/projects/skill/language_ruby/3/?onlyRecruiting=true"],
                 fetcher.requested_urls
  end

  # 存在しないページ番号（404）を叩かないよう、ページャの最終ページで上限を抑える。
  def test_fetch_stops_at_last_page_even_when_max_pages_is_larger
    fetcher = RecordingFetcher.new(body: read_fixture(RUBY_FIXTURE))
    skill_targets = [{ path: "skill/language_ruby", category_hint: "Ruby" }]
    source = FreelanceJobs::Sources::TechStock.new(fetcher: fetcher, today: TODAY,
                                                   skill_targets: skill_targets, max_pages: 20)

    source.fetch

    assert_equal 8, fetcher.requested_urls.size, "Ruby一覧は全8ページなので9ページ目以降は取りに行かないはず"
    assert_equal "https://tech-stock.com/projects/skill/language_ruby/8/?onlyRecruiting=true",
                 fetcher.requested_urls.last
  end

  # --- 既定の取得対象（4スキル＝既定4リクエスト） ---

  def test_default_skill_targets_cover_four_skills_without_query
    targets = FreelanceJobs::Sources::TechStock::DEFAULT_SKILL_TARGETS

    assert_equal ["skill/language_ruby", "skill/framework_ruby-on-rails",
                  "skill/language_typescript", "skill/framework_react"],
                 targets.map { |target| target[:path] }
    assert_equal ["Ruby", "Ruby", "TypeScript", "React"], targets.map { |target| target[:category_hint] }
    assert_equal 1, FreelanceJobs::Sources::TechStock::DEFAULT_MAX_PAGES
  end

  # --- Profile::ENGINEER にTechStockが含まれる（BEGINNERには含まれない） ---

  def test_engineer_profile_includes_tech_stock_source
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_includes source_classes, FreelanceJobs::Sources::TechStock
  end

  def test_beginner_profile_does_not_include_tech_stock_source
    source_classes = FreelanceJobs::Profile::BEGINNER.source_specs.map(&:first)

    refute_includes source_classes, FreelanceJobs::Sources::TechStock
  end

  private

  # 一覧カード1件分のHTML片（実データのDOM構造を模したもの）。
  # 単価はem（金額）とspan（単位）に分かれており、実データに無い時給表記・単価非公開を
  # 再現するためにここで組み立てる。
  def build_card_html(amount:, unit:, title: "【Ruby】テスト案件", detail_url: "https://tech-stock.com/projects/details/0000001/")
    <<~HTML
      <article class="project-article-card">
        <div class="project-article-card-header">
          <h2 class="project-article-card-header__title">
            <a href="#{detail_url}">#{title}<span>のエンジニア求人・案件</span></a>
          </h2>
          <span class="project-article-card-header__chip project-article-card-header__chip--remote">フルリモート</span>
          <div class="project-article-card-header__price"><em>#{amount}</em><span>#{unit}</span></div>
        </div>
        <ul>
          <li><img alt="職種" src="/icon.svg"><span>システムエンジニア・プログラマー</span></li>
          <li><img alt="勤務地" src="/icon.svg"><span>東京都港区</span></li>
          <li><img alt="契約形態" src="/icon.svg"><span>業務委託（フリーランス）</span></li>
          <li><img alt="言語" src="/icon.svg"><span>Ruby</span></li>
        </ul>
        <div class="project-info-table">
          <div class="project-info-table__label">業務内容</div>
          <div class="project-info-table__content">既存システムの保守開発</div>
        </div>
        <a href="#{detail_url}">詳細を見る</a>
      </article>
    HTML
  end

  # パンくず（スキル表示名）付きの一覧ページを組み立ててパースする。
  def parse_card_fragment(amount:, unit:, skill_label: "Ruby")
    body = wrap_html(<<~HTML)
      <nav><span class="post-projects current-item">#{skill_label}</span></nav>
      #{build_card_html(amount: amount, unit: unit)}
    HTML

    FreelanceJobs::Sources::TechStock.parse(body, today: TODAY, category_hint: skill_label)
  end
end
