# frozen_string_literal: true
# test/services/freelance_jobs/sources_engineer_factory_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"
require "json"

class FreelanceJobsSourcesEngineerFactoryTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 13)
  # /freelance/jobs/skill/22003（Ruby）の1ページ目そのまま（40カード＋__NUXT_DATA__）。
  FIXTURE_NAME = "engineer_factory_ruby.html"
  LIST_URL_PREFIX = "https://www.engineer-factory.com/freelance/jobs/skill"

  def parse_fixture(category_hint: "Ruby")
    FreelanceJobs::Sources::EngineerFactory.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  def parse_fragment(fragment, category_hint: "Ruby")
    FreelanceJobs::Sources::EngineerFactory.parse(wrap_html(fragment), today: TODAY, category_hint: category_hint)
  end

  # 一覧は40件/頁。同じhrefが「詳細を見る」ボタンにもあるが、h3側だけを案件として数える。
  def test_parse_fixture_returns_forty_postings
    assert_equal 40, parse_fixture.size
  end

  # --- 1件目の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "エンジニアファクトリー", first.site
    assert_equal "https://www.engineer-factory.com/freelance/jobs/136813", first.url
    assert_equal "【飯田橋/Go・Java】AI×IoTで駐車場DXを推進するシニアソフトウエアエンジニア", first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "650,000〜700,000円／月", first.reward
    assert_equal "月額制（業務委託）", first.work_format
    assert_equal %w[Java Ruby SQL Ruby\ on\ Rails MySQL MariaDB AWS Git Terraform Linux Tomcat Apache Docker], first.skills
    assert_equal ["リモート可"], first.tags
    assert_equal "", first.client
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_equal Date.new(2026, 7, 17), first.posted_on
  end

  # descriptionは「業務内容（NUXT） / 必須スキル / 歓迎スキル（NUXT） / 使用技術 / 職種 / 勤務地 / 業界 / タグ」の順。
  # 業務内容の技術スタック・使用技術が入ることでEngineerClassifierの技術判定が本文で決まる。
  def test_parse_first_posting_description_joins_all_sections_in_order
    description = parse_fixture.first.description

    assert description.start_with?("業務内容: AI・IoT・Web技術を活用した自社プロダクトの企画から設計、開発、テスト、リリース、運用保守まで一貫して担当します。"),
           description
    assert_includes description, "【技術スタック】 ・開発言語：Go、Java、Ruby on Rails ・インフラ：AWS（ECS、Lambda"
    assert_includes description, " / 必須スキル: ・Webアプリケーションのバックエンド開発経験2年以上（JavaまたはGoでの実務歓迎） / "
    assert_includes description, " / 歓迎スキル: ・AWS上でのインフラ構築・設計・運用経験 ・要件定義・技術選定を含むプロジェクトマネジメント経験"
    assert description.end_with?(
      " / 使用技術: Java / Ruby / SQL / Ruby on Rails / MySQL / MariaDB / AWS / Git / Terraform / Linux / Tomcat / Apache / Docker" \
      " / 職種: バックエンドエンジニア / 勤務地: 東京都（最寄駅: 飯田橋（東京都）） / 業界: ソフトウェア・情報処理・メーカー（その他メーカー） / リモート可"
    ), description

    section_labels = description.scan(/(?:\A| \/ )([^ ]+?): /).flatten
    assert_equal %w[業務内容 必須スキル 歓迎スキル 使用技術 職種 勤務地 業界], section_labels
  end

  # 単価の生テキスト「月額 65 ～ 70 万円」はClassifier::SUSPICIOUS_RE（月N万）に誤爆するのでdescriptionに入れない。
  def test_description_does_not_include_raw_wage_text
    parse_fixture.each do |posting|
      refute_match(/月額\s*\d+/, posting.description, posting.url)
    end
  end

  # --- 案件名のSEO接尾辞（" | " ＋ <span>東京都の案件・求人</span>）を落とす ---

  def test_title_excludes_seo_suffix_and_trailing_separator
    parse_fixture.each do |posting|
      refute_includes posting.title, "|", posting.url
      refute_includes posting.title, "の案件・求人", posting.url
    end
  end

  # --- バッジ（Hot）はタグの先頭、こだわりタグが続く。単価は100万円台も換算できる ---

  def test_parse_posting_with_hot_badge_and_multiple_tags
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/116491") }

    refute_nil posting
    assert_equal "【フルリモート/AWS】負荷分散とビッグデータを考慮した構築経験のあるインフラエンジニア", posting.title
    assert_equal "1,000,000〜1,080,000円／月", posting.reward
    assert_equal ["Hot", "フルリモート", "長期案件"], posting.tags
    assert_equal ["Ruby", "Ruby on Rails", "AWS", "Linux"], posting.skills
    assert_equal Date.new(2025, 9, 1), posting.posted_on
    assert_includes posting.description, "職種: クラウドエンジニア"
    assert_includes posting.description, "勤務地: 東京都（最寄駅: 五反田（東京都））"
  end

  # --- 全件で必須フィールドが埋まる（"-"や""は許すがnilは許さない） ---

  def test_every_posting_fills_display_fields
    postings = parse_fixture

    postings.each do |posting|
      refute_empty posting.title, "案件名が空のカードは除外されるはず"
      assert_match(/\A[\d,]+〜[\d,]+円／月\z/, posting.reward, "フィクスチャは全件が月額の範囲表記のはず")
      assert_equal "月額制（業務委託）", posting.work_format
      refute_empty posting.skills, "絞り込みスキルの一覧なので使用技術は必ず入るはず"
      assert_includes posting.skills, "Ruby", "Ruby絞り込みの一覧なので全件にRubyタグがあるはず"
      assert_instance_of Array, posting.tags
      assert_includes posting.description, "業務内容: ", "__NUXT_DATA__から業務内容が補完されるはず"
    end
    # publication_date_ef が空文字の案件が1件だけあり、その posted_on は nil になる。
    assert_equal 1, postings.count { |posting| posting.posted_on.nil? }
  end

  # --- URL正規化 ---

  def test_urls_are_normalized_absolute_without_trailing_slash_or_query
    urls = parse_fixture.map(&:url)

    urls.each do |url|
      assert_match %r{\Ahttps://www\.engineer-factory\.com/freelance/jobs/\d+\z}, url,
                   "末尾スラッシュなし・クエリなしの正規化された絶対URLのはず"
    end
    assert_equal urls.uniq.size, urls.size
  end

  # --- category_hint が引数どおり全件に伝わる ---

  def test_category_hint_is_propagated_to_every_posting
    postings = parse_fixture(category_hint: "TypeScript")

    assert(postings.all? { |posting| posting.category_hint == "TypeScript" },
           "全件のcategory_hintが引数のTypeScriptになるはず")
  end

  # --- 単価表記のバリエーション ---

  def test_reward_without_upper_bound_uses_single_amount
    posting = parse_fragment(build_card_html(job_id: 1, wage_text: "月額 <strong>65</strong> 万円")).first

    assert_equal "650,000円／月", posting.reward
    assert_equal "月額制（業務委託）", posting.work_format
  end

  def test_reward_falls_back_when_wage_is_not_monthly
    posting = parse_fragment(build_card_html(job_id: 2, wage_text: "時給 <strong>5,000</strong> 円")).first

    assert_equal "要確認", posting.reward
    assert_equal "業務委託（フリーランス）", posting.work_format
  end

  def test_reward_falls_back_when_wage_definition_list_is_missing
    posting = parse_fragment(build_card_html(job_id: 3, wage_text: nil)).first

    assert_equal "要確認", posting.reward
    assert_equal "業務委託（フリーランス）", posting.work_format
  end

  # --- 業界の dl が無いカードも落ちない ---

  def test_parse_card_without_industry
    posting = parse_fragment(build_card_html(job_id: 4, industries: [])).first

    refute_nil posting
    refute_includes posting.description, "業界:"
    assert_includes posting.description, "勤務地: 東京都"
  end

  # --- 必須要素（案件パス・案件名）が欠けたカードは黙って除外する ---

  def test_parse_skips_card_without_title_link
    fragment = <<~HTML
      <section class="modJobBlock modJobBlock--large">
        <h3 class="modJobBlock__title">リンクなし</h3>
        <a href="/freelance/jobs/5" class="modBtnSearch">詳細を見る</a>
      </section>
    HTML

    assert_equal [], parse_fragment(fragment)
  end

  def test_parse_skips_card_whose_href_is_not_a_job_path
    assert_equal [], parse_fragment(build_card_html(job_id: nil, href: "/freelance/jobs/area/13"))
  end

  def test_parse_skips_card_without_title_text
    assert_equal [], parse_fragment(build_card_html(job_id: 6, title: ""))
  end

  # --- __NUXT_DATA__ の補完は任意扱い（無い・壊れていてもHTML分は返す） ---

  def test_parse_without_nuxt_data_returns_html_only_posting
    posting = parse_fragment(build_card_html(job_id: 7)).first

    refute_nil posting
    assert_nil posting.posted_on
    refute_includes posting.description, "業務内容:"
    assert description_without_nuxt = posting.description
    assert_equal(
      "必須スキル: ・Ruby経験3年以上・AWS経験 / 使用技術: Ruby / Ruby on Rails / 職種: バックエンドエンジニア" \
      " / 勤務地: 東京都 / 業界: インターネット・通信 / New・リモート可",
      description_without_nuxt
    )
  end

  def test_parse_with_broken_nuxt_data_returns_html_only_posting
    fragment = build_card_html(job_id: 8) + %(<script id="__NUXT_DATA__" type="application/json">[["ShallowReactive",1],{"data":</script>)
    posting = parse_fragment(fragment).first

    refute_nil posting
    assert_nil posting.posted_on
    refute_includes posting.description, "業務内容:"
  end

  def test_parse_supplements_posted_on_and_duties_from_nuxt_data
    fragment = build_card_html(job_id: 9) + build_nuxt_data_html(
      job_id: "9", publication_date: "2026-09-10", duties: "Railsで\n開発する",
      preferred_skills: "・TypeScript経験", nearest_stations: "渋谷（東京都）"
    )
    posting = parse_fragment(fragment).first

    assert_equal Date.new(2026, 9, 10), posting.posted_on
    assert description = posting.description
    assert description.start_with?("業務内容: Railsで 開発する / 必須スキル: "), description
    assert_includes description, " / 歓迎スキル: ・TypeScript経験 / "
    assert_includes description, "勤務地: 東京都（最寄駅: 渋谷（東京都））"
  end

  def test_posted_on_is_nil_when_publication_date_is_unexpected
    fragment = build_card_html(job_id: 10) + build_nuxt_data_html(job_id: "10", publication_date: "2026/09/10")

    assert_nil parse_fragment(fragment).first.posted_on
  end

  # --- fetch: search_targets × ページ数だけ取得し、URL重複を排除する ---

  # 取得URLを記録し、1ページ目以外には empty_body を返せるFakeフェッチャー（通信しない）。
  class RecordingFetcher
    def initialize(body:, empty_body: nil)
      @body = body
      @empty_body = empty_body
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      @empty_body && url.include?("?page=") ? @empty_body : @body
    end
  end

  def test_fetch_requests_each_target_and_page_and_deduplicates_urls
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    search_targets = [
      { skill_id: 22003, category_hint: "Ruby" },
      { skill_id: 22045, category_hint: "Ruby" }
    ]
    source = FreelanceJobs::Sources::EngineerFactory.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, max_pages: 2
    )

    postings = source.fetch

    assert_equal(
      [
        "#{LIST_URL_PREFIX}/22003",
        "#{LIST_URL_PREFIX}/22003?page=2",
        "#{LIST_URL_PREFIX}/22045",
        "#{LIST_URL_PREFIX}/22045?page=2"
      ],
      fetcher.requested_urls, "1ページ目はクエリ無し、2ページ目以降は ?page=<n> のはず"
    )
    assert_equal 40, postings.size, "同じURLの案件が4ページ分返っても重複排除され40件のままのはず"
  end

  def test_fetch_stops_paging_when_a_page_has_no_card
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME), empty_body: empty_list_body)
    source = FreelanceJobs::Sources::EngineerFactory.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ skill_id: 22064, category_hint: "React" }], max_pages: 3
    )

    postings = source.fetch

    assert_equal 2, fetcher.requested_urls.size, "2ページ目が0件なら3ページ目は取得しないはず"
    assert_equal 40, postings.size
  end

  # 既定値で1回のバッチが発行するリクエスト数はリクエスト予算40回に収まる。
  def test_default_configuration_stays_within_request_budget
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    FreelanceJobs::Sources::EngineerFactory.new(fetcher: fetcher, today: TODAY).fetch

    assert_equal 6, fetcher.requested_urls.size, "3スキル×2ページ＝6リクエストのはず"
    assert_operator fetcher.requested_urls.size, :<=, 40
  end

  # --- ページ単位の取得失敗に耐える ---

  # URLごとの応答を台本化するFakeフェッチャー（通信しない）。
  # 各URLの配列を先頭から1回ずつ消費し、例外なら送出・文字列ならbodyとして返す。
  # 台本に無いURL・使い切ったURLはカード0件のページを返す（＝ページ送りの終端）。
  class ScriptedFetcher
    def initialize(script, fallback_body:)
      @script = script
      @fallback_body = fallback_body
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      response = @script[url]&.shift || @fallback_body
      raise response if response.is_a?(StandardError)

      response
    end
  end

  def test_fetch_retries_once_when_a_page_returns_server_error
    fetcher = ScriptedFetcher.new(
      { "#{LIST_URL_PREFIX}/22003" => [server_error, read_fixture(FIXTURE_NAME)] },
      fallback_body: empty_list_body
    )
    postings = silencing_fetch_logs { build_source(fetcher, [22003]).fetch }

    assert_equal 40, postings.size, "1回目が500でも再取得すれば取得できるはず"
    assert_equal(
      ["#{LIST_URL_PREFIX}/22003", "#{LIST_URL_PREFIX}/22003", "#{LIST_URL_PREFIX}/22003?page=2"],
      fetcher.requested_urls
    )
  end

  # HTTPエラーだけでなく通信層の切断・タイムアウトも同じく1スキルの打ち切りで済ませる。
  def test_fetch_skips_failing_skill_and_keeps_other_skills
    connection_error = Errno::ECONNRESET.new("Connection reset by peer")
    fetcher = ScriptedFetcher.new(
      {
        "#{LIST_URL_PREFIX}/22013" => [connection_error, connection_error],
        "#{LIST_URL_PREFIX}/22003" => [read_fixture(FIXTURE_NAME)]
      },
      fallback_body: empty_list_body
    )
    postings = silencing_fetch_logs { build_source(fetcher, [22013, 22003]).fetch }

    assert_equal 40, postings.size, "TypeScriptが落ちてもRubyの結果は返すはず"
    refute_includes fetcher.requested_urls, "#{LIST_URL_PREFIX}/22013?page=2",
                    "2回とも失敗したスキルはページ送りを打ち切るはず"
  end

  # 全滅を黙って0件で返すとサイト構造の崩れに気付けないため、最初の失敗を送出する。
  def test_fetch_raises_when_no_page_succeeds
    error = server_error
    fetcher = ScriptedFetcher.new({ "#{LIST_URL_PREFIX}/22003" => [error, error] }, fallback_body: empty_list_body)

    raised = assert_raises(FreelanceJobs::FetchError) do
      silencing_fetch_logs { build_source(fetcher, [22003]).fetch }
    end

    assert_equal error.message, raised.message
  end

  # WAFのアクセス制限は取り直しても解消しないので、再取得せずそのまま送出する。
  def test_fetch_does_not_retry_when_access_is_blocked
    fetcher = ScriptedFetcher.new(
      { "#{LIST_URL_PREFIX}/22003" => [FreelanceJobs::AccessBlockedError.new("アクセス制限（WAF captcha）")] },
      fallback_body: empty_list_body
    )

    assert_raises(FreelanceJobs::AccessBlockedError) do
      silencing_fetch_logs { build_source(fetcher, [22003]).fetch }
    end
    assert_equal 1, fetcher.requested_urls.size, "アクセス制限では再取得しないはず"
  end

  def test_default_search_targets_cover_three_skills
    targets = FreelanceJobs::Sources::EngineerFactory::DEFAULT_SEARCH_TARGETS

    assert_equal [22003, 22013, 22064], targets.map { |target| target[:skill_id] }
    assert_equal %w[Ruby TypeScript React], targets.map { |target| target[:category_hint] }
    assert_equal 2, FreelanceJobs::Sources::EngineerFactory::MAX_PAGES
    assert_equal 1.5, FreelanceJobs::Sources::EngineerFactory::REQUEST_INTERVAL
  end

  # --- Profile::ENGINEER にEngineerFactoryが含まれる（BEGINNERには含まれない） ---

  def test_engineer_profile_includes_engineer_factory_source
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_includes source_classes, FreelanceJobs::Sources::EngineerFactory
  end

  def test_beginner_profile_does_not_include_engineer_factory_source
    source_classes = FreelanceJobs::Profile::BEGINNER.source_specs.map(&:first)

    refute_includes source_classes, FreelanceJobs::Sources::EngineerFactory
  end

  private

  def build_source(fetcher, skill_ids, max_pages: 2)
    search_targets = skill_ids.map { |skill_id| { skill_id: skill_id, category_hint: "Ruby" } }
    FreelanceJobs::Sources::EngineerFactory.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, max_pages: max_pages
    )
  end

  def server_error
    FreelanceJobs::FetchError.new("HTTP 500 #{LIST_URL_PREFIX}/22003")
  end

  # 最終ページを超えた page は HTTP 200 でカード0件・本文「ありません」になる。
  def empty_list_body
    wrap_html("<h1>Rubyのフリーランス求人・案件一覧</h1><p>該当する案件はありません</p>")
  end

  # 取得失敗の警告ログでテスト出力が汚れるのを防ぐ。FreelanceJobs.loggerはメモ化された
  # インスタンス変数で差し替え口が無いため、テスト中だけ直接入れ替えて必ず戻す。
  def silencing_fetch_logs
    original_logger = FreelanceJobs.logger
    FreelanceJobs.instance_variable_set(:@logger, Logger.new(File::NULL))
    yield
  ensure
    FreelanceJobs.instance_variable_set(:@logger, original_logger)
  end

  # エンジニアファクトリーのカード1件分のHTML片（実データのDOM構造を模したもの）。
  # 案件名の " | <span>東京都の案件・求人</span>" というSEO接尾辞も再現する。
  def build_card_html(job_id:, href: nil, title: "【リモート/Ruby】ECサイトのバックエンド開発",
                      wage_text: "月額 <strong>65</strong> ～ <strong>70</strong> 万円<span>(税抜)</span>",
                      areas: ["東京都"], occupations: ["バックエンドエンジニア"], skills: ["Ruby", "Ruby on Rails"],
                      industries: ["インターネット・通信"], required_skills: "・Ruby経験3年以上・AWS経験",
                      tags: ["リモート可"], badges: ["New"])
    link_href = href || "/freelance/jobs/#{job_id}"
    title_html = title.empty? ? "" : "#{title} | "
    badge_html = badges.map { |badge| %(<span class="tag-job bg-red-500">#{badge}</span>) }.join
    tag_html = tags.map { |tag| %(<li class="modListTag__item"><a href="/freelance/jobs/fastidiousness/30000">#{tag}</a></li>) }.join
    wage_html = wage_text.nil? ? "" : build_definition_list_html("単価", "<div>#{wage_text}</div>")

    <<~HTML
      <section class="modJobBlock modJobBlock--large">
        <div class="modJobBlock__inner">
          <div class="flex gap-1">#{badge_html}</div>
          <h3 class="modJobBlock__title"><a href="#{link_href}" target="_blank">#{title_html}<span><span>東京都</span><span>の案件・求人</span></span></a></h3>
          <ul class="modListTag">#{tag_html}</ul>
          <div class="space-y-2">
            #{wage_html}
            #{build_link_definition_list_html("エリア", "area", areas)}
            #{build_link_definition_list_html("職種", "occupation", occupations)}
            #{build_link_definition_list_html("スキル", "skill", skills)}
            #{industries.empty? ? "" : build_link_definition_list_html("業界", "industry", industries)}
          </div>
          <dl>
            <dt class="font-bold">必須スキル</dt>
            <dd class="flex flex-col gap-2"><p class="line-clamp-3">#{required_skills}</p></dd>
          </dl>
          <div class="flex justify-center"><a href="#{link_href}" class="modBtnSearch">詳細を見る</a></div>
        </div>
      </section>
    HTML
  end

  def build_definition_list_html(label, dd_inner_html)
    <<~HTML
      <dl class="grid">
        <dt class="font-bold flex items-center gap-2 text-sm"><span class="iconify" aria-hidden="true"></span> #{label} </dt>
        <dd class="text-sm">#{dd_inner_html}</dd>
      </dl>
    HTML
  end

  def build_link_definition_list_html(label, path_segment, names)
    links = names.each_with_index.map { |name, index| %(<a href="/freelance/jobs/#{path_segment}/#{index + 1}">#{name}</a>) }.join
    build_definition_list_html(label, links)
  end

  # __NUXT_DATA__（devalue形式）を模したscript要素。Hashの値は同じ配列へのインデックス参照で、
  # 実データと同じく ShallowReactive/Reactive のラッパー越しに案件オブジェクトへ辿り着く形にする。
  def build_nuxt_data_html(job_id:, publication_date:, duties: "業務内容です", preferred_skills: "", nearest_stations: "")
    entries = [
      ["ShallowReactive", 1],                                        # 0: root
      { "data" => 2 },                                                # 1
      ["Reactive", 3],                                                # 2
      { "fetch-key" => 4 },                                           # 3
      { "data" => 5 },                                                # 4
      [6],                                                            # 5: 案件配列
      { "id" => 7, "type" => 8, "attributes" => 9, "relationships" => 14 }, # 6: 案件オブジェクト
      job_id,                                                         # 7
      "freelance_job",                                                # 8
      { "publication_date_ef" => 10, "duties" => 11, "skills_preferred" => 12, "nearest_stations" => 13, "wage_min" => 15 }, # 9
      publication_date,                                               # 10
      duties,                                                         # 11
      preferred_skills,                                               # 12
      nearest_stations,                                               # 13
      {},                                                             # 14
      65                                                              # 15
    ]
    %(<script id="__NUXT_DATA__" type="application/json">#{JSON.generate(entries)}</script>)
  end
end
