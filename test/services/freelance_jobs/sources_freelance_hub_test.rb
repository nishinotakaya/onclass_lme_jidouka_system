# frozen_string_literal: true
# test/services/freelance_jobs/sources_freelance_hub_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesFreelanceHubTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 13)
  FIXTURE_NAME = "freelance_hub_ruby.html"
  LIST_URL_PREFIX = "https://freelance-hub.jp/project/skill"

  def parse_fixture(category_hint: "Ruby")
    FreelanceJobs::Sources::FreelanceHub.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  # 一覧は40件/頁（PC版カードのみSSRされ、SP版カードは含まれない）。
  def test_parse_fixture_returns_forty_postings
    assert_equal 40, parse_fixture.size
  end

  # --- 1件目の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "フリーランスHub", first.site
    assert_equal "https://freelance-hub.jp/project/detail/459935", first.url
    assert_equal "【Ruby】Rubyを用いたWebシステムの開発支援", first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "650,000円／月", first.reward
    assert_equal "月額制（業務委託）", first.work_format
    assert_equal ["Ruby", "Rails", "React"], first.skills
    assert_equal ["NEW"], first.tags
    assert_equal "Midworks", first.client
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_equal Date.new(2026, 9, 11), first.posted_on, "「2日前・」は today - 2 になるはず"
  end

  # descriptionは「作業内容 / 募集職種 / 使用技術 / 勤務地 / リモート等タグ」の順に連結する。
  # 先頭の見出し語「作業内容」は剥がして "作業内容: …" に組み立て直す。
  def test_parse_first_posting_description_joins_detail_occupation_skills_and_location
    description = parse_fixture.first.description

    assert_equal(
      "作業内容: 【案件概要】 Rubyを用いたシステム開発案件に参画していただくポジションです。 " \
      "スキルやご経験に応じて、複数の開発プロジェクトの中から適した案件をご担当いただきます。 " \
      "ECパッケージ製品やサブスクリプションサービスサイトなど、多様なWebサービス開発に携わることができます。 " \
      "設計から開発、テストまで幅広い工程で活躍いただける環境です。 " \
      "【作業内容】 ・Rubyを用いたWebシステムの開発 ・ECパッケージ製品の機能開発および改修 " \
      "・サブスクリプションサービスサイトの開発対応 ・設計書作成およびプログラム実装対応 ・テスト実施およびリリース支援対応 / " \
      "募集職種: フロントエンドエンジニア / 使用技術: Ruby / Rails / React / 勤務地: 東京都 東京駅",
      description
    )
    refute_match(/\A作業内容 作業内容/, description, "見出し語「作業内容」が二重になってはいけない")
  end

  # --- 状態タグ・HotTags・提供元が別案件でも取れる ---

  def test_parse_posting_with_status_and_hot_tags
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/459017") }

    refute_nil posting
    assert_equal "【Ruby/Python】建設業向け自社サービス開発案件", posting.title
    assert_equal ["Python", "Ruby", "Rails"], posting.skills
    assert_equal ["NEW", "注目", "フルリモート", "オンライン商談OK"], posting.tags
    assert_equal "レバテックフリーランス", posting.client
    assert_equal Date.new(2026, 9, 8), posting.posted_on
    assert_includes posting.description, "募集職種: フロントエンドエンジニア / サーバーサイドエンジニア"
    assert_includes posting.description, "勤務地: 福岡県 博多駅 / フルリモート・オンライン商談OK"
  end

  # 幅のある単価は "800,000 〜 900,000円／月" の形で残す（high_reward? は先頭の数値を読む）。
  def test_parse_posting_with_reward_range
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/459268") }

    refute_nil posting
    assert_equal "800,000 〜 900,000円／月", posting.reward
    assert_equal "月額制（業務委託）", posting.work_format
  end

  # 勤務地・最寄駅が無いカードでも落とさず、descriptionに「勤務地:」を入れないだけにする。
  def test_parse_posting_without_location_omits_location_part
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/458473") }

    refute_nil posting
    refute_includes posting.description, "勤務地:"
  end

  # --- 全件で必須フィールドが埋まる（"-"や""は許すがnilは許さない） ---

  def test_every_posting_fills_display_fields
    parse_fixture.each do |posting|
      refute_empty posting.title, "案件名が空のカードは除外されるはず"
      refute_empty posting.reward
      refute_empty posting.work_format
      refute_empty posting.client, "提供元はフィクスチャ全件に存在する"
      assert_instance_of Array, posting.skills
      assert_instance_of Array, posting.tags
      refute_nil posting.posted_on, "掲載日はフィクスチャ全件に存在する"
      assert_match(/\A作業内容: /, posting.description)
    end
  end

  # --- URL正規化 ---

  def test_urls_are_normalized_absolute_without_trailing_slash_or_query
    parse_fixture.each do |posting|
      assert_match %r{\Ahttps://freelance-hub\.jp/project/detail/\d+\z}, posting.url,
                   "末尾スラッシュなし・クエリなしの正規化された絶対URLのはず"
    end
  end

  # robots.txt で禁止されている /project/search/ 配下のURLを案件URLにしてはいけない。
  def test_urls_never_point_under_forbidden_search_path
    parse_fixture.each do |posting|
      refute_includes posting.url, "/project/search/"
    end
  end

  # --- category_hint が引数どおり全件に伝わる ---

  def test_category_hint_is_propagated_to_every_posting
    postings = parse_fixture(category_hint: "TypeScript")

    assert(postings.all? { |posting| posting.category_hint == "TypeScript" },
           "全件のcategory_hintが引数のTypeScriptになるはず")
  end

  # --- 単価の単位で work_format が分岐する ---

  def test_work_format_is_hourly_when_reward_is_per_hour
    fragment = build_card_html(project_id: 1, title: "時間単価の案件", money: "<strong>7,680</strong>円/時")
    posting = parse_fragment(fragment).first

    assert_equal "7,680円／時", posting.reward
    assert_equal "時間単価制", posting.work_format
  end

  def test_work_format_falls_back_when_reward_is_missing
    fragment = build_card_html(project_id: 2, title: "単価非公開の案件", money: "")
    posting = parse_fragment(fragment).first

    assert_equal "要確認", posting.reward
    assert_equal "業務委託（フリーランス）", posting.work_format
  end

  # --- 掲載日の相対表記 ---

  def test_posted_on_subtracts_months_when_text_is_months_ago
    fragment = build_card_html(project_id: 3, title: "数ヶ月前の案件", posted_text: "2ヶ月前・")
    posting = parse_fragment(fragment).first

    assert_equal Date.new(2026, 7, 13), posting.posted_on
  end

  def test_posted_on_is_nil_when_date_format_is_unexpected
    fragment = build_card_html(project_id: 4, title: "掲載日が想定外", posted_text: "2026年9月8日")
    posting = parse_fragment(fragment).first

    assert_nil posting.posted_on
  end

  # --- 必須要素（案件ID・案件名）が欠けたカードは黙って除外する ---

  def test_parse_skips_card_without_numeric_id
    fragment = build_card_html(project_id: "abc", title: "IDが数値でないカード")

    assert_equal [], parse_fragment(fragment)
  end

  def test_parse_skips_card_without_id
    fragment = <<~HTML
      <div class="ProjectCard">
        <h3 class="ProjectCard_Title">idなし</h3>
      </div>
    HTML

    assert_equal [], parse_fragment(fragment)
  end

  def test_parse_skips_card_without_title
    fragment = <<~HTML
      <div class="ProjectCard" id="ProjectListPc_ProjectCard_5">
        <div class="ProjectCard_SummaryItem ProjectCard_SummaryItem--money"><p><strong>650,000</strong>円/月</p></div>
      </div>
    HTML

    assert_equal [], parse_fragment(fragment)
  end

  # フッター（掲載日・提供元）が無くても案件としては残し、client は空・posted_on は nil にする。
  def test_parse_keeps_card_without_footer
    fragment = build_card_html(project_id: 6, title: "フッターなし", footer: false)
    posting = parse_fragment(fragment).first

    refute_nil posting
    assert_equal "", posting.client
    assert_nil posting.posted_on
  end

  # --- fetch: search_targets × ページ数だけ取得し、URL重複を排除する ---

  # 取得URLを記録し、ページ番号に応じたbodyを返すFakeフェッチャー（通信しない）。
  class RecordingFetcher
    def initialize(body:, empty_body: nil)
      @body = body
      @empty_body = empty_body
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      @empty_body && !url.end_with?("page=1") ? @empty_body : @body
    end
  end

  def test_fetch_requests_each_target_and_page_and_deduplicates_urls
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    search_targets = [
      { skill_id: 8, category_hint: "Ruby" },
      { skill_id: 80, category_hint: "Ruby" }
    ]
    source = FreelanceJobs::Sources::FreelanceHub.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, max_pages: 2, excluded_providers: []
    )

    postings = source.fetch

    assert_equal 4, fetcher.requested_urls.size, "2スキル×2ページぶん取得するはず"
    assert_equal(
      [
        "#{LIST_URL_PREFIX}/8/?order=created_at&page=1",
        "#{LIST_URL_PREFIX}/8/?order=created_at&page=2",
        "#{LIST_URL_PREFIX}/80/?order=created_at&page=1",
        "#{LIST_URL_PREFIX}/80/?order=created_at&page=2"
      ],
      fetcher.requested_urls
    )
    assert_equal 40, postings.size, "同じURLの案件が4ページ分返っても重複排除され40件のままのはず"
  end

  def test_fetch_stops_paging_when_a_page_has_no_card
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME), empty_body: empty_list_body)
    source = build_source(fetcher, [8], max_pages: 3, excluded_providers: [])

    postings = source.fetch

    assert_equal 2, fetcher.requested_urls.size, "2ページ目が0件なら3ページ目は取得しないはず"
    assert_equal 40, postings.size
  end

  # --- excluded_providers: 既存取得元と重複する提供元のカードだけを捨てる ---

  # フィクスチャの提供元分布: Midworks 17 / Findy Freelance 8 / フリコン 7 / レバテックフリーランス 5 /
  # ココナラテック（旧：フリエン/furien） 2 / mijicaフリーランス 1。既定の除外で 15 件落ち 25 件残る。
  def test_fetch_excludes_default_providers_and_keeps_others
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    source = build_source(fetcher, [8], max_pages: 1)

    postings = source.fetch
    remaining_clients = postings.map(&:client).uniq

    assert_equal 25, postings.size
    assert_equal ["Midworks", "フリコン", "mijicaフリーランス"].sort, remaining_clients.sort
    FreelanceJobs::Sources::FreelanceHub::DEFAULT_EXCLUDED_PROVIDERS.each do |provider|
      refute_includes remaining_clients, provider
    end
  end

  def test_fetch_excludes_only_given_providers
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    source = build_source(fetcher, [8], max_pages: 1, excluded_providers: ["Midworks"])

    postings = source.fetch

    assert_equal 23, postings.size, "Midworks の17件だけが落ちるはず"
    refute_includes postings.map(&:client), "Midworks"
    assert_includes postings.map(&:client), "レバテックフリーランス"
  end

  # 提供元は完全一致で判定する（部分一致で "レバテッククリエイター" まで巻き込まない）。
  def test_excluded_providers_match_exactly
    fragment = build_card_html(project_id: 7, title: "別エージェント", provider: "レバテックフリーランス株式会社")
    fetcher = RecordingFetcher.new(body: wrap_html(fragment))
    source = build_source(fetcher, [8], max_pages: 1, excluded_providers: ["レバテックフリーランス"])

    assert_equal 1, source.fetch.size
  end

  # 除外対象ばかりのページでページ送りが止まってはいけない（終端判定は除外前のカード数で行う）。
  def test_fetch_keeps_paging_when_a_page_has_only_excluded_providers
    excluded_page = wrap_html(build_card_html(project_id: 8, title: "除外対象だけ", provider: "レバテックフリーランス"))
    fetcher = ScriptedFetcher.new(
      { "#{LIST_URL_PREFIX}/8/?order=created_at&page=1" => [excluded_page] },
      fallback_body: read_fixture(FIXTURE_NAME)
    )
    source = build_source(fetcher, [8], max_pages: 2)

    postings = source.fetch

    assert_equal 2, fetcher.requested_urls.size, "1ページ目が全件除外でも2ページ目を取得するはず"
    assert_equal 25, postings.size
  end

  # --- ページ単位の取得失敗に耐える ---

  # URLごとの応答を台本化するFakeフェッチャー（通信しない）。
  # 各URLの配列を先頭から1回ずつ消費し、例外なら送出・文字列ならbodyとして返す。
  # 台本に無いURL・使い切ったURLは fallback_body を返す。
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
      { "#{LIST_URL_PREFIX}/8/?order=created_at&page=1" => [server_error, read_fixture(FIXTURE_NAME)] },
      fallback_body: empty_list_body
    )
    postings = silencing_fetch_logs { build_source(fetcher, [8], excluded_providers: []).fetch }

    assert_equal 40, postings.size, "1回目が500でも再取得すれば取得できるはず"
    assert_equal(
      [
        "#{LIST_URL_PREFIX}/8/?order=created_at&page=1",
        "#{LIST_URL_PREFIX}/8/?order=created_at&page=1",
        "#{LIST_URL_PREFIX}/8/?order=created_at&page=2"
      ],
      fetcher.requested_urls
    )
  end

  # HTTPエラーだけでなく通信層の切断・タイムアウトも同じく1スキルの打ち切りで済ませる。
  def test_fetch_skips_failing_skill_and_keeps_other_skills
    connection_error = Errno::ECONNRESET.new("Connection reset by peer")
    fetcher = ScriptedFetcher.new(
      {
        "#{LIST_URL_PREFIX}/409/?order=created_at&page=1" => [connection_error, connection_error],
        "#{LIST_URL_PREFIX}/8/?order=created_at&page=1" => [read_fixture(FIXTURE_NAME)]
      },
      fallback_body: empty_list_body
    )
    postings = silencing_fetch_logs { build_source(fetcher, [409, 8], excluded_providers: []).fetch }

    assert_equal 40, postings.size, "TypeScriptが落ちてもRubyの結果は返すはず"
    refute_includes fetcher.requested_urls, "#{LIST_URL_PREFIX}/409/?order=created_at&page=2",
                    "2回とも失敗したスキルはページ送りを打ち切るはず"
  end

  # 全滅を黙って0件で返すとサイト構造の崩れに気付けないため、最初の失敗を送出する。
  def test_fetch_raises_when_no_page_succeeds
    error = server_error
    fetcher = ScriptedFetcher.new(
      { "#{LIST_URL_PREFIX}/8/?order=created_at&page=1" => [error, error] },
      fallback_body: empty_list_body
    )

    raised = assert_raises(FreelanceJobs::FetchError) do
      silencing_fetch_logs { build_source(fetcher, [8]).fetch }
    end

    assert_equal error.message, raised.message
  end

  # WAFのアクセス制限は取り直しても解消しないので、再取得せずそのまま送出する。
  def test_fetch_does_not_retry_when_access_is_blocked
    fetcher = ScriptedFetcher.new(
      { "#{LIST_URL_PREFIX}/8/?order=created_at&page=1" => [FreelanceJobs::AccessBlockedError.new("アクセス制限（WAF captcha）")] },
      fallback_body: empty_list_body
    )

    assert_raises(FreelanceJobs::AccessBlockedError) do
      silencing_fetch_logs { build_source(fetcher, [8]).fetch }
    end
    assert_equal 1, fetcher.requested_urls.size, "アクセス制限では再取得しないはず"
  end

  # --- 既定値: 3スキル×3頁=9リクエストでリクエスト予算40回に収まる ---

  def test_default_search_targets_and_pages_stay_within_request_budget
    targets = FreelanceJobs::Sources::FreelanceHub::DEFAULT_SEARCH_TARGETS

    assert_equal [8, 409, 359], targets.map { |target| target[:skill_id] }
    assert_equal %w[Ruby TypeScript React], targets.map { |target| target[:category_hint] }
    assert_equal 3, FreelanceJobs::Sources::FreelanceHub::MAX_PAGES
    assert_operator targets.size * FreelanceJobs::Sources::FreelanceHub::MAX_PAGES, :<=, 40
  end

  def test_default_excluded_providers_cover_existing_sources
    assert_equal(
      [
        "レバテックフリーランス",
        "レバテッククリエイター",
        "ココナラテック（旧：フリエン/furien）",
        "ビズリンク",
        "HiPro Tech（ハイプロテック）",
        "Findy Freelance",
        "フォスターフリーランス"
      ],
      FreelanceJobs::Sources::FreelanceHub::DEFAULT_EXCLUDED_PROVIDERS
    )
  end

  # --- Profile::ENGINEER にFreelanceHubが含まれる（BEGINNERには含まれない） ---

  def test_engineer_profile_includes_freelance_hub_source
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_includes source_classes, FreelanceJobs::Sources::FreelanceHub
  end

  def test_beginner_profile_does_not_include_freelance_hub_source
    source_classes = FreelanceJobs::Profile::BEGINNER.source_specs.map(&:first)

    refute_includes source_classes, FreelanceJobs::Sources::FreelanceHub
  end

  private

  def parse_fragment(fragment)
    FreelanceJobs::Sources::FreelanceHub.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  def build_source(fetcher, skill_ids, max_pages: 2,
                   excluded_providers: FreelanceJobs::Sources::FreelanceHub::DEFAULT_EXCLUDED_PROVIDERS)
    search_targets = skill_ids.map { |skill_id| { skill_id: skill_id, category_hint: "Ruby" } }
    FreelanceJobs::Sources::FreelanceHub.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, max_pages: max_pages,
      excluded_providers: excluded_providers
    )
  end

  def server_error
    FreelanceJobs::FetchError.new("HTTP 500 #{LIST_URL_PREFIX}/8/?order=created_at&page=1")
  end

  def empty_list_body
    wrap_html("<div>該当なし</div>")
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

  # フリーランスHubのカード1件分のHTML片（実データのDOM構造を模したもの）。
  # id は "ProjectListPc_ProjectCard_<数値>" 形式で、案件URLはここから組み立てる。
  def build_card_html(project_id:, title:, money: "<strong>650,000</strong>円/月", posted_text: "2日前・",
                      provider: "Midworks", footer: true, skills: ["Ruby"], hot_tags: [])
    skill_html = skills.map { |skill| %(<span class="TagLink TagLink_TextTag">#{skill}</span>) }.join
    hot_tag_html = hot_tags.map { |tag| %(<a class="ProjectCard_HotTags" href="/project/search/?keyword=#{tag}">#{tag}</a>) }.join
    footer_html = if footer
                    %(<p class="ProjectCard__Footer__Info"><span>#{posted_text} </span><span>提供元: #{provider}</span></p>)
                  else
                    ""
                  end

    <<~HTML
      <div class="ProjectCard" id="ProjectListPc_ProjectCard_#{project_id}">
        <ul class="ProjectCard__Status__List">
          <li class="ProjectCard__Status__Item"><span class="TagText TagText_New">NEW</span></li>
        </ul>
        <h3 class="ProjectCard_Title">#{title}</h3>
        <div class="ProjectCard_Summary">
          <div class="ProjectCard_SummaryItem ProjectCard_SummaryItem--money"><p>#{money}</p></div>
          <div class="ProjectCard_SummaryCell">
            <div class="ProjectCard_SummaryItem ProjectCard_SummaryItem--location"><span>東京都</span></div>
            <div class="ProjectCard_SummaryItem ProjectCard_SummaryItem--station"><span>東京駅 </span></div>
          </div>
          <div class="ProjectCard_SummaryItem ProjectCard_SummaryItem--skill">#{skill_html}</div>
        </div>
        <div class="ProjectCard_Tags">#{hot_tag_html}</div>
        <div class="ProjectCard_DetailText"> 作業内容 テスト用の作業内容です。</div>
        <div class="ProjectCard__Footer">#{footer_html}</div>
      </div>
    HTML
  end
end
