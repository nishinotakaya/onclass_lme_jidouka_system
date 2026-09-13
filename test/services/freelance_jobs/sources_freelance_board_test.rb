# frozen_string_literal: true
# test/services/freelance_jobs/sources_freelance_board_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"
require "json"

class FreelanceJobsSourcesFreelanceBoardTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 13)
  RUBY_FIXTURE_NAME = "freelance_board_ruby.html"
  TYPESCRIPT_FIXTURE_NAME = "freelance_board_typescript.html"
  LIST_URL = "https://freelance-board.com/jobs?keyword="

  def parse_fixture(fixture_name = RUBY_FIXTURE_NAME, category_hint: "Ruby")
    FreelanceJobs::Sources::FreelanceBoard.parse(read_fixture(fixture_name), today: TODAY, category_hint: category_hint)
  end

  # 一覧は30件/頁（__NUXT_DATA__ の limit_value=30）。parse は提供元による除外を行わないので30件そのまま。
  def test_parse_fixture_returns_thirty_postings
    assert_equal 30, parse_fixture.size
    assert_equal 30, parse_fixture(TYPESCRIPT_FIXTURE_NAME, category_hint: "TypeScript").size
  end

  # --- 1件目の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "フリーランスボード", first.site
    assert_equal "https://freelance-board.com/jobs/detail/749530", first.url
    assert_equal "Ruby on Rails バックエンドエンジニア｜電子コミックサービスの決済機能基盤開発", first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "800,000円／月", first.reward
    assert_equal "月額制（業務委託）", first.work_format
    assert_equal ["Ruby", "Ruby on Rails"], first.skills
    assert_equal ["フルリモート"], first.tags
    assert_equal "FLEXY", first.client
    assert_equal "-", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_equal Date.new(2026, 9, 12), first.posted_on
  end

  # descriptionは「概要 / 業務内容 / 必須スキル / (歓迎スキル) / 使用技術 / 募集職種 / 勤務地 / 勤務形態 / 稼働」の順。
  # 概要を先頭に置くのはRowBuilderが先頭160字を要約列に使うため。detail 内の改行は空白に畳まれる。
  def test_parse_first_posting_description_joins_sections_in_expected_order
    description = parse_fixture.first.description

    assert_equal(
      "概要: Ruby on Railsを活用し、電子コンテンツ配信サービスの決済機能分離と独立システム化を推進するエンジニアを募集します。" \
      "要件定義から設計、実装、テスト、運用保守まで一連の基盤開発業務を担当し、フルリモートで参画可能です。 / " \
      "業務内容: 電子コンテンツ配信サービスにおける決済機能の分離および独立システム化を担当します。 " \
      "要件定義、基本設計、詳細設計、実装、テスト、運用保守まで一連の業務を担当します。 ■募集背景 人員不足のため。 / " \
      "必須スキル: ・Ruby on Railsを使用したチーム開発経験（3年以上） / " \
      "使用技術: Ruby / Ruby on Rails / 募集職種: バックエンドエンジニア・サーバーサイドエンジニア / " \
      "勤務地: 東京都 池袋駅 / 勤務形態: フルリモート / 稼働: 週5日",
      description
    )
  end

  # --- エージェント定型の注意書き（ROSCA freelance の「====」ブロック）は description に載せない ---

  # 実データの ROSCA freelance 案件は detail 末尾に「※必ずお読みください※ … 直接スカウトを送信 …」の
  # 定型文を付けており、載せたままだと EngineerClassifier の NON_DEV_RE（スカウト）で全件が分類対象外になる。
  def test_parse_strips_agent_notice_block_from_rosca_postings
    rosca_postings = parse_fixture.select { |posting| posting.client == "ROSCA freelance" }

    assert_equal 13, rosca_postings.size
    rosca_postings.each do |posting|
      refute_includes posting.description, "※必ずお読みください※"
      refute_includes posting.description, "スカウト"
      refute_includes posting.description, "========"
      assert_includes posting.description, "業務内容:"
      refute_nil FreelanceJobs::EngineerClassifier.classify(posting, today: TODAY).category,
                 "定型文を落とせば開発案件として分類されるはず: #{posting.title}"
    end
  end

  def test_parse_keeps_text_around_agent_notice_block
    detail = "決済基盤の開発を担当。\n" \
             "========================\n※必ずお読みください※\n弊社より直接スカウトを送信させていただきます。\n" \
             "========================\n■備考\n週5日稼働"
    posting = parse_job_record(build_job_record("detail" => detail)).first

    assert_includes posting.description, "業務内容: 決済基盤の開発を担当。 ■備考 週5日稼働 /"
    refute_includes posting.description, "スカウト"
  end

  # 閉じの「====」行が無い（末尾まで注意書き）場合も落とす。
  def test_parse_strips_agent_notice_block_without_closing_line
    detail = "決済基盤の開発を担当。\n========================\n※必ずお読みください※\n直接スカウトを送信します。"
    posting = parse_job_record(build_job_record("detail" => detail)).first

    assert_includes posting.description, "業務内容: 決済基盤の開発を担当。 /"
    refute_includes posting.description, "スカウト"
  end

  # --- レンジ単価・スキル無し・生成AIタグが別案件で取れる ---

  def test_parse_posting_with_reward_range
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/748917") }

    refute_nil posting
    assert_equal "1,050,000〜1,200,000円／月", posting.reward, "DOM表記「105-120万円/月額」をJSONの整数から組み立てるはず"
    assert_equal "月額制（業務委託）", posting.work_format
  end

  def test_parse_posting_without_skill_ids_has_empty_skills_and_generation_ai_tag
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/748251") }

    refute_nil posting
    assert_equal [], posting.skills
    assert_equal "Bizlink", posting.client
    assert_equal ["一部リモート可", "生成AI活用案件"], posting.tags
    refute_includes posting.description, "使用技術:"
  end

  # skill_ids の順序で名前を引く（key_values のHash順ではない）。
  def test_parse_posting_keeps_skill_order_from_skill_ids
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/749526") }

    refute_nil posting
    assert_equal ["Ruby", "Ruby on Rails", "React", "Vue.js", "RSpec", "ファイヤーウォール"], posting.skills
    assert_equal "780,000円／月", posting.reward
  end

  # --- 全件で必須フィールドが埋まる（"-"や""は許すがnilは許さない） ---

  def test_every_posting_fills_display_fields
    parse_fixture.each do |posting|
      refute_empty posting.title, "案件名が空の案件は除外されるはず"
      refute_empty posting.reward
      refute_empty posting.work_format
      refute_empty posting.client, "提供元ラベルはフィクスチャ全件に存在する"
      assert_instance_of Array, posting.skills
      assert_instance_of Array, posting.tags
      refute_nil posting.posted_on, "掲載日はフィクスチャ全件に存在する"
      assert_includes posting.description, "業務内容:"
    end
  end

  # --- URL正規化 ---

  def test_urls_are_normalized_absolute_without_trailing_slash_or_query
    parse_fixture.each do |posting|
      assert_match %r{\Ahttps://freelance-board\.com/jobs/detail/\d+\z}, posting.url,
                   "末尾スラッシュなし・クエリなしの正規化された絶対URLのはず"
    end
  end

  # --- category_hint が引数どおり全件に伝わる ---

  def test_category_hint_is_propagated_to_every_posting
    postings = parse_fixture(TYPESCRIPT_FIXTURE_NAME, category_hint: "TypeScript")

    assert(postings.all? { |posting| posting.category_hint == "TypeScript" },
           "全件のcategory_hintが引数のTypeScriptになるはず")
  end

  # --- 提供元ラベルの集計（フィクスチャの実データ） ---

  def test_ruby_fixture_provider_labels
    provider_counts = count_by(parse_fixture.map(&:client))

    assert_equal(
      { "FLEXY" => 6, "ROSCA freelance" => 13, "Findy Freelance" => 5, "Midworks" => 3, "Bizlink" => 2, "TechReach" => 1 },
      provider_counts
    )
  end

  # --- 単価の単位で work_format が分岐する（実データは全件月額なので合成レコードで確かめる） ---

  def test_work_format_is_hourly_when_only_hourly_payment_exists
    posting = parse_job_record(build_job_record(
      "monthly_payment_f_num" => nil, "monthly_payment_l_num" => nil,
      "hourly_payment_f_num" => 7680, "hourly_payment_l_num" => 7680
    )).first

    assert_equal "7,680円／時", posting.reward
    assert_equal "時間単価制", posting.work_format
  end

  def test_work_format_falls_back_when_payment_is_missing
    posting = parse_job_record(build_job_record(
      "monthly_payment_f_num" => nil, "monthly_payment_l_num" => nil,
      "hourly_payment_f_num" => nil, "hourly_payment_l_num" => nil
    )).first

    assert_equal "要確認", posting.reward
    assert_equal "業務委託（フリーランス）", posting.work_format
  end

  # 下限だけ入っている案件は1要素の表記にする。
  def test_reward_uses_single_amount_when_only_lower_bound_exists
    posting = parse_job_record(build_job_record("monthly_payment_l_num" => nil)).first

    assert_equal "800,000円／月", posting.reward
  end

  # --- display_title が空なら name（エージェント側の原題）にフォールバックする ---

  def test_title_falls_back_to_name_when_display_title_is_blank
    posting = parse_job_record(build_job_record("display_title" => "")).first

    assert_equal "【Ruby on Rails】原題の案件名", posting.title
  end

  # --- 想定外の掲載日表記は posted_on を nil にする ---

  def test_posted_on_is_nil_when_date_format_is_unexpected
    posting = parse_job_record(build_job_record("first_published_at" => "不明")).first

    assert_nil posting.posted_on
  end

  # --- 必須要素（案件ID・案件名）が欠けた案件は黙って除外する ---

  def test_parse_skips_record_without_id
    assert_equal [], parse_job_record(build_job_record("id" => nil))
  end

  def test_parse_skips_record_without_title
    assert_equal [], parse_job_record(build_job_record("display_title" => "", "name" => ""))
  end

  # devalue の負のインデックス（undefined 等の特殊値）は nil として扱い、落とさない。
  def test_parse_treats_negative_reference_as_nil
    top_level_values = build_nuxt_top_level_values([build_job_record])
    job_index = top_level_values[JOB_INDEX_POSITION]
    first_job = top_level_values[top_level_values[job_index["jobs"]].first]
    first_job["ai_summary"] = -1

    posting = FreelanceJobs::Sources::FreelanceBoard.parse(wrap_nuxt_data(top_level_values), today: TODAY).first

    refute_nil posting
    refute_includes posting.description, "概要:"
    assert_includes posting.description, "業務内容:"
  end

  # --- 壊れたHTML/JSONは0件（例外にしない） ---

  def test_parse_returns_empty_when_nuxt_data_script_is_missing
    assert_equal [], FreelanceJobs::Sources::FreelanceBoard.parse(wrap_html("<div>該当なし</div>"), today: TODAY)
  end

  def test_parse_returns_empty_when_nuxt_data_is_broken_json
    body = wrap_html('<script type="application/json" id="__NUXT_DATA__">[{"jobs": </script>')

    assert_equal [], FreelanceJobs::Sources::FreelanceBoard.parse(body, today: TODAY)
  end

  def test_parse_returns_empty_when_job_index_is_missing
    body = wrap_nuxt_data([["Reactive", 1], { "data" => 2 }, { "other" => 3 }, "x"])

    assert_equal [], FreelanceJobs::Sources::FreelanceBoard.parse(body, today: TODAY)
  end

  # --- fetch: search_targets × ページ数だけ取得し、URL重複を排除し、提供元で除外する ---

  # 取得URLを記録し、常に同じbodyを返すFakeフェッチャー（通信しない）。
  class RecordingFetcher
    def initialize(body:, empty_body: nil)
      @body = body
      @empty_body = empty_body
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      @empty_body && url.include?("&page=") ? @empty_body : @body
    end
  end

  def test_fetch_requests_each_target_and_page_and_deduplicates_urls
    fetcher = RecordingFetcher.new(body: read_fixture(RUBY_FIXTURE_NAME))
    search_targets = [{ keyword: "Ruby", hint: "Ruby" }, { keyword: "React", hint: "React" }]
    source = FreelanceJobs::Sources::FreelanceBoard.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, max_pages: 2, excluded_providers: []
    )

    postings = source.fetch

    assert_equal(
      [
        "#{LIST_URL}Ruby",
        "#{LIST_URL}Ruby&page=2",
        "#{LIST_URL}React",
        "#{LIST_URL}React&page=2"
      ],
      fetcher.requested_urls,
      "1ページ目は page パラメータ無し、2ページ目以降は &page=N のはず"
    )
    assert_equal 30, postings.size, "同じURLの案件が4ページ分返っても重複排除され30件のままのはず"
  end

  def test_fetch_stops_paging_when_a_page_has_no_job
    fetcher = RecordingFetcher.new(body: read_fixture(RUBY_FIXTURE_NAME), empty_body: wrap_html("<div>該当なし</div>"))
    source = FreelanceJobs::Sources::FreelanceBoard.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ keyword: "Ruby", hint: "Ruby" }], max_pages: 3,
      excluded_providers: []
    )

    postings = source.fetch

    assert_equal 2, fetcher.requested_urls.size, "2ページ目が0件なら3ページ目は取得しないはず"
    assert_equal 30, postings.size
  end

  # 既定の excluded_providers で、既存取得元と重複する提供元（Findy Freelance / Bizlink）が落ちる。
  def test_fetch_excludes_default_providers_and_keeps_others
    fetcher = RecordingFetcher.new(body: read_fixture(RUBY_FIXTURE_NAME))
    source = FreelanceJobs::Sources::FreelanceBoard.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ keyword: "Ruby", hint: "Ruby" }], max_pages: 1
    )

    postings = source.fetch

    assert_equal 23, postings.size, "30件中 Findy Freelance 5件 + Bizlink 2件 が除外されるはず"
    assert_equal(
      { "FLEXY" => 6, "ROSCA freelance" => 13, "Midworks" => 3, "TechReach" => 1 },
      count_by(postings.map(&:client))
    )
  end

  # 正式名ラベル「レバテックフリーランス」も除外される（TypeScriptフィクスチャに3件含まれる）。
  def test_fetch_excludes_levtech_label_from_typescript_fixture
    fetcher = RecordingFetcher.new(body: read_fixture(TYPESCRIPT_FIXTURE_NAME))
    source = FreelanceJobs::Sources::FreelanceBoard.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ keyword: "TypeScript", hint: "TypeScript" }], max_pages: 1
    )

    postings = source.fetch

    assert_equal 21, postings.size, "30件中 Findy Freelance 6件 + レバテックフリーランス 3件 が除外されるはず"
    refute_includes postings.map(&:client), "レバテックフリーランス"
    refute_includes postings.map(&:client), "Findy Freelance"
    assert_includes postings.map(&:client), "ギークスジョブ"
  end

  # 提供元の比較は大小文字・空白のゆれを吸収する（"TECH STOCK" 指定でも "TechStock" が落ちる）。
  def test_fetch_excluded_providers_comparison_ignores_case_and_spaces
    body = wrap_nuxt_data(build_nuxt_top_level_values([
      build_job_record("id" => 1, "service_name" => "TechStock"),
      build_job_record("id" => 2, "service_name" => "FLEXY")
    ]))
    fetcher = RecordingFetcher.new(body: body)
    source = FreelanceJobs::Sources::FreelanceBoard.new(
      fetcher: fetcher, today: TODAY, search_targets: [{ keyword: "Ruby", hint: "Ruby" }], max_pages: 1,
      excluded_providers: ["TECH STOCK"]
    )

    postings = source.fetch

    assert_equal ["FLEXY"], postings.map(&:client)
  end

  # --- リクエスト予算: search_targets 数 × max_pages が40を超えないよう丸める ---

  def test_fetch_never_exceeds_request_budget
    fetcher = RecordingFetcher.new(body: read_fixture(RUBY_FIXTURE_NAME))
    source = FreelanceJobs::Sources::FreelanceBoard.new(fetcher: fetcher, today: TODAY, max_pages: 100)

    source.fetch

    assert_equal 39, fetcher.requested_urls.size, "3キーワード × 13ページ = 39回に丸められるはず"
  end

  # --- ページ単位の取得失敗に耐える（1回リトライ → キーワード単位で打ち切り → 全滅時のみ例外） ---

  # URLごとの応答を台本化するFakeフェッチャー（通信しない）。
  # 各URLの配列を先頭から1回ずつ消費し、例外なら送出・文字列ならbodyとして返す。
  # 台本に無いURL・使い切ったURLは案件0件のページを返す（＝ページ送りの終端）。
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
      { "#{LIST_URL}Ruby" => [server_error, read_fixture(RUBY_FIXTURE_NAME)] },
      fallback_body: empty_list_body
    )
    postings = silencing_fetch_logs { build_source(fetcher, %w[Ruby]).fetch }

    assert_equal 30, postings.size, "1回目が500でも再取得すれば取得できるはず"
    assert_equal ["#{LIST_URL}Ruby", "#{LIST_URL}Ruby", "#{LIST_URL}Ruby&page=2"], fetcher.requested_urls
  end

  # HTTPエラーだけでなく通信層の切断・タイムアウトも同じく1キーワードの打ち切りで済ませる。
  def test_fetch_skips_failing_keyword_and_keeps_other_keywords
    connection_error = Errno::ECONNRESET.new("Connection reset by peer")
    fetcher = ScriptedFetcher.new(
      {
        "#{LIST_URL}TypeScript" => [connection_error, connection_error],
        "#{LIST_URL}Ruby" => [read_fixture(RUBY_FIXTURE_NAME)]
      },
      fallback_body: empty_list_body
    )
    postings = silencing_fetch_logs { build_source(fetcher, %w[TypeScript Ruby]).fetch }

    assert_equal 30, postings.size, "TypeScriptが落ちてもRubyの結果は返すはず"
    refute_includes fetcher.requested_urls, "#{LIST_URL}TypeScript&page=2",
                    "2回とも失敗したキーワードはページ送りを打ち切るはず"
  end

  # 全滅を黙って0件で返すとサイト構造の崩れに気付けないため、最初の失敗を送出する。
  def test_fetch_raises_when_no_page_succeeds
    error = server_error
    fetcher = ScriptedFetcher.new({ "#{LIST_URL}Ruby" => [error, error] }, fallback_body: empty_list_body)

    raised = assert_raises(FreelanceJobs::FetchError) do
      silencing_fetch_logs { build_source(fetcher, %w[Ruby]).fetch }
    end

    assert_equal error.message, raised.message
  end

  # WAFのアクセス制限は取り直しても解消しないので、再取得せずそのまま送出する。
  def test_fetch_does_not_retry_when_access_is_blocked
    fetcher = ScriptedFetcher.new(
      { "#{LIST_URL}Ruby" => [FreelanceJobs::AccessBlockedError.new("アクセス制限（WAF captcha）")] },
      fallback_body: empty_list_body
    )

    assert_raises(FreelanceJobs::AccessBlockedError) do
      silencing_fetch_logs { build_source(fetcher, %w[Ruby]).fetch }
    end
    assert_equal 1, fetcher.requested_urls.size, "アクセス制限では再取得しないはず"
  end

  # --- 既定値 ---

  def test_default_search_targets_and_excluded_providers
    keywords = FreelanceJobs::Sources::FreelanceBoard::DEFAULT_SEARCH_TARGETS.map { |target| target[:keyword] }

    assert_equal %w[Ruby TypeScript React], keywords
    assert_equal 3, FreelanceJobs::Sources::FreelanceBoard::MAX_PAGES
    assert_equal 1.5, FreelanceJobs::Sources::FreelanceBoard::REQUEST_INTERVAL
    assert_includes FreelanceJobs::Sources::FreelanceBoard::DEFAULT_EXCLUDED_PROVIDERS, "レバテックフリーランス"
    assert_includes FreelanceJobs::Sources::FreelanceBoard::DEFAULT_EXCLUDED_PROVIDERS, "Findy Freelance"
    assert_includes FreelanceJobs::Sources::FreelanceBoard::DEFAULT_EXCLUDED_PROVIDERS, "Bizlink"
  end

  # --- Profile::ENGINEER にFreelanceBoardが含まれる（BEGINNERには含まれない） ---

  def test_engineer_profile_includes_freelance_board_source
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_includes source_classes, FreelanceJobs::Sources::FreelanceBoard
  end

  def test_beginner_profile_does_not_include_freelance_board_source
    source_classes = FreelanceJobs::Profile::BEGINNER.source_specs.map(&:first)

    refute_includes source_classes, FreelanceJobs::Sources::FreelanceBoard
  end

  private

  def build_source(fetcher, keywords, max_pages: 2)
    search_targets = keywords.map { |keyword| { keyword: keyword, hint: keyword } }
    FreelanceJobs::Sources::FreelanceBoard.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, max_pages: max_pages, excluded_providers: []
    )
  end

  def server_error
    FreelanceJobs::FetchError.new("HTTP 500 #{LIST_URL}Ruby")
  end

  def empty_list_body
    wrap_html("<div>該当なし</div>")
  end

  # Ruby 2.6 には Array#tally が無いため自前で数える。
  def count_by(values)
    values.each_with_object(Hash.new(0)) { |value, counts| counts[value] += 1 }
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

  # 合成した案件レコード（普通のHash）を devalue 形式に変換して parse に通す。
  def parse_job_record(job_record)
    body = wrap_nuxt_data(build_nuxt_top_level_values([job_record]))
    FreelanceJobs::Sources::FreelanceBoard.parse(body, today: TODAY, category_hint: "Ruby")
  end

  def wrap_nuxt_data(top_level_values)
    wrap_html(%(<script type="application/json" id="__NUXT_DATA__">#{JSON.generate(top_level_values)}</script>))
  end

  # build_nuxt_top_level_values が案件一覧Hash（"jobs"/"count"）を置くトップ配列上の位置。
  JOB_INDEX_POSITION = 3

  # 案件レコードの配列から、実サイトと同じ devalue 形式のトップ配列を組み立てる
  # （Hash/Array の値はすべてトップ配列のインデックス）。先頭にはサイト同様のラッパを置く。
  def build_nuxt_top_level_values(job_records)
    top_level_values = [["Reactive", 1], { "data" => 2 }, { "getJobIndex" => JOB_INDEX_POSITION }, nil]
    job_references = job_records.map { |job_record| devalue_encode(top_level_values, job_record) }
    jobs_reference = devalue_push(top_level_values, job_references)
    count_reference = devalue_push(top_level_values, job_records.size)
    top_level_values[JOB_INDEX_POSITION] = { "limit_value" => count_reference, "count" => count_reference, "jobs" => jobs_reference }
    top_level_values
  end

  # 値をトップ配列に積み、そのインデックスを返す。Hash/Array は中身も再帰的に積んで参照に置き換える。
  def devalue_encode(top_level_values, value)
    case value
    when Hash
      devalue_push(top_level_values, value.each_with_object({}) do |(key, inner_value), encoded|
        encoded[key] = devalue_encode(top_level_values, inner_value)
      end)
    when Array
      devalue_push(top_level_values, value.map { |inner_value| devalue_encode(top_level_values, inner_value) })
    else
      devalue_push(top_level_values, value)
    end
  end

  def devalue_push(top_level_values, value)
    top_level_values << value
    top_level_values.size - 1
  end

  # 実データのキー構成を模した案件レコード1件（既定値は月額80万円・FLEXY提供の正常系）。
  def build_job_record(overrides = {})
    service_name = overrides.delete("service_name") || "FLEXY"
    {
      "id" => 900001,
      "display_title" => "Ruby on Rails バックエンドエンジニア｜テスト案件",
      "name" => "【Ruby on Rails】原題の案件名",
      "detail" => "決済基盤の開発を担当します。\n要件定義から運用まで。",
      "required" => "・Ruby on Rails 3年以上",
      "welcome" => "・AWS運用経験",
      "ai_summary" => "Railsで決済基盤を開発する案件です。",
      "monthly_payment_f_num" => 800_000,
      "monthly_payment_l_num" => 800_000,
      "hourly_payment_f_num" => nil,
      "hourly_payment_l_num" => nil,
      "business_day_desc" => "週5日",
      "generation_ai_project_flg" => 2,
      "skill_ids" => %w[4 130],
      "skill_key_values" => { "4" => { "id" => 4, "name" => "Ruby" }, "130" => { "id" => 130, "name" => "Ruby on Rails" } },
      "occupation_ids" => %w[2],
      "occupation_key_values" => { "2" => { "id" => 2, "name" => "バックエンドエンジニア" } },
      "prefecture_key_values" => { "13" => { "id" => 13, "name" => "東京都" } },
      "station_key_values" => nil,
      "work_styles_key_values" => { "3" => { "id" => 3, "name" => "フルリモート" } },
      "agent_key_values" => { "5" => { "id" => 5, "service_name" => service_name } },
      "first_published_at" => "2026-09-12T04:07:30.000+09:00",
      "closed_at" => nil
    }.merge(overrides)
  end
end
