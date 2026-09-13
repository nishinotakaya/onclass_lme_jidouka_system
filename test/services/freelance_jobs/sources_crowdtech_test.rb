# frozen_string_literal: true
# test/services/freelance_jobs/sources_crowdtech_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"
require "json"

class FreelanceJobsSourcesCrowdtechTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 13)
  # 一覧API（word=Ruby 検索）の生レスポンス。裸の配列で20要素。
  FIXTURE_NAME = "crowdtech_ruby.json"
  LIST_API_URL = "https://tech.crowdworks.jp/api/v1/users/job_offers"

  def parse_fixture(category_hint: "Ruby")
    FreelanceJobs::Sources::Crowdtech.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  def parse_json(job_offers, category_hint: "Ruby")
    FreelanceJobs::Sources::Crowdtech.parse(JSON.generate(job_offers), today: TODAY, category_hint: category_hint)
  end

  def test_parse_fixture_returns_twenty_postings
    assert_equal 20, parse_fixture.size
  end

  # --- 1件目の全フィールド（id 96503） ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "クラウドテック", first.site
    assert_equal "https://tech.crowdworks.jp/job_offers/96503", first.url
    assert_equal "【Ruby/週5日/一部リモート/恵比寿】トレーディングカード事業のプロダクト開発業務案件", first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "〜1,000,000円／月", first.reward
    assert_equal "月額制（業務委託）", first.work_format
    assert_equal "募集中", first.application_status
    assert_equal ["Rails", "ReactNative", "TypeScript"], first.skills
    assert_equal ["NEW", "一部リモート", "服装自由", "髪型自由", "自社サービスがある", "BtoCサービス"], first.tags
    assert_equal "", first.client
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_nil first.posted_on
  end

  # descriptionは「募集職種 / 使用技術 / 稼働 / 勤務形態 / 勤務地 / 本文」の順に連結する。
  # RowBuilderが先頭160文字で切るため、分類・メモ判定に効くメタ情報を本文より前に置く。
  def test_parse_first_posting_description_puts_meta_before_body
    description = parse_fixture.first.description

    assert description.start_with?(
      "募集職種: バックエンドエンジニア / 使用技術: Rails / ReactNative / TypeScript / 稼働: 週5日 / " \
      "勤務形態: 一部リモート / 勤務地: 恵比寿 / ■期待するミッション 事業の中核を担うプロダクトの設計・開発・運用を通じて"
    ), description
    assert_includes description, "■開発環境 ・プログラミング：Ruby, TypeScript ・FW：Ruby on Rails, React Native (Expo)"
    refute_includes description, "\n", "本文の改行は normalize_description で畳まれるはず"
  end

  # --- 勤務形態（workStyle）は3値を日本語にして description と tags の両方に入れる ---

  # フルリモート案件でも officeLocation には所属拠点の駅名が入ることがある（96455 は "東大前"）ので、
  # リモート可否は officeLocation ではなく workStyle から出す。
  def test_parse_full_remote_posting_uses_work_style_not_office_location
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/96455") }

    refute_nil posting
    assert_equal "〜750,000円／月", posting.reward
    assert_includes posting.description, "勤務形態: フルリモート / 勤務地: 東大前"
    assert_equal ["NEW", "フルリモート"], posting.tags.first(2), "NEW → 勤務形態 → こだわり条件 の順のはず"
  end

  def test_parse_office_work_posting
    posting = parse_fixture.find { |candidate| candidate.url.end_with?("/95234") }

    refute_nil posting
    assert_includes posting.description, "勤務形態: 常駐（出社） / 勤務地: 天神南駅"
    assert_equal "常駐（出社）", posting.tags.first
  end

  # --- 全件で必須フィールドが埋まる（"-"や""は許すがnilは許さない） ---

  def test_every_posting_fills_display_fields
    parse_fixture.each do |posting|
      refute_empty posting.title
      refute_empty posting.reward
      refute_empty posting.work_format
      assert_equal "募集中", posting.application_status, "openonly=true の結果は全件募集中のはず"
      assert_instance_of Array, posting.skills
      refute_empty posting.skills, "フィクスチャ全件に使用技術がある"
      assert_instance_of Array, posting.tags
    end
  end

  # --- URL正規化 ---

  def test_urls_are_normalized_absolute_without_trailing_slash_or_query
    parse_fixture.each do |posting|
      assert_match %r{\Ahttps://tech\.crowdworks\.jp/job_offers/\d+\z}, posting.url,
                   "末尾スラッシュなし・クエリなしの正規化された絶対URLのはず"
    end
  end

  # --- category_hint が引数どおり全件に伝わる ---

  def test_category_hint_is_propagated_to_every_posting
    postings = parse_fixture(category_hint: "TypeScript")

    assert(postings.all? { |posting| posting.category_hint == "TypeScript" })
  end

  # --- 単価が無いときは要確認・汎用の業務委託に寄せる ---

  def test_reward_and_work_format_fall_back_when_max_unit_price_is_missing
    posting = parse_json([build_job_offer(1, "maxUnitPrice" => nil)]).first

    assert_equal "要確認", posting.reward
    assert_equal "業務委託（フリーランス）", posting.work_format
  end

  def test_reward_falls_back_when_max_unit_price_is_zero
    posting = parse_json([build_job_offer(2, "maxUnitPrice" => 0)]).first

    assert_equal "要確認", posting.reward
  end

  # --- 募集終了・新着なし・未知の勤務形態 ---

  def test_closed_posting_without_new_badge
    posting = parse_json([build_job_offer(3, "open" => false, "new" => false)]).first

    assert_equal "募集終了", posting.application_status
    refute_includes posting.tags, "NEW"
  end

  def test_application_status_is_dash_when_open_flag_is_missing
    posting = parse_json([build_job_offer(4, "open" => nil)]).first

    assert_equal "-", posting.application_status
  end

  def test_unknown_work_style_is_passed_through
    posting = parse_json([build_job_offer(5, "workStyle" => "hybrid_work")]).first

    assert_includes posting.description, "勤務形態: hybrid_work"
    assert_includes posting.tags, "hybrid_work"
  end

  # --- 必須要素（id・title）が欠けた要素は黙って除外する ---

  def test_parse_skips_entry_without_id
    job_offer = build_job_offer(7).tap { |entry| entry.delete("id") }

    assert_equal [], parse_json([job_offer])
  end

  def test_parse_skips_entry_with_blank_title
    assert_equal [], parse_json([build_job_offer(8, "title" => "  ")])
  end

  def test_parse_skips_non_hash_entries_but_keeps_valid_ones
    postings = parse_json(["文字列", 42, nil, build_job_offer(9)])

    assert_equal ["https://tech.crowdworks.jp/job_offers/9"], postings.map(&:url)
  end

  def test_parse_returns_empty_when_body_is_not_an_array
    assert_equal [], parse_json({ "jobOffers" => [build_job_offer(10)] })
  end

  def test_parse_returns_empty_when_body_is_not_json
    assert_equal [], FreelanceJobs::Sources::Crowdtech.parse(wrap_html("<div id=\"app\"></div>"), today: TODAY)
  end

  # occupation / skills / appeals のキー名が変わって取れなくなっても、行そのものは落とさない。
  def test_parse_tolerates_missing_nested_fields
    job_offer = build_job_offer(11, "occupation" => nil, "skills" => nil, "appeals" => "壊れた値")
    posting = parse_json([job_offer]).first

    refute_nil posting
    assert_equal [], posting.skills
    assert_equal ["NEW", "一部リモート"], posting.tags
    refute_includes posting.description, "募集職種"
  end

  def test_parse_deduplicates_same_id_within_a_page
    postings = parse_json([build_job_offer(12), build_job_offer(12, "title" => "重複")])

    assert_equal 1, postings.size
  end

  # --- fetch: 絞り込み×ページ数だけ取得し、URL重複を排除する ---

  # 取得URLとヘッダを記録し、20件の本文を返すFakeフェッチャー（通信しない）。
  class RecordingFetcher
    def initialize(body:)
      @body = body
      @requested_urls = []
      @requested_headers = []
    end

    attr_reader :requested_urls, :requested_headers

    def get(url, headers: {})
      @requested_urls << url
      @requested_headers << headers
      @body
    end
  end

  def test_fetch_requests_each_target_and_page_with_json_accept_header_and_deduplicates_urls
    fetcher = RecordingFetcher.new(body: read_fixture(FIXTURE_NAME))
    search_targets = [
      { master_skill_id: 3, hint: "Ruby" },
      { master_skill_id: 15, hint: "TypeScript" }
    ]
    source = FreelanceJobs::Sources::Crowdtech.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, max_pages: 2
    )

    postings = source.fetch

    assert_equal(
      [
        "#{LIST_API_URL}?skill_ids%5B%5D=3&openonly=true&order=newest_first&page=1",
        "#{LIST_API_URL}?skill_ids%5B%5D=3&openonly=true&order=newest_first&page=2",
        "#{LIST_API_URL}?skill_ids%5B%5D=15&openonly=true&order=newest_first&page=1",
        "#{LIST_API_URL}?skill_ids%5B%5D=15&openonly=true&order=newest_first&page=2"
      ],
      fetcher.requested_urls
    )
    assert(fetcher.requested_headers.all? { |headers| headers == { "Accept" => "application/json" } },
           "全リクエストに Accept: application/json を付けるはず")
    assert_equal 20, postings.size, "同じ案件が4ページ分返っても重複排除され20件のままのはず"
    assert_equal "Ruby", postings.first.category_hint, "先に取得した絞り込みの hint が残るはず"
  end

  def test_fetch_stops_paging_when_a_page_returns_fewer_than_page_size
    fetcher = RecordingFetcher.new(body: JSON.generate(fixture_job_offers.first(19)))
    source = build_source(fetcher, [3], max_pages: 3)

    postings = source.fetch

    assert_equal 1, fetcher.requested_urls.size, "19件（20件未満）なら最終ページなので2ページ目は取らないはず"
    assert_equal 19, postings.size
  end

  # URLごとの応答を台本化するFakeフェッチャー（通信しない）。
  # 各URLの配列を先頭から1回ずつ消費し、例外なら送出・文字列ならbodyとして返す。
  # 台本に無いURL・使い切ったURLは「ページ無し」の 404 を送出する（＝実サイトのページ送りの終端）。
  class ScriptedFetcher
    def initialize(script)
      @script = script
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      response = @script[url]&.shift || FreelanceJobs::FetchError.new("HTTP 404 #{url}")
      raise response if response.is_a?(StandardError)

      response
    end
  end

  # ちょうど20件で終わった絞り込みは次ページが 404 になる。これは終端であって失敗ではないので、
  # 再取得せず・失敗にも数えず、そのまま結果を返す。
  def test_fetch_treats_not_found_on_second_page_as_end_of_listing
    fetcher = ScriptedFetcher.new({ list_url(3, 1) => [read_fixture(FIXTURE_NAME)] })

    postings = silencing_fetch_logs { build_source(fetcher, [3], max_pages: 3).fetch }

    assert_equal 20, postings.size
    assert_equal [list_url(3, 1), list_url(3, 2)], fetcher.requested_urls, "2ページ目の404は取り直さないはず"
  end

  def test_fetch_retries_once_when_a_page_returns_server_error
    fetcher = ScriptedFetcher.new({ list_url(3, 1) => [server_error, read_fixture(FIXTURE_NAME)] })

    postings = silencing_fetch_logs { build_source(fetcher, [3]).fetch }

    assert_equal 20, postings.size, "1回目が500でも再取得すれば取得できるはず"
    assert_equal [list_url(3, 1), list_url(3, 1), list_url(3, 2)], fetcher.requested_urls
  end

  # HTTPエラーだけでなく通信層の切断・タイムアウトも同じく1絞り込みの打ち切りで済ませる。
  def test_fetch_skips_failing_target_and_keeps_other_targets
    connection_error = Errno::ECONNRESET.new("Connection reset by peer")
    fetcher = ScriptedFetcher.new(
      {
        list_url(15, 1) => [connection_error, connection_error],
        list_url(3, 1) => [read_fixture(FIXTURE_NAME)]
      }
    )

    postings = silencing_fetch_logs { build_source(fetcher, [15, 3]).fetch }

    assert_equal 20, postings.size, "TypeScriptが落ちてもRubyの結果は返すはず"
    refute_includes fetcher.requested_urls, list_url(15, 2), "2回とも失敗した絞り込みはページ送りを打ち切るはず"
  end

  # 全滅を黙って0件で返すとAPIやマスターIDの変更に気付けないため、最初の失敗を送出する。
  def test_fetch_raises_when_no_page_succeeds
    error = server_error
    fetcher = ScriptedFetcher.new({ list_url(3, 1) => [error, error] })

    raised = assert_raises(FreelanceJobs::FetchError) do
      silencing_fetch_logs { build_source(fetcher, [3]).fetch }
    end

    assert_equal error.message, raised.message
  end

  # 1ページ目の404は「該当0件」＝マスタースキルIDの失効が疑われるので、失敗として扱う
  # （取り直しはせず、全絞り込みが404なら送出する）。
  def test_fetch_raises_when_every_target_returns_not_found_on_first_page
    fetcher = ScriptedFetcher.new({})

    raised = assert_raises(FreelanceJobs::FetchError) do
      silencing_fetch_logs { build_source(fetcher, [3, 15]).fetch }
    end

    assert_match(/HTTP 404/, raised.message)
    assert_equal [list_url(3, 1), list_url(15, 1)], fetcher.requested_urls, "404は取り直さないはず"
  end

  # WAFのアクセス制限は取り直しても解消しないので、再取得せずそのまま送出する。
  def test_fetch_does_not_retry_when_access_is_blocked
    fetcher = ScriptedFetcher.new(
      { list_url(3, 1) => [FreelanceJobs::AccessBlockedError.new("アクセス制限（WAF captcha）")] }
    )

    assert_raises(FreelanceJobs::AccessBlockedError) do
      silencing_fetch_logs { build_source(fetcher, [3]).fetch }
    end
    assert_equal 1, fetcher.requested_urls.size
  end

  # --- 既定値: マスタースキルID（案件JSONの skills[].id とは別体系）と最大ページ数 ---

  def test_default_search_targets_use_master_skill_ids
    targets = FreelanceJobs::Sources::Crowdtech::DEFAULT_SEARCH_TARGETS

    assert_equal [3, 15, 103], targets.map { |target| target[:master_skill_id] }
    assert_equal %w[Ruby TypeScript React], targets.map { |target| target[:hint] }
    assert_equal 2, FreelanceJobs::Sources::Crowdtech::MAX_PAGES
    assert_equal 20, FreelanceJobs::Sources::Crowdtech::PAGE_SIZE
  end

  def test_site_name_is_distinct_from_crowdworks
    refute_equal FreelanceJobs::Sources::Crowdworks::SITE_NAME, FreelanceJobs::Sources::Crowdtech::SITE_NAME
    assert_equal "クラウドテック", FreelanceJobs::Sources::Crowdtech::SITE_NAME
  end

  # --- Profile::ENGINEER にCrowdtechが含まれる（BEGINNERには含まれない） ---

  def test_engineer_profile_includes_crowdtech_source
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_includes source_classes, FreelanceJobs::Sources::Crowdtech
  end

  def test_beginner_profile_does_not_include_crowdtech_source
    source_classes = FreelanceJobs::Profile::BEGINNER.source_specs.map(&:first)

    refute_includes source_classes, FreelanceJobs::Sources::Crowdtech
  end

  private

  def build_source(fetcher, master_skill_ids, max_pages: 2)
    search_targets = master_skill_ids.map { |master_skill_id| { master_skill_id: master_skill_id, hint: "Ruby" } }
    FreelanceJobs::Sources::Crowdtech.new(
      fetcher: fetcher, today: TODAY, search_targets: search_targets, max_pages: max_pages
    )
  end

  def list_url(master_skill_id, page_number)
    "#{LIST_API_URL}?skill_ids%5B%5D=#{master_skill_id}&openonly=true&order=newest_first&page=#{page_number}"
  end

  def server_error
    FreelanceJobs::FetchError.new("HTTP 500 #{list_url(3, 1)}")
  end

  def fixture_job_offers
    JSON.parse(read_fixture(FIXTURE_NAME))
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

  # 一覧API 1要素分の案件Hash（実データのキー構成を模したもの）。上書きしたいキーだけ渡す。
  def build_job_offer(id, overrides = {})
    {
      "id" => id,
      "open" => true,
      "new" => true,
      "title" => "【Ruby/週5日/一部リモート/恵比寿】テスト案件",
      "maxUnitPrice" => 800_000,
      "description" => "■期待するミッション\nRailsでの開発",
      "officeLocation" => "恵比寿",
      "requiredWorkingDays" => 5,
      "workStyle" => "partial_remote_work",
      "occupation" => { "id" => 54, "name" => "バックエンドエンジニア" },
      "skills" => [{ "id" => 273, "name" => "Rails" }],
      "appeals" => [{ "id" => 7, "description" => "服装自由" }]
    }.merge(overrides)
  end
end
