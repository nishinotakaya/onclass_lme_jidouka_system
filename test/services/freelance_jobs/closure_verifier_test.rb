# frozen_string_literal: true
# test/services/freelance_jobs/closure_verifier_test.rb
#
# 通信なし。Fakeフェッチャー（URL固定のHashを返す・呼ばれたURLを記録する・特定URLで例外を
# 投げられる）とFakeロガー（warn/infoを配列に貯める）を使い、AC-12のFreelanceJobs::ClosureVerifier
# の振る舞いだけを検証する。app/側の実装はまだ無いので、現時点は全件Red（NameError等）で良い。

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"

class FreelanceJobsClosureVerifierTest < Minitest::Test
  include FreelanceJobsTestHelpers

  # warn/infoの呼び出し内容を配列に貯めるだけのFakeロガー。
  class FakeLogger
    attr_reader :warnings, :infos

    def initialize
      @warnings = []
      @infos = []
    end

    def warn(message)
      @warnings << message
    end

    def info(message)
      @infos << message
    end
  end

  # URL => 本文（またはraiseする例外）のHashを台本にするFakeフェッチャー（通信しない）。
  # 台本に無いURLは空文字を返す（テスト対象外の取りこぼしが起きてもエラーにしない）。
  class ScriptedDetailFetcher
    def initialize(bodies_by_url: {}, raising_urls_by_url: {})
      @bodies_by_url = bodies_by_url
      @raising_urls_by_url = raising_urls_by_url
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url)
      @requested_urls << url
      raise @raising_urls_by_url[url] if @raising_urls_by_url[url]

      @bodies_by_url.fetch(url, "")
    end
  end

  # closed_detail?を持つダミーのsource_class。本文が"closed"のときだけ募集終了と判定する。
  class DetailCheckableSource
    SITE_NAME = "詳細確認ありサイト"

    def self.closed_detail?(body)
      body == "closed"
    end
  end

  # closed_detail?を持たないダミーのsource_class（確認対象外）。
  class DetailUncheckableSource
    SITE_NAME = "詳細確認なしサイト"
  end

  # SITE_NAMEだけが異なるもう一つの確認対象サイト（フェッチャーがサイトごとに1個であることの確認用）。
  class AnotherDetailCheckableSource
    SITE_NAME = "詳細確認ありサイト2"

    def self.closed_detail?(body)
      body == "closed"
    end
  end

  def build_row(site_name, url)
    row = Array.new(FreelanceJobs::SheetMerger::COLUMN_COUNT, "")
    row[FreelanceJobs::SheetMerger::SITE_COLUMN_INDEX] = site_name
    row[FreelanceJobs::SheetMerger::URL_COLUMN_INDEX] = url
    row
  end

  def build_verifier(source_specs:, fetcher_factory:, logger: FakeLogger.new)
    FreelanceJobs::ClosureVerifier.new(source_specs: source_specs, fetcher_factory: fetcher_factory, logger: logger)
  end

  # --- 確認順序: 既存行(existing)が新規候補行(candidateのみ)より先に確認される ---

  def test_existing_rows_are_checked_before_new_candidate_rows
    site_name = DetailCheckableSource::SITE_NAME
    new_url = "https://example.com/jobs/new-1"
    matched_url = "https://example.com/jobs/matched-1"
    existing_only_url = "https://example.com/jobs/existing-only-1"

    candidate_rows = [build_row(site_name, new_url), build_row(site_name, matched_url)]
    existing_rows = [build_row(site_name, matched_url), build_row(site_name, existing_only_url)]

    fetcher = ScriptedDetailFetcher.new
    verifier = build_verifier(source_specs: [[DetailCheckableSource, {}]], fetcher_factory: ->(_source_class) { fetcher })

    verifier.call(candidate_rows: candidate_rows, existing_rows: existing_rows)

    assert_equal [matched_url, existing_only_url, new_url], fetcher.requested_urls,
                 "existing_rowsの並び順(matched_url→existing_only_url)で先に確認され、新規候補(new_url)は最後に確認されるはず"
  end

  # --- existing_rows側の同サイト行も確認される ---

  def test_existing_rows_for_the_same_site_are_checked_even_without_new_candidates
    site_name = DetailCheckableSource::SITE_NAME
    existing_url = "https://example.com/jobs/existing-2"

    fetcher = ScriptedDetailFetcher.new(bodies_by_url: { existing_url => "closed" })
    verifier = build_verifier(source_specs: [[DetailCheckableSource, {}]], fetcher_factory: ->(_source_class) { fetcher })

    closed_urls = verifier.call(candidate_rows: [], existing_rows: [build_row(site_name, existing_url)])

    assert_equal [existing_url], fetcher.requested_urls
    assert_equal [FreelanceJobs::JobPosting.normalize_url(existing_url)], closed_urls
  end

  # --- 同じURLがcandidateとexisting両方にあっても1回しかgetされない ---

  def test_same_url_in_candidate_and_existing_is_checked_only_once
    site_name = DetailCheckableSource::SITE_NAME
    shared_url = "https://example.com/jobs/shared-1"

    fetcher = ScriptedDetailFetcher.new
    verifier = build_verifier(source_specs: [[DetailCheckableSource, {}]], fetcher_factory: ->(_source_class) { fetcher })

    verifier.call(
      candidate_rows: [build_row(site_name, shared_url)],
      existing_rows: [build_row(site_name, shared_url)]
    )

    assert_equal [shared_url], fetcher.requested_urls
  end

  # --- closed_detail?を持たないサイトの行には一切getしない ---

  def test_rows_of_a_site_without_closed_detail_support_are_never_fetched
    site_name = DetailUncheckableSource::SITE_NAME
    url = "https://example.com/jobs/uncheckable-1"

    fetcher = ScriptedDetailFetcher.new
    verifier = build_verifier(source_specs: [[DetailUncheckableSource, {}]], fetcher_factory: ->(_source_class) { fetcher })

    closed_urls = verifier.call(candidate_rows: [build_row(site_name, url)], existing_rows: [])

    assert_empty fetcher.requested_urls
    assert_empty closed_urls
  end

  # --- 空URLの行は対象外 ---

  def test_rows_with_blank_url_are_skipped
    site_name = DetailCheckableSource::SITE_NAME

    fetcher = ScriptedDetailFetcher.new
    verifier = build_verifier(source_specs: [[DetailCheckableSource, {}]], fetcher_factory: ->(_source_class) { fetcher })

    closed_urls = verifier.call(candidate_rows: [build_row(site_name, "")], existing_rows: [build_row(site_name, "")])

    assert_empty fetcher.requested_urls
    assert_empty closed_urls
  end

  # --- MAX_CHECKS_PER_SITE(80件)を超えたらgetは80回まで・超過分は警告ログ ---

  def test_max_checks_per_site_caps_fetch_count_and_logs_a_warning_for_the_rest
    site_name = DetailCheckableSource::SITE_NAME
    urls = (1..81).map { |sequence_number| "https://example.com/jobs/over-limit-#{sequence_number}" }
    candidate_rows = urls.map { |url| build_row(site_name, url) }

    fetcher = ScriptedDetailFetcher.new
    logger = FakeLogger.new
    verifier = build_verifier(source_specs: [[DetailCheckableSource, {}]], fetcher_factory: ->(_source_class) { fetcher },
                               logger: logger)

    verifier.call(candidate_rows: candidate_rows, existing_rows: [])

    assert_equal 80, fetcher.requested_urls.size, "MAX_CHECKS_PER_SITE(80)を超えた分はgetを呼ばないはず"
    assert_equal 80, FreelanceJobs::ClosureVerifier::MAX_CHECKS_PER_SITE
    assert(
      logger.warnings.any? { |message| message.include?(site_name) && message.include?("上限(80)") && message.include?("1 件") },
      "上限超過の警告ログが出るはず: #{logger.warnings.inspect}"
    )
  end

  # --- 上限超過時でも既存行(シートに載っている行)が必ず先に確認される ---

  def test_existing_rows_are_always_checked_first_even_when_new_candidates_exceed_the_limit
    site_name = DetailCheckableSource::SITE_NAME
    existing_urls = (1..3).map { |sequence_number| "https://example.com/jobs/existing-priority-#{sequence_number}" }
    new_candidate_urls = (1..100).map { |sequence_number| "https://example.com/jobs/new-flood-#{sequence_number}" }

    fetcher = ScriptedDetailFetcher.new
    verifier = build_verifier(source_specs: [[DetailCheckableSource, {}]], fetcher_factory: ->(_source_class) { fetcher })

    verifier.call(
      candidate_rows: new_candidate_urls.map { |url| build_row(site_name, url) },
      existing_rows: existing_urls.map { |url| build_row(site_name, url) }
    )

    assert_equal existing_urls, fetcher.requested_urls.first(3),
                 "新規候補が上限を超えても、既存行3件が先頭で確認されるはず"
    assert_equal 80, fetcher.requested_urls.size, "全体はMAX_CHECKS_PER_SITE(80)件で打ち切られるはず"
  end

  # --- AccessBlockedErrorが出たらそのサイトの確認を打ち切るが、既に閉鎖判定した分は戻り値に残る ---

  def test_access_blocked_error_stops_checking_the_site_but_keeps_already_found_closed_urls
    site_name = DetailCheckableSource::SITE_NAME
    closed_before_url = "https://example.com/jobs/closed-before-block"
    blocked_url = "https://example.com/jobs/blocked"
    never_checked_url = "https://example.com/jobs/never-checked"

    fetcher = ScriptedDetailFetcher.new(
      bodies_by_url: { closed_before_url => "closed", never_checked_url => "closed" },
      raising_urls_by_url: { blocked_url => FreelanceJobs::AccessBlockedError.new("アクセス制限（WAF captcha）") }
    )
    logger = FakeLogger.new
    candidate_rows = [closed_before_url, blocked_url, never_checked_url].map { |url| build_row(site_name, url) }
    verifier = build_verifier(source_specs: [[DetailCheckableSource, {}]], fetcher_factory: ->(_source_class) { fetcher },
                               logger: logger)

    closed_urls = verifier.call(candidate_rows: candidate_rows, existing_rows: [])

    assert_equal [closed_before_url, blocked_url], fetcher.requested_urls,
                 "AccessBlockedError発生後はそのサイトの以降の行を確認しないはず"
    assert_equal [FreelanceJobs::JobPosting.normalize_url(closed_before_url)], closed_urls,
                 "ブロック前に閉鎖判定した分は戻り値に残るはず"
    refute_empty logger.warnings
  end

  # --- その他のStandardErrorはそのURLだけ未判定で、後続のURLは確認が続く ---

  def test_other_standard_error_leaves_that_url_undetermined_but_continues_to_next_url
    site_name = DetailCheckableSource::SITE_NAME
    failing_url = "https://example.com/jobs/fetch-error"
    closed_after_url = "https://example.com/jobs/closed-after-error"

    fetcher = ScriptedDetailFetcher.new(
      bodies_by_url: { closed_after_url => "closed" },
      raising_urls_by_url: { failing_url => FreelanceJobs::FetchError.new("HTTP 500") }
    )
    logger = FakeLogger.new
    candidate_rows = [failing_url, closed_after_url].map { |url| build_row(site_name, url) }
    verifier = build_verifier(source_specs: [[DetailCheckableSource, {}]], fetcher_factory: ->(_source_class) { fetcher },
                               logger: logger)

    closed_urls = verifier.call(candidate_rows: candidate_rows, existing_rows: [])

    assert_equal [failing_url, closed_after_url], fetcher.requested_urls, "エラーになったURLの次も確認が続くはず"
    assert_equal [FreelanceJobs::JobPosting.normalize_url(closed_after_url)], closed_urls,
                 "エラーになったURLは閉鎖として扱われず、後続の閉鎖判定だけが残るはず"
    refute_empty logger.warnings
  end

  # --- 戻り値はnormalize_url済み ---

  def test_returned_urls_are_normalized
    site_name = DetailCheckableSource::SITE_NAME
    raw_url = "HTTP://Example.com/jobs/NeedsNormalizing/"

    fetcher = ScriptedDetailFetcher.new(bodies_by_url: { raw_url => "closed" })
    verifier = build_verifier(source_specs: [[DetailCheckableSource, {}]], fetcher_factory: ->(_source_class) { fetcher })

    closed_urls = verifier.call(candidate_rows: [build_row(site_name, raw_url)], existing_rows: [])

    assert_equal [FreelanceJobs::JobPosting.normalize_url(raw_url)], closed_urls
    refute_equal [raw_url], closed_urls, "正規化前の生URLのままでは無いはず"
  end

  # --- サイトごとに1個だけfetcher_factory.callでfetcherを作る（HttpFetcherの間隔がサイト内で効くため） ---

  def test_fetcher_factory_is_called_exactly_once_per_site
    site_a_name = DetailCheckableSource::SITE_NAME
    site_b_name = AnotherDetailCheckableSource::SITE_NAME
    fetcher_a = ScriptedDetailFetcher.new
    fetcher_b = ScriptedDetailFetcher.new
    fetcher_factory_calls = []
    fetcher_factory = lambda do |source_class|
      fetcher_factory_calls << source_class
      source_class == DetailCheckableSource ? fetcher_a : fetcher_b
    end

    candidate_rows = [
      build_row(site_a_name, "https://example.com/jobs/a-1"),
      build_row(site_a_name, "https://example.com/jobs/a-2"),
      build_row(site_b_name, "https://example.com/jobs/b-1")
    ]
    verifier = build_verifier(
      source_specs: [[DetailCheckableSource, {}], [AnotherDetailCheckableSource, {}]],
      fetcher_factory: fetcher_factory
    )

    verifier.call(candidate_rows: candidate_rows, existing_rows: [])

    assert_equal [DetailCheckableSource, AnotherDetailCheckableSource], fetcher_factory_calls
    assert_equal 2, fetcher_a.requested_urls.size
    assert_equal 1, fetcher_b.requested_urls.size
  end

  # --- 対象URLが0件のサイトはfetcher_factoryすら呼ばない（無駄なfetcher生成＝REQUEST_INTERVALの
  #     sleepを避けるため）。closed_detail?を持つが該当行が1件も無いサイトを混ぜて確認する ---

  def test_fetcher_factory_is_not_called_for_a_site_with_no_target_rows
    site_with_rows_name = DetailCheckableSource::SITE_NAME
    fetcher_for_site_with_rows = ScriptedDetailFetcher.new
    fetcher_factory_calls = []
    fetcher_factory = lambda do |source_class|
      fetcher_factory_calls << source_class
      fetcher_for_site_with_rows
    end

    # AnotherDetailCheckableSource宛ての行は candidate にも existing にも無い。
    candidate_rows = [build_row(site_with_rows_name, "https://example.com/jobs/with-rows-1")]
    verifier = build_verifier(
      source_specs: [[DetailCheckableSource, {}], [AnotherDetailCheckableSource, {}]],
      fetcher_factory: fetcher_factory
    )

    verifier.call(candidate_rows: candidate_rows, existing_rows: [])

    assert_equal [DetailCheckableSource], fetcher_factory_calls,
                 "対象行が無いAnotherDetailCheckableSourceではfetcher_factoryを呼ばないはず"
    refute_includes fetcher_factory_calls, AnotherDetailCheckableSource
  end

  # --- info ログ: サイトごとに実際にgetした件数と閉鎖判定数 ---

  def test_logs_an_info_summary_per_site_with_checked_and_closed_counts
    site_name = DetailCheckableSource::SITE_NAME
    open_url = "https://example.com/jobs/open-1"
    closed_url = "https://example.com/jobs/closed-1"

    fetcher = ScriptedDetailFetcher.new(bodies_by_url: { closed_url => "closed", open_url => "open" })
    logger = FakeLogger.new
    candidate_rows = [open_url, closed_url].map { |url| build_row(site_name, url) }
    verifier = build_verifier(source_specs: [[DetailCheckableSource, {}]], fetcher_factory: ->(_source_class) { fetcher },
                               logger: logger)

    verifier.call(candidate_rows: candidate_rows, existing_rows: [])

    assert(
      logger.infos.any? { |message| message.include?(site_name) && message.include?("2 件中") && message.include?("1 件") },
      "サイト名・確認件数・閉鎖件数を含むinfoログが出るはず: #{logger.infos.inspect}"
    )
  end
end
