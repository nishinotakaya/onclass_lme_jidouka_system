# frozen_string_literal: true
# test/services/freelance_jobs/sources_coconala_tech_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesCoconalaTechTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 12)
  FIXTURE_NAME = "coconala_tech_ruby.html"
  # フィクスチャ（skillIds=4 の1ページ目）の内訳。募集中が先頭5件、残り25件が募集終了。
  FIXTURE_POSTING_COUNT = 30
  FIXTURE_OPEN_POSTING_COUNT = 5

  def parse_fixture(category_hint: "Ruby")
    FreelanceJobs::Sources::CoconalaTech.parse(read_fixture(FIXTURE_NAME), today: TODAY, category_hint: category_hint)
  end

  # --- 件数（PC用/SP用で同じアンカーが2本出るが、URLキーで重複排除される） ---

  def test_parse_fixture_returns_thirty_postings
    assert_equal FIXTURE_POSTING_COUNT, parse_fixture.size,
                 "1ページ60本のアンカーがユニーク30件に重複排除されるはず"
  end

  # --- 1件目の全フィールド ---

  def test_parse_first_posting_has_expected_fields
    first = parse_fixture.first

    assert_equal "ココナラテック", first.site
    assert_equal "https://tech.coconala.com/job-postings/019dab34-506e-75d6-9e10-b3966f0d3d64", first.url
    assert_equal "【Ruby on Rails/React/TypeScript/リモート併用】フルスタックエンジニア／運送会社向けクラウド業務支援システム開発・保守",
                 first.title
    assert_equal "Ruby", first.category_hint
    assert_equal "〜800,000円/月", first.reward
    assert_equal "月額制（業務委託）", first.work_format
    assert_equal "募集中", first.application_status
    assert_equal "-", first.deadline_text
    assert_nil first.deadline_on
    assert_equal ["Ruby", "Rails", "AWS", "GraphQL"], first.skills
    assert_equal "", first.client, "発注者名は一覧にも詳細ページにも無い（社名非公開）ため空文字"
    assert_equal ["リモート可"], first.tags
    assert_equal Date.new(2026, 4, 20), first.posted_on
  end

  # descriptionは「職種・契約形態 / 勤務地 / 使用技術 / 募集本文」の順に連結される。
  # 分類器の精度がこの連結内容で決まるため、各パートが欠けていないことを個別に確かめる。
  def test_parse_first_posting_description_contains_every_part
    description = parse_fixture.first.description

    assert description.start_with?("フルスタックエンジニア(業務委託・フリーランス) / 中央区（東京都） / 使用技術: Ruby / Rails / AWS / GraphQL / "),
           "職種・勤務地・使用技術がこの順で先頭に並ぶはず: #{description[0, 120]}"
    assert_includes description, "【作業内容】", "CSSで省略表示されているだけの募集本文がDOMから全文取れるはず"
    assert_includes description, "【開発環境】"
    assert_includes description, "React 18 / TypeScript 5.1", "開発環境の技術名まで含まれるはず"
  end

  # --- 募集終了カードもparseはそのまま返す（除外はfetchのinclude_closedが担当） ---

  def test_parse_keeps_closed_postings_with_closed_status
    postings = parse_fixture
    closed_postings = postings.select { |posting| posting.application_status == "募集終了" }

    assert_equal FIXTURE_POSTING_COUNT - FIXTURE_OPEN_POSTING_COUNT, closed_postings.size
    assert_equal ["募集終了"], closed_postings.first.tags
    assert_equal "https://tech.coconala.com/job-postings/019406b6-ce3b-77d2-baec-e0294783c406", closed_postings.first.url
    assert_equal "【Ruby】動画配信システムのカスタマイズ開発", closed_postings.first.title
    assert_equal Date.new(2024, 12, 27), closed_postings.first.posted_on
  end

  def test_parse_marks_cards_without_closed_badge_as_open
    open_postings = parse_fixture.select { |posting| posting.application_status == "募集中" }

    assert_equal FIXTURE_OPEN_POSTING_COUNT, open_postings.size
    assert(open_postings.none? { |posting| posting.tags.include?("募集終了") })
  end

  # --- URL正規化 ---

  def test_urls_are_normalized_absolute_without_trailing_slash_or_query
    parse_fixture.each do |posting|
      assert_match %r{\Ahttps://tech\.coconala\.com/job-postings/[0-9a-f-]{36}\z}, posting.url,
                   "末尾スラッシュなし・クエリなしの正規化された絶対URLのはず"
    end
  end

  # --- category_hint が引数どおり全件に伝わる ---

  def test_category_hint_is_propagated_to_every_posting
    postings = parse_fixture(category_hint: "React")

    assert(postings.all? { |posting| posting.category_hint == "React" },
           "全件のcategory_hintが引数のReactになるはず")
  end

  # --- 必須フィールドが全件埋まる（シートに nil が出ない） ---

  def test_every_posting_fills_display_fields_without_nil
    parse_fixture.each do |posting|
      refute_empty posting.title
      refute_empty posting.reward
      refute_empty posting.work_format
      refute_empty posting.application_status
      refute_empty posting.deadline_text
      assert_instance_of Array, posting.skills
      assert_instance_of Array, posting.tags
      refute_empty posting.skills, "フィクスチャは全件で使用技術が取れるはず"
      refute_empty posting.description
    end
  end

  # --- 掲載日はUUIDv7の先頭48bit（ミリ秒）から導出する ---

  def test_posted_on_is_derived_from_uuid_v7_for_every_posting
    postings = parse_fixture

    assert(postings.all? { |posting| posting.posted_on.is_a?(Date) },
           "フィクスチャの案件IDは全てUUIDv7なので掲載日が導出できるはず")
  end

  def test_posted_on_is_nil_when_job_id_is_not_uuid_v7
    # バージョンnibbleが4（UUIDv4）。採番方式が変わってもnilになるだけで壊れないことの確認。
    uuid_v4 = "019dab34-506e-45d6-9e10-b3966f0d3d64"

    assert_nil FreelanceJobs::Sources::CoconalaTech.parse_posted_on(uuid_v4, today: TODAY)
  end

  def test_posted_on_is_nil_when_derived_date_is_out_of_range
    assert_nil FreelanceJobs::Sources::CoconalaTech.parse_posted_on(build_uuid_v7(Time.utc(2030, 1, 1)), today: TODAY),
               "未来日は異常値としてnilにするはず"
    assert_nil FreelanceJobs::Sources::CoconalaTech.parse_posted_on(build_uuid_v7(Time.utc(2014, 12, 31)), today: TODAY),
               "サイト開設前の日付は異常値としてnilにするはず"
  end

  # タイムスタンプはUTCとして読む（localtimeで読むと9時間ずれて日付が1日後になる）。
  def test_posted_on_reads_timestamp_as_utc_wall_clock
    uuid = build_uuid_v7(Time.utc(2026, 4, 20, 23, 30, 0))

    assert_equal Date.new(2026, 4, 20), FreelanceJobs::Sources::CoconalaTech.parse_posted_on(uuid, today: TODAY)
  end

  # ★「掲載日が1日ズレている」と言われたときに照合先を間違えないための回帰テスト。
  # 実案件 01a0897c… の詳細ページは createdAt="2026-09-10T04:03:37+09:00"（JSTの壁時計）だが、
  # 同じページのJSON-LD datePosted は同じ瞬間をUTC日付に直した "2026-09-09" になっている。
  # シートに出す掲載日はサイト表示と同じJST日付なので 9月10日が正解。datePosted に合わせて
  # 9時間引く実装に変えてはいけない。
  def test_posted_on_follows_jst_created_at_not_utc_date_of_json_ld
    assert_equal Date.new(2026, 9, 10),
                 FreelanceJobs::Sources::CoconalaTech.parse_posted_on("01a0897c-44c2-7233-98ee-ed0932baf5e8",
                                                                      today: TODAY)
  end

  # --- 単価の単位で work_format が分岐する ---

  def test_work_format_is_monthly_when_reward_is_per_month
    assert_equal "月額制（業務委託）", parse_fixture.first.work_format
  end

  def test_work_format_is_hourly_when_reward_is_per_hour
    body = wrap_html(build_card_html(job_id: build_uuid_v7(Time.utc(2026, 9, 1)), reward: "〜7,680円/時"))
    posting = FreelanceJobs::Sources::CoconalaTech.parse(body, today: TODAY, category_hint: "Ruby").first

    assert_equal "〜7,680円/時", posting.reward
    assert_equal "時間単価制", posting.work_format
  end

  def test_work_format_falls_back_when_reward_has_no_known_unit
    body = wrap_html(build_card_html(job_id: build_uuid_v7(Time.utc(2026, 9, 1)), reward: "応相談"))
    posting = FreelanceJobs::Sources::CoconalaTech.parse(body, today: TODAY, category_hint: "Ruby").first

    assert_equal "応相談", posting.reward
    assert_equal "業務委託（フリーランス）", posting.work_format
  end

  def test_reward_falls_back_to_placeholder_when_price_item_is_empty
    body = wrap_html(build_card_html(job_id: build_uuid_v7(Time.utc(2026, 9, 1)), reward: ""))
    posting = FreelanceJobs::Sources::CoconalaTech.parse(body, today: TODAY, category_hint: "Ruby").first

    assert_equal "要確認", posting.reward
  end

  # --- 必須要素が欠けたカードは黙って除外する ---

  def test_parse_skips_card_without_title_heading
    fragment = build_card_html(job_id: build_uuid_v7(Time.utc(2026, 9, 1))).sub(%r{<h2>.*?</h2>}m, "")

    assert_equal [], FreelanceJobs::Sources::CoconalaTech.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  def test_parse_skips_card_without_summary_list
    fragment = build_card_html(job_id: build_uuid_v7(Time.utc(2026, 9, 1))).sub(%r{<ul>.*?</ul>}m, "")

    assert_equal [], FreelanceJobs::Sources::CoconalaTech.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  # 単価/勤務地/職種の対応付けは出現順に依存するため、項目数が足りないカードは
  # 誤った値を入れるより除外する。
  def test_parse_skips_card_whose_summary_list_has_too_few_items
    fragment = <<~HTML
      <a href="/job-postings/#{build_uuid_v7(Time.utc(2026, 9, 1))}">
        <div class="tw-mb-2"><span>リモート可</span></div>
        <h2>項目が足りない案件</h2>
        <ul><li><div>〜500,000円/月</div></li><li><div>東京都</div></li></ul>
      </a>
    HTML

    assert_equal [], FreelanceJobs::Sources::CoconalaTech.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  # パンくず等の「/job-postings」配下だがUUIDではないリンクは案件として扱わない。
  def test_parse_skips_links_that_are_not_job_detail_pages
    fragment = <<~HTML
      <a href="/job-postings"><h2>求人一覧</h2><ul><li>a</li><li>b</li><li>c</li></ul></a>
      <a href="/job-postings/019dab34-506e-75d6-9e10-b3966f0d3d64/apply"><h2>応募する</h2><ul><li>a</li><li>b</li><li>c</li></ul></a>
    HTML

    assert_equal [], FreelanceJobs::Sources::CoconalaTech.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby")
  end

  # 使用技術ブロックが無いカードでも落とさず、skillsを空配列にして返す。
  def test_parse_allows_card_without_skills_block
    fragment = <<~HTML
      <a href="/job-postings/#{build_uuid_v7(Time.utc(2026, 9, 1))}">
        <div class="tw-mb-2"><span>リモート可</span></div>
        <h2>使用技術欄なし案件</h2>
        <ul>
          <li><div>〜500,000円/月</div></li>
          <li><div>東京都</div></li>
          <li><div>サーバサイドエンジニア(業務委託・フリーランス)</div></li>
        </ul>
      </a>
    HTML
    posting = FreelanceJobs::Sources::CoconalaTech.parse(wrap_html(fragment), today: TODAY, category_hint: "Ruby").first

    refute_nil posting
    assert_equal [], posting.skills
    assert_equal "サーバサイドエンジニア(業務委託・フリーランス) / 東京都", posting.description,
                 "使用技術が空のときは「使用技術:」のラベルごと落とすはず"
  end

  # --- fetch: 通信せずFakeフェッチャーで検証する ---

  # 呼び出されたURLを記録し、あらかじめ積んだbodyを順に返すFakeフェッチャー。
  # bodiesを使い切ったあとは最後のbodyを返し続ける。
  class RecordingFetcher
    def initialize(bodies:)
      @bodies = bodies
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      @bodies[[@requested_urls.size - 1, @bodies.size - 1].min]
    end
  end

  def test_fetch_requests_each_search_target_and_deduplicates_urls_across_skills
    fetcher = RecordingFetcher.new(bodies: [read_fixture(FIXTURE_NAME)])
    source = FreelanceJobs::Sources::CoconalaTech.new(
      fetcher: fetcher, today: TODAY,
      search_targets: [{ skill_id: 4, hint: "Ruby" }, { skill_id: 31, hint: "TypeScript" }]
    )

    postings = source.fetch

    assert_equal ["https://tech.coconala.com/job-postings?skillIds=4&page=1",
                  "https://tech.coconala.com/job-postings?skillIds=31&page=1"],
                 fetcher.requested_urls
    assert_equal FIXTURE_OPEN_POSTING_COUNT, postings.size,
                 "2スキルとも同じbodyなので、URLキーで重複排除されて5件のままのはず"
  end

  def test_fetch_excludes_closed_postings_by_default
    source = FreelanceJobs::Sources::CoconalaTech.new(
      fetcher: RecordingFetcher.new(bodies: [read_fixture(FIXTURE_NAME)]), today: TODAY,
      search_targets: [{ skill_id: 4, hint: "Ruby" }]
    )

    postings = source.fetch

    assert_equal FIXTURE_OPEN_POSTING_COUNT, postings.size
    assert(postings.none? { |posting| posting.application_status == "募集終了" },
           "既定では募集終了案件をシートに載せないはず")
  end

  def test_fetch_includes_closed_postings_when_include_closed_is_true
    source = FreelanceJobs::Sources::CoconalaTech.new(
      fetcher: RecordingFetcher.new(bodies: [read_fixture(FIXTURE_NAME)]), today: TODAY,
      search_targets: [{ skill_id: 4, hint: "Ruby" }], include_closed: true
    )

    assert_equal FIXTURE_POSTING_COUNT, source.fetch.size
  end

  # 募集中→募集終了の並び順を利用した打ち切り。募集終了が1件でも出たページで止める。
  def test_fetch_stops_paging_when_page_contains_a_closed_posting
    fetcher = RecordingFetcher.new(bodies: [read_fixture(FIXTURE_NAME)])
    source = FreelanceJobs::Sources::CoconalaTech.new(
      fetcher: fetcher, today: TODAY,
      search_targets: [{ skill_id: 4, hint: "Ruby" }], max_pages: 3
    )

    source.fetch

    assert_equal 1, fetcher.requested_urls.size,
                 "1ページ目に募集終了が含まれるので、2ページ目以降は取りに行かないはず"
  end

  # 募集中しか無いページならmax_pagesまで進む（募集中案件が30件を超えた場合の保険）。
  def test_fetch_continues_to_next_page_while_every_posting_is_open
    open_page_body = wrap_html(build_card_html(job_id: build_uuid_v7(Time.utc(2026, 9, 1)), title: "募集中案件A") +
                               build_card_html(job_id: build_uuid_v7(Time.utc(2026, 9, 2)), title: "募集中案件B"))
    fetcher = RecordingFetcher.new(bodies: [open_page_body])
    source = FreelanceJobs::Sources::CoconalaTech.new(
      fetcher: fetcher, today: TODAY,
      search_targets: [{ skill_id: 4, hint: "Ruby" }], max_pages: 2
    )

    postings = source.fetch

    assert_equal ["https://tech.coconala.com/job-postings?skillIds=4&page=1",
                  "https://tech.coconala.com/job-postings?skillIds=4&page=2"],
                 fetcher.requested_urls
    assert_equal 2, postings.size, "2ページとも同じ2件なので重複排除されるはず"
  end

  def test_fetch_stops_paging_when_page_has_no_cards
    fetcher = RecordingFetcher.new(bodies: [wrap_html("<p>該当する案件がありません</p>")])
    source = FreelanceJobs::Sources::CoconalaTech.new(
      fetcher: fetcher, today: TODAY,
      search_targets: [{ skill_id: 4, hint: "Ruby" }], max_pages: 3
    )

    assert_equal [], source.fetch
    assert_equal 1, fetcher.requested_urls.size, "カードが0件のページで打ち切るはず"
  end

  # --- 既定の取得対象 ---

  def test_default_search_targets_cover_ruby_typescript_and_react
    hints = FreelanceJobs::Sources::CoconalaTech::DEFAULT_SEARCH_TARGETS.map { |target| target[:hint] }

    assert_equal ["Ruby", "TypeScript", "React"], hints
    assert_equal [4, 31, 64],
                 FreelanceJobs::Sources::CoconalaTech::DEFAULT_SEARCH_TARGETS.map { |target| target[:skill_id] },
                 "実HTTPで確定したskillIds（Ruby=4 / TypeScript=31 / React=64）"
  end

  # 既存のココナラ（coconala.com/requests の公開依頼）とは別サービスなので掲載サイト名も別。
  def test_site_name_differs_from_coconala_crowdsourcing_source
    refute_equal FreelanceJobs::Sources::Coconala::SITE_NAME, FreelanceJobs::Sources::CoconalaTech::SITE_NAME
    assert_equal "ココナラテック", FreelanceJobs::Sources::CoconalaTech::SITE_NAME
  end

  # --- Profile::ENGINEER にCoconalaTechが含まれる（BEGINNERには含まれない） ---

  def test_engineer_profile_includes_coconala_tech_source
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_includes source_classes, FreelanceJobs::Sources::CoconalaTech
  end

  def test_beginner_profile_does_not_include_coconala_tech_source
    source_classes = FreelanceJobs::Profile::BEGINNER.source_specs.map(&:first)

    refute_includes source_classes, FreelanceJobs::Sources::CoconalaTech
  end

  private

  # 一覧カード1件分のHTML片（実データのDOM構造を模したもの）。
  # バッジ / h2 / ul>li×3（単価・勤務地・職種） / 使用技術div / 募集本文p の並びを再現する。
  def build_card_html(job_id:, title: "テスト案件", reward: "〜800,000円/月", work_location: "東京都",
                      occupation: "サーバサイドエンジニア(業務委託・フリーランス)",
                      badges: ["リモート可"], skills: ["Ruby", "Rails"], summary: "【作業内容】テストの説明文です。")
    badge_html = badges.map { |badge| "<span>#{badge}</span>" }.join
    # 実データと同じく、区切りの「・」は最後以外のspanの末尾に付く。
    skill_html = skills.each_with_index.map do |skill, index|
      "<span>#{skill}#{index == skills.size - 1 ? "" : "・"}</span>"
    end.join

    <<~HTML
      <a href="/job-postings/#{job_id}">
        <div class="tw-mb-2">#{badge_html}</div>
        <h2>#{title}</h2>
        <ul>
          <li><div class="tw-flex-shrink-0"><span><svg></svg></span></div><div>#{reward}</div></li>
          <li><div class="tw-flex-shrink-0"><span><svg></svg></span></div><div>#{work_location}</div></li>
          <li><div class="tw-flex-shrink-0"><span><svg></svg></span></div><div>#{occupation}</div></li>
        </ul>
        <div><div class="tw-flex-shrink-0"><span><svg></svg></span></div><div>#{skill_html}</div></div>
        <div><p>#{summary}</p></div>
      </a>
    HTML
  end

  # 指定時刻（UTCの壁時計として扱う）を先頭48bitに持つUUIDv7を組み立てる。
  # 3番目のブロック先頭がバージョンnibbleで、ここが "7" のときだけ掲載日として採用される。
  def build_uuid_v7(posted_time)
    timestamp_hex = format("%012x", (posted_time.to_i * 1000))
    "#{timestamp_hex[0, 8]}-#{timestamp_hex[8, 4]}-7abc-9def-0123456789ab"
  end
end
