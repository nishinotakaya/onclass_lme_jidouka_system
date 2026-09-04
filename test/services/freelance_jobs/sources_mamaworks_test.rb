# frozen_string_literal: true
# test/services/freelance_jobs/sources_mamaworks_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsSourcesMamaworksTest < Minitest::Test
  include FreelanceJobsTestHelpers

  TODAY = Date.new(2026, 9, 4)

  def test_parse_fixture_returns_expected_count_and_first_fields
    body = read_fixture("mama_jobs_noquery.html")
    postings = FreelanceJobs::Sources::Mamaworks.parse(body, today: TODAY)

    # fixtureの先頭8枚(実カード4枚+空要素4枚)のうちキーワードに一致するのは2枚だけ。
    assert_equal 2, postings.size

    first = postings.first
    assert_equal "https://mamaworks.jp/job/38626", first.url
    assert_equal "【フルリモート】経理・税務サポート（業務委託）｜税理士事務所経験を活かして活躍しませんか？", first.title
    assert_equal "時間単価：1,500円（税込）～2,000円（税込） ※ご経験やスキルに応じて変更", first.reward
    assert_equal "業務委託（求人）", first.work_format
    assert_equal "ママワークス", first.site
    assert_equal "-", first.application_status
    assert_nil first.deadline_on
    assert_equal "株式会社ファーストアソシエイツ", first.client
  end

  # --- 除外ルール: キーワードに一致しないカードは除外される ---

  def test_parse_excludes_cards_not_matching_keyword_filter
    body = read_fixture("mama_jobs_noquery.html")
    postings = FreelanceJobs::Sources::Mamaworks.parse(body, today: TODAY)

    refute(postings.any? { |posting| posting.url == "https://mamaworks.jp/job/38093" },
           "job/38093（経理アシスタント）はキーワード不一致のため除外されるはず")
  end

  def test_parse_skips_cards_without_a_job_link
    fragment = <<~HTML
      <li class="p-recruit-index__result-box">
        <h2 class="p-recruit-index__result-ttl">リンクが無いHTMLコーディング案件</h2>
      </li>
    HTML
    postings = FreelanceJobs::Sources::Mamaworks.parse(wrap_html(fragment), today: TODAY)

    assert_equal [], postings
  end

  def test_reward_falls_back_to_placeholder_when_no_reward_paragraph_found
    fragment = <<~HTML
      <li class="p-recruit-index__result-box">
        <a href="/job/99999">リンク</a>
        <h2 class="p-recruit-index__result-ttl">HTMLコーディングのお仕事</h2>
        <p class="p-recruit-index__result-description">未経験者歓迎です</p>
        <section class="p-recruit-index__result-detail-box">
          <p>在宅ワーク</p>
        </section>
      </li>
    HTML
    posting = FreelanceJobs::Sources::Mamaworks.parse(wrap_html(fragment), today: TODAY).first

    refute_nil posting
    assert_equal "求人ページ参照", posting.reward
  end

  def test_url_is_absolutized_when_href_is_relative
    fragment = <<~HTML
      <li class="p-recruit-index__result-box">
        <a href="/job/12345">リンク</a>
        <h2 class="p-recruit-index__result-ttl">Excelでのデータ入力（相対パス確認）</h2>
      </li>
    HTML
    posting = FreelanceJobs::Sources::Mamaworks.parse(wrap_html(fragment), today: TODAY).first

    assert_equal "https://mamaworks.jp/job/12345", posting.url
  end

  def test_parse_returns_empty_array_when_no_cards_present
    postings = FreelanceJobs::Sources::Mamaworks.parse("<html><body>該当なし</body></html>", today: TODAY)

    assert_equal [], postings
  end

  # === D2: engineeringカテゴリfixture + keyword_filter オプション ===

  def test_parse_engineering_fixture_with_default_keyword_filter_matches_20_cards
    body = read_fixture("mama_engineering.html")
    postings = FreelanceJobs::Sources::Mamaworks.parse(body, today: TODAY)

    assert_equal 20, postings.size, "既定のKEYWORD_FILTER_RE（未経験向けキーワード）は変更されていない想定"
  end

  def test_parse_engineering_fixture_with_nil_keyword_filter_returns_all_cards_with_valid_links
    body = read_fixture("mama_engineering.html")

    with_filter = FreelanceJobs::Sources::Mamaworks.parse(body, today: TODAY)
    without_filter = FreelanceJobs::Sources::Mamaworks.parse(body, today: TODAY, keyword_filter: nil)

    assert_operator without_filter.size, :>, with_filter.size,
                     "keyword_filter: nilは事前フィルタを行わないため既定より多くの案件が残る想定"
    assert_equal 38, without_filter.size
  end

  # === D2: initialize のオプション（category_paths / keyword_filter）が fetch に反映される ===

  # urlをそのままキーに本文を返すFakeフェッチャー（呼び出されたURLを記録する）。
  class RecordingFetcher
    def initialize(body_by_url:)
      @body_by_url = body_by_url
      @requested_urls = []
    end

    attr_reader :requested_urls

    def get(url, headers: {})
      @requested_urls << url
      @body_by_url.fetch(url) { raise "no fixture stubbed for #{url}" }
    end
  end

  def test_fetch_with_custom_category_paths_requests_only_those_paths
    expected_url = "https://mamaworks.jp/jobs/engineering"
    fetcher = RecordingFetcher.new(body_by_url: { expected_url => read_fixture("mama_engineering.html") })
    source = FreelanceJobs::Sources::Mamaworks.new(fetcher: fetcher, today: TODAY, category_paths: ["/jobs/engineering"])

    source.fetch

    assert_equal [expected_url], fetcher.requested_urls, "category_pathsを1件に絞ればそのURLだけがリクエストされる想定"
  end

  def test_fetch_with_keyword_filter_nil_skips_pre_filtering_and_leaves_it_to_the_classifier
    url = "https://mamaworks.jp/jobs/engineering"
    fetcher = RecordingFetcher.new(body_by_url: { url => read_fixture("mama_engineering.html") })
    source = FreelanceJobs::Sources::Mamaworks.new(fetcher: fetcher, today: TODAY,
                                                     category_paths: ["/jobs/engineering"], keyword_filter: nil)

    postings = source.fetch

    assert_equal 38, postings.size, "keyword_filter: nilをinitializeで渡した場合もfetch内のparseへ引き継がれる想定"
  end

  def test_fetch_with_default_options_behaves_like_existing_behavior
    url = "https://mamaworks.jp/jobs/engineering"
    fetcher = RecordingFetcher.new(body_by_url: { url => read_fixture("mama_engineering.html") })
    source = FreelanceJobs::Sources::Mamaworks.new(fetcher: fetcher, today: TODAY, category_paths: ["/jobs/engineering"])

    postings = source.fetch

    assert_equal 20, postings.size, "keyword_filterを省略すれば既定のKEYWORD_FILTER_REで従来通り絞り込まれる想定"
  end
end
