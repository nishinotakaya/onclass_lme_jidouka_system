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
end
