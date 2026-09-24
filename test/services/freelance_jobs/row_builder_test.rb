# frozen_string_literal: true
# test/services/freelance_jobs/row_builder_test.rb

require_relative "../../support/freelance_jobs_loader"
require "date"

class FreelanceJobsRowBuilderTest < Minitest::Test
  ClassificationDouble = Struct.new(:recommend, :category, :difficulty, :memo, :skills_text, keyword_init: true)

  NOW = Time.new(2026, 9, 4, 8, 30, 0, "+09:00")

  def build_posting(title: "テスト案件", description: "説明文です。", reward: "10,000円", work_format: "固定報酬制",
                     application_status: "応募 1件", deadline_text: "2026-09-10", url: "https://example.com/jobs/1",
                     site: "CrowdWorks")
    FreelanceJobs::JobPosting.new(
      site: site, url: url, title: title, description: description, category_hint: nil, reward: reward,
      work_format: work_format, application_status: application_status, deadline_text: deadline_text,
      deadline_on: nil, skills: [], client: "", tags: [], posted_on: nil
    )
  end

  def build_classification(recommend: "🌟", category: "HTML/CSS", difficulty: "★☆☆ 未経験OK", memo: "メモ",
                            skills_text: "HTML/CSSの基本")
    ClassificationDouble.new(recommend: recommend, category: category, difficulty: difficulty, memo: memo,
                              skills_text: skills_text)
  end

  # --- HEADER ---
  # AC-03: 行モデルに「追加日」列(index 15)を足して16列にする。

  def test_header_has_16_columns_including_added_on_as_the_last_column
    expected = ["🌟おすすめ", "No.", "分類", "案件名", "掲載サイト", "案件URL", "難易度", "内容（要約）",
                "必要スキル", "報酬", "形式", "応募状況（応募数 / 契約状況）", "締切", "一言メモ（おすすめ理由・注意点）",
                "取得日時", "追加日"]

    assert_equal expected, FreelanceJobs::RowBuilder::HEADER
    assert_equal 16, FreelanceJobs::RowBuilder::HEADER.size
    assert_equal "追加日", FreelanceJobs::RowBuilder::HEADER.last
  end

  # --- build: 基本のマッピング ---

  def test_build_maps_posting_and_classification_fields_into_16_columns
    posting = build_posting
    classification = build_classification
    row = FreelanceJobs::RowBuilder.build(posting, classification, now: NOW)

    assert_equal 16, row.size
    assert_equal "🌟", row[0]
    assert_equal 0, row[1] # No.はSheetMergerで採番するため0
    assert_equal "HTML/CSS", row[2]
    assert_equal "テスト案件", row[3]
    assert_equal "CrowdWorks", row[4]
    assert_equal "https://example.com/jobs/1", row[5]
    assert_equal "★☆☆ 未経験OK", row[6]
    assert_equal "説明文です。", row[7]
    assert_equal "HTML/CSSの基本", row[8]
    assert_equal "10,000円", row[9]
    assert_equal "固定報酬制", row[10]
    assert_equal "応募 1件", row[11]
    assert_equal "2026-09-10", row[12]
    assert_equal "メモ", row[13]
    assert_equal "2026-09-04 08:30", row[14]
    assert_equal "2026-09-04", row[15]
  end

  # 追加日(index15)はnowの日付部分のみ（取得日時のような時刻は含まない）。
  def test_build_added_on_column_uses_date_only_format_derived_from_now
    posting = build_posting
    classification = build_classification
    now = Time.new(2026, 12, 31, 23, 59, 0, "+09:00")
    row = FreelanceJobs::RowBuilder.build(posting, classification, now: now)

    assert_equal "2026-12-31 23:59", row[14]
    assert_equal "2026-12-31", row[15]
  end

  def test_build_recommend_column_is_stringified
    posting = build_posting
    classification = build_classification(recommend: nil)
    row = FreelanceJobs::RowBuilder.build(posting, classification, now: NOW)

    assert_equal "", row[0]
  end

  # --- summarize (内容の要約: 160文字+…) ---

  def test_summarize_truncates_description_over_160_chars_with_ellipsis
    posting = build_posting(description: "あ" * 200)
    text = FreelanceJobs::RowBuilder.summarize(posting)

    assert_equal 161, text.length
    assert_equal "#{"あ" * 160}…", text
  end

  def test_summarize_keeps_description_at_exactly_160_chars_unchanged
    posting = build_posting(description: "あ" * 160)
    text = FreelanceJobs::RowBuilder.summarize(posting)

    assert_equal "あ" * 160, text
    refute_includes text, "…"
  end

  def test_summarize_falls_back_to_title_when_description_blank
    posting = build_posting(description: "   ", title: "タイトルだけの案件")
    text = FreelanceJobs::RowBuilder.summarize(posting)

    assert_equal "タイトルだけの案件", text
  end

  # --- ラウンド2 C5: 各列の文字数上限（超過は"…"を付けて切る） ---

  def test_title_is_truncated_at_120_chars_with_ellipsis
    posting = build_posting(title: "あ" * 130)
    row = FreelanceJobs::RowBuilder.build(posting, build_classification, now: NOW)

    assert_equal "#{"あ" * 120}…", row[3]
  end

  def test_title_at_exactly_120_chars_is_not_truncated
    posting = build_posting(title: "あ" * 120)
    row = FreelanceJobs::RowBuilder.build(posting, build_classification, now: NOW)

    assert_equal "あ" * 120, row[3]
  end

  def test_reward_is_truncated_at_60_chars_with_ellipsis
    posting = build_posting(reward: "1" * 70)
    row = FreelanceJobs::RowBuilder.build(posting, build_classification, now: NOW)

    assert_equal "#{"1" * 60}…", row[9]
  end

  def test_application_status_is_truncated_at_40_chars_with_ellipsis
    posting = build_posting(application_status: "あ" * 50)
    row = FreelanceJobs::RowBuilder.build(posting, build_classification, now: NOW)

    assert_equal "#{"あ" * 40}…", row[11]
  end

  def test_deadline_text_is_truncated_at_40_chars_with_ellipsis
    posting = build_posting(deadline_text: "あ" * 50)
    row = FreelanceJobs::RowBuilder.build(posting, build_classification, now: NOW)

    assert_equal "#{"あ" * 40}…", row[12]
  end

  def test_skills_text_is_truncated_at_120_chars_with_ellipsis
    classification = build_classification(skills_text: "あ" * 130)
    row = FreelanceJobs::RowBuilder.build(build_posting, classification, now: NOW)

    assert_equal "#{"あ" * 120}…", row[8]
  end

  def test_memo_is_truncated_at_140_chars_with_ellipsis
    classification = build_classification(memo: "あ" * 150)
    row = FreelanceJobs::RowBuilder.build(build_posting, classification, now: NOW)

    assert_equal "#{"あ" * 140}…", row[13]
    assert_equal 141, row[13].length
  end

  def test_memo_at_exactly_140_chars_is_not_truncated
    classification = build_classification(memo: "あ" * 140)
    row = FreelanceJobs::RowBuilder.build(build_posting, classification, now: NOW)

    assert_equal "あ" * 140, row[13]
  end

  def test_work_format_and_category_are_not_truncated_by_row_builder
    posting = build_posting
    classification = build_classification(category: "×" * 500)
    row = FreelanceJobs::RowBuilder.build(posting, classification, now: NOW)

    assert_equal "×" * 500, row[2], "分類(C列)はRowBuilderの文字数制限の対象外"
  end
end
