# frozen_string_literal: true
# test/services/freelance_jobs/hokkaido_classifier_test.rb

require_relative "../../support/freelance_jobs_loader"
require_relative "../../support/freelance_jobs_test_helpers"
require "date"

class FreelanceJobsHokkaidoClassifierTest < Minitest::Test
  include FreelanceJobsTestHelpers if defined?(FreelanceJobsTestHelpers)

  TODAY = Date.new(2026, 10, 8)
  FIXTURE_DIRECTORY = File.expand_path("../../fixtures/files/freelance_jobs", __dir__)
  ONSITE_MEMO_PREFIX = "出社"
  HYBRID_MEMO_PREFIX = "ハイブリッド（リモート併用）"
  FULL_REMOTE_MEMO_PREFIX = "フルリモート"

  def build_posting(title: "", description: "", category_hint: nil, skills: [], tags: [],
                    reward: "要相談", work_format: "業務委託")
    FreelanceJobs::JobPosting.new(
      site: "テスト", url: "https://example.com/jobs/1", title: title, description: description,
      category_hint: category_hint, reward: reward, work_format: work_format,
      application_status: "-", deadline_text: "-", deadline_on: nil,
      skills: skills, client: "", tags: tags, posted_on: nil
    )
  end

  def classify(posting)
    FreelanceJobs::HokkaidoClassifier.classify(posting, today: TODAY)
  end

  # === 除外条件 ===

  def test_non_dev_posting_is_excluded
    posting = build_posting(title: "Ruby記事作成ライター募集", description: "勤務地: 北海道 札幌市")

    assert_nil classify(posting).category
  end

  def test_tokyo_posting_without_hokkaido_is_excluded
    posting = build_posting(title: "Rubyエンジニア募集", description: "勤務地: 東京都 渋谷区 / 週5日出社")

    assert_nil classify(posting).category
  end

  def test_full_remote_hokkaido_posting_is_excluded
    posting = build_posting(title: "Javaエンジニア（北海道在住）", description: "勤務地: 北海道 札幌市 / フルリモート")

    assert_nil classify(posting).category
  end

  # 2026-10-08 社長要求「フルリモートのRubyも入れて」: Ruby だけはフルリモートでも残す
  def test_full_remote_ruby_posting_remains_with_full_remote_memo
    posting = build_posting(title: "Rubyエンジニア", description: "勤務地: 北海道 札幌市 / 勤務形態: フルリモート")
    result = classify(posting)

    assert_equal "Ruby", result.category
    assert result.memo.start_with?("フルリモート（北海道 札幌市）"), result.memo
  end

  def test_full_remote_java_posting_is_excluded
    posting = build_posting(title: "Javaエンジニア", description: "勤務地: 北海道 札幌市 / 勤務形態: フルリモート")

    assert_nil classify(posting).category
  end

  def test_posting_without_engineer_words_is_excluded
    posting = build_posting(title: "受付スタッフ募集", description: "勤務地: 北海道 札幌市 の事務所で受付対応")

    assert_nil classify(posting).category
  end

  # === 勤務形態の振り分け ===

  def test_full_remote_with_weekly_onsite_remains_as_hybrid
    posting = build_posting(title: "Rubyエンジニア", description: "勤務地: 北海道 札幌市 / フルリモート（週1日出社）")
    result = classify(posting)

    assert_equal "Ruby", result.category
    assert result.memo.start_with?(HYBRID_MEMO_PREFIX), result.memo
  end

  def test_onsite_posting_has_onsite_memo_with_location
    posting = build_posting(title: "Ruby on Rails開発", description: "Railsアプリの開発。勤務地: 北海道 札幌市")
    result = classify(posting)

    assert_equal "Ruby", result.category
    assert result.memo.start_with?(ONSITE_MEMO_PREFIX), result.memo
    refute result.memo.start_with?(HYBRID_MEMO_PREFIX), result.memo
    assert_includes result.memo, "北海道 札幌市"
  end

  def test_remote_allowed_is_hybrid
    posting = build_posting(title: "Rubyエンジニア", description: "勤務地: 北海道 札幌市 / リモート可")

    assert classify(posting).memo.start_with?(HYBRID_MEMO_PREFIX)
  end

  def test_partial_remote_is_hybrid
    posting = build_posting(title: "Rubyエンジニア", description: "勤務地: 北海道 札幌市 / 一部リモート")

    assert classify(posting).memo.start_with?(HYBRID_MEMO_PREFIX)
  end

  def test_basically_remote_with_monthly_onsite_is_hybrid
    posting = build_posting(title: "Rubyエンジニア", description: "勤務地: 北海道 札幌市 / 基本リモート、月1出社")
    result = classify(posting)

    refute_nil result.category
    assert result.memo.start_with?("ハイブリッド（リモート併用）（北海道 札幌市）"), result.memo
  end

  def test_business_trip_to_hokkaido_without_location_label_is_excluded
    posting = build_posting(title: "Rubyエンジニア", description: "東京本社。北海道出張あり。週5日出社")

    assert_nil classify(posting).category
  end

  def test_sapporo_office_without_location_label_remains_as_onsite
    posting = build_posting(title: "Rubyエンジニア", description: "札幌オフィス勤務")
    result = classify(posting)

    refute_nil result.category
    assert result.memo.start_with?(ONSITE_MEMO_PREFIX), result.memo
  end

  # === カテゴリ ===

  def test_non_target_technology_engineer_posting_is_other
    posting = build_posting(title: "Javaエンジニア募集", description: "業務システムの開発。勤務地: 北海道 札幌市")
    result = classify(posting)

    assert_equal "その他", result.category
    assert_equal "Java", result.skills_text
  end

  def test_other_category_without_detected_skills_falls_back_to_posting_skills
    posting = build_posting(title: "システムエンジニア募集", skills: %w[COBOL Oracle], description: "勤務地: 北海道 札幌市")
    result = classify(posting)

    assert_equal "その他", result.category
    assert_equal "COBOL / Oracle", result.skills_text
  end

  def test_other_category_without_any_skills_has_empty_skills_text
    posting = build_posting(title: "システムエンジニア募集", description: "勤務地: 北海道 札幌市")
    result = classify(posting)

    assert_equal "その他", result.category
    assert_equal "", result.skills_text
  end

  # === 否定・条件付きの出社表現 ===

  def test_onsite_not_required_hokkaido_posting_is_excluded
    posting = build_posting(title: "Javaエンジニア", description: "勤務地: 北海道 札幌市 / フルリモート・出社不要")

    assert_nil classify(posting).category
  end

  def test_resident_not_required_hokkaido_posting_is_excluded
    posting = build_posting(title: "Javaエンジニア", description: "勤務地: 北海道 札幌市 / フルリモート・常駐なし")

    assert_nil classify(posting).category
  end

  def test_full_remote_after_onsite_is_not_treated_as_full_remote
    posting = build_posting(title: "Rubyエンジニア", description: "勤務地: 北海道 札幌市 / 出社後フルリモートが可能")
    result = classify(posting)

    assert_equal "Ruby", result.category
    assert result.memo.start_with?(ONSITE_MEMO_PREFIX), result.memo
    refute result.memo.start_with?(HYBRID_MEMO_PREFIX), result.memo
  end

  def test_full_remote_case_section_is_ignored_when_judging_work_style
    posting = build_posting(
      title: "Javaエンジニア",
      description: "勤務地: 北海道 札幌市 / フルリモート 【フルリモート案件の場合】必要に応じて都内の出社をお願いします【その他】備考"
    )

    assert_nil classify(posting).category
  end

  def test_full_remote_section_without_closing_bracket_keeps_trailing_full_remote_label
    posting = build_posting(
      title: "Javaエンジニア",
      description: "【フルリモート案件の場合】必要に応じて都内の出社あり / 勤務地: 北海道 札幌市 / 勤務形態: フルリモート"
    )

    assert_nil classify(posting).category
  end

  def test_fullwidth_digit_weekly_onsite_is_hybrid
    posting = build_posting(title: "Rubyエンジニア", description: "勤務地: 北海道 札幌市 / フルリモート（週５日出社）")
    result = classify(posting)

    assert_equal "Ruby", result.category
    assert result.memo.start_with?(HYBRID_MEMO_PREFIX), result.memo
  end

  def test_nil_description_skills_tags_do_not_raise
    posting = build_posting(title: "Rubyエンジニア", description: nil, skills: nil, tags: nil)

    assert_nil classify(posting).category
  end

  # === 北海道判定は「勤務地」の値を優先 ===

  def test_last_work_location_label_wins_over_body_mention
    posting = build_posting(
      title: "Rubyエンジニア",
      description: "本文。勤務地：都内 の案件も紹介可 / 勤務地: 北海道 札幌駅"
    )
    result = classify(posting)

    assert_equal "Ruby", result.category
    assert_includes result.memo, "北海道 札幌駅"
    refute_includes result.memo, "都内"
  end

  def test_additional_hokkaido_city_remains
    posting = build_posting(title: "Rubyエンジニア", description: "勤務地: 北広島市 / 週5日出社")

    assert_equal "Ruby", classify(posting).category
  end

  def test_date_city_alone_is_not_hokkaido
    posting = build_posting(title: "Rubyエンジニア", description: "勤務地: 伊達市 / 週5日出社")

    assert_nil classify(posting).category
  end

  def test_tokyo_work_location_is_excluded_even_if_body_mentions_hokkaido
    posting = build_posting(title: "Rubyエンジニア", description: "北海道・東北地域の案件も紹介可。勤務地: 東京都 渋谷区 / 週5日出社")

    assert_nil classify(posting).category
  end

  def test_major_hokkaido_city_work_location_remains
    posting = build_posting(title: "Rubyエンジニア", description: "勤務地: 旭川市 / 週5日出社")
    result = classify(posting)

    assert_equal "Ruby", result.category
    assert_includes result.memo, "旭川市"
  end

  def test_bare_chitose_is_not_hokkaido
    posting = build_posting(title: "Rubyエンジニア", description: "勤務地: 世田谷区 千歳船橋駅 / 週5日出社")

    assert_nil classify(posting).category
  end

  # === EngineerClassifier との整合 ===

  def test_difficulty_recommend_skills_text_match_engineer_classifier
    posting = build_posting(
      title: "Ruby on Rails開発リード", reward: "〜800,000円／月", skills: %w[Ruby Rails AWS],
      description: "経験5年以上。要件定義から。勤務地: 北海道 札幌市 / リモート可 / 長期"
    )
    expected = FreelanceJobs::EngineerClassifier.classify(posting, today: TODAY)
    actual = classify(posting)

    assert_equal "Ruby", actual.category
    assert_equal expected.difficulty, actual.difficulty
    assert_equal expected.recommend, actual.recommend
    assert_equal expected.skills_text, actual.skills_text
  end

  # === 実フィクスチャ結合 ===

  def fixture_postings(file_name, source_class)
    body = File.read(File.join(FIXTURE_DIRECTORY, file_name))
    source_class.parse(body, today: TODAY)
  end

  def assert_fixture_classification(file_name, source_class, expected_kept_count)
    postings = fixture_postings(file_name, source_class)
    refute_empty postings, "#{file_name} がパースできていない"

    kept = postings.filter_map do |posting|
      result = classify(posting)
      [posting, result] unless result.category.nil?
    end
    assert_equal expected_kept_count, kept.size, "#{file_name}: 残件数が実測値と違う"

    kept.each do |posting, result|
      searched_text = [posting.title, posting.description, Array(posting.tags).join(" ")].join(" ")
      assert_match(FreelanceJobs::HokkaidoClassifier::HOKKAIDO_RE, searched_text, posting.url)
      assert [ONSITE_MEMO_PREFIX, HYBRID_MEMO_PREFIX, FULL_REMOTE_MEMO_PREFIX].any? { |prefix| result.memo.start_with?(prefix) },
             "#{posting.url}: memo=#{result.memo}"
    end
  end

  def test_freelance_board_hokkaido_fixture
    assert_fixture_classification("freelance_board_hokkaido.html", FreelanceJobs::Sources::FreelanceBoard, 19)
  end

  def test_freelance_hub_hokkaido_fixture
    assert_fixture_classification("freelance_hub_hokkaido.html", FreelanceJobs::Sources::FreelanceHub, 21)

    # 勤務地が東京都の推薦枠（実測7件）が本文の「北海道」に釣られて残らないこと。
    kept_locations = fixture_postings("freelance_hub_hokkaido.html", FreelanceJobs::Sources::FreelanceHub).filter_map do |posting|
      posting.description[FreelanceJobs::HokkaidoClassifier::LOCATION_VALUE_RE, 1] unless classify(posting).category.nil?
    end
    assert_empty kept_locations.grep(/東京都/)
  end

  def test_pe_bank_hokkaido_fixture
    assert_fixture_classification("pe_bank_hokkaido.html", FreelanceJobs::Sources::PeBank, 47)
  end
end
