# frozen_string_literal: true
# test/services/freelance_jobs/engineer_classifier_test.rb

require_relative "../../support/freelance_jobs_loader"
require "date"

class FreelanceJobsEngineerClassifierTest < Minitest::Test
  TODAY = Date.new(2026, 9, 4)

  def build_posting(site: "CrowdWorks", title: "", description: "", category_hint: nil, skills: [],
                     reward: "要相談", application_status: "-", work_format: "固定報酬制")
    FreelanceJobs::JobPosting.new(
      site: site, url: "https://example.com/jobs/1", title: title, description: description,
      category_hint: category_hint, reward: reward, work_format: work_format,
      application_status: application_status, deadline_text: "-", deadline_on: nil,
      skills: skills, client: "", tags: [], posted_on: nil
    )
  end

  def classify(posting)
    FreelanceJobs::EngineerClassifier.classify(posting, today: TODAY)
  end

  # === classifyはtoday:キーワードを要求する（Classifierと同じインターフェース） ===

  def test_classify_requires_today_keyword
    assert_raises(ArgumentError) { FreelanceJobs::EngineerClassifier.classify(build_posting(title: "Ruby")) }
  end

  # === 技術検出の境界ケース（正規表現レベル） ===

  def test_react_re_does_not_match_domain_like_react_corp_com
    refute FreelanceJobs::EngineerClassifier::REACT_RE.match?("react-corp.com")
  end

  def test_react_re_matches_react_native_with_space
    assert FreelanceJobs::EngineerClassifier::REACT_RE.match?("React Native")
  end

  def test_react_re_matches_react_native_without_space
    assert FreelanceJobs::EngineerClassifier::REACT_RE.match?("ReactNative")
  end

  def test_react_re_matches_react_native_with_hyphen
    assert FreelanceJobs::EngineerClassifier::REACT_RE.match?("React-Native")
  end

  def test_react_re_does_not_match_reactive
    refute FreelanceJobs::EngineerClassifier::REACT_RE.match?("Reactiveなアーキテクチャ")
  end

  def test_java_additional_skill_regex_does_not_match_javascript
    refute FreelanceJobs::EngineerClassifier::ADDITIONAL_SKILLS["Java"].match?("JavaScript")
  end

  def test_java_additional_skill_regex_matches_plain_java
    assert FreelanceJobs::EngineerClassifier::ADDITIONAL_SKILLS["Java"].match?("Javaでの開発経験")
  end

  def test_go_additional_skill_regex_matches_golang
    assert FreelanceJobs::EngineerClassifier::ADDITIONAL_SKILLS["Go"].match?("Golang")
  end

  def test_remote_re_does_not_match_remote_not_allowed
    refute FreelanceJobs::EngineerClassifier::REMOTE_RE.match?("リモート不可、常駐必須")
  end

  def test_remote_re_matches_remote_allowed
    assert FreelanceJobs::EngineerClassifier::REMOTE_RE.match?("リモート可、応相談")
  end

  # === 技術検出の境界ケース（classify経由の統合確認） ===

  def test_category_nil_when_only_a_domain_name_looking_like_react_is_present
    posting = build_posting(title: "react-corp.com案件", description: "詳細は react-corp.com をご覧ください")
    result = classify(posting)

    assert_nil result.category
  end

  def test_category_react_for_react_native_title
    posting = build_posting(title: "React Native アプリ開発エンジニア募集")
    result = classify(posting)

    assert_equal "React", result.category
  end

  def test_category_react_for_reactnative_without_separator
    posting = build_posting(title: "ReactNativeアプリ開発エンジニア募集")
    result = classify(posting)

    assert_equal "React", result.category
  end

  def test_category_react_for_react_hyphen_native
    posting = build_posting(title: "React-Nativeアプリ開発エンジニア募集")
    result = classify(posting)

    assert_equal "React", result.category
  end

  def test_category_nil_for_reactive_keyword_alone
    posting = build_posting(title: "Reactiveなシステム設計案件、エンジニア募集")
    result = classify(posting)

    assert_nil result.category
  end

  # === NON_DEV除外（技術キーワードがあっても除外される） ===

  def test_category_nil_for_sales_posting_even_with_ruby_in_title
    posting = build_posting(title: "Ruby開発会社の法人営業スタッフ募集")
    result = classify(posting)

    assert_nil result.category
  end

  # 「営業」が開発対象システムの業務ドメインを指す場合は除外しない（実案件での取りこぼし防止）。
  def test_category_is_detected_for_sales_support_system_development
    posting = build_posting(title: "【Ruby】営業支援系サブシステムの開発")
    result = classify(posting)

    assert_equal "Ruby", result.category
  end

  def test_category_is_detected_for_sales_promotion_saas_development
    posting = build_posting(title: "営業促進を目的としたSaaSのTypeScript開発")
    result = classify(posting)

    assert_equal "TypeScript", result.category
  end

  def test_category_is_detected_for_sales_management_system_development
    posting = build_posting(title: "営業管理システムのReactフロントエンド開発")
    result = classify(posting)

    assert_equal "React", result.category
  end

  # 営業そのものを請け負う募集は従来どおり除外し続ける（上の緩和で漏れないことの固定）。
  def test_category_nil_for_sales_agency_posting
    posting = build_posting(title: "Rubyエンジニア向けサービスの営業代行")
    result = classify(posting)

    assert_nil result.category
  end

  def test_category_nil_for_interview_article_posting_even_with_ruby_in_title
    posting = build_posting(title: "エンジニアへのインタビュー記事作成（Ruby経験者向け）")
    result = classify(posting)

    assert_nil result.category
  end

  def test_category_nil_for_survey_monitor_posting_even_with_ruby_in_title
    posting = build_posting(title: "Rubyエンジニア向けアンケートモニター募集")
    result = classify(posting)

    assert_nil result.category
  end

  def test_non_dev_result_has_only_category_field_set
    posting = build_posting(title: "Ruby開発会社の法人営業スタッフ募集")
    result = classify(posting)

    assert_nil result.category
    assert_nil result.difficulty
    assert_nil result.recommend
    assert_nil result.memo
    assert_nil result.skills_text
  end

  # === 主分類のスコアリング ===

  def test_category_ruby_when_title_matches
    posting = build_posting(title: "Rubyエンジニア募集")
    result = classify(posting)

    assert_equal "Ruby", result.category
  end

  def test_category_prefers_title_technology_over_a_single_body_mention_of_another_technology
    # タイトルReact(+3) vs 本文Ruby(+1)。スコアで上回るReactが採用される。
    posting = build_posting(title: "Reactエンジニア募集", description: "Rubyの知識があれば尚可のフロントエンド開発です。")
    result = classify(posting)

    assert_equal "React", result.category
  end

  def test_category_ties_broken_by_category_order_ruby_over_typescript
    # タイトルにRuby/TypeScriptが同時に登場 → title一致がどちらも+3で同点 → 並び順(Ruby > TypeScript)でRubyを採用。
    posting = build_posting(title: "Ruby/TypeScript両方できるフルスタックエンジニア募集")
    result = classify(posting)

    assert_equal "Ruby", result.category
  end

  def test_category_uses_skills_field_with_more_weight_than_description
    # skills一致(+2)がdescription一致(+1)を上回るケース。
    posting = build_posting(title: "エンジニア募集", description: "TypeScriptも使うことがあります。",
                             skills: ["React"])
    result = classify(posting)

    assert_equal "React", result.category
  end

  # === hintフォールバック ===

  def test_category_hint_is_adopted_with_memo_when_no_direct_tech_match_but_dev_strong_matches
    posting = build_posting(title: "自社サービスの開発エンジニア募集", description: "詳細は面談時にお伝えします。",
                             category_hint: "Ruby")
    result = classify(posting)

    assert_equal "Ruby", result.category
    assert_includes result.memo, "検索語「Ruby」でヒット（要約に記載なし・詳細ページで要確認）"
  end

  def test_category_nil_when_hint_present_but_dev_strong_regex_does_not_match
    posting = build_posting(title: "自社サービスのお仕事", description: "詳細は面談時にお伝えします。", category_hint: "Ruby")
    result = classify(posting)

    assert_nil result.category
  end

  def test_category_nil_when_no_hint_and_no_tech_match
    posting = build_posting(title: "自社サービスの開発エンジニア募集", description: "詳細は面談時にお伝えします。")
    result = classify(posting)

    assert_nil result.category
  end

  def test_category_hint_fallback_is_not_used_when_a_technology_already_scored
    # 本文でReactに直接ヒットしている場合はhintフォールバック（メモ付き）を使わない。
    posting = build_posting(title: "Reactエンジニア募集", description: "フロントエンド開発です。", category_hint: "Ruby")
    result = classify(posting)

    assert_equal "React", result.category
    refute_includes result.memo.to_s, "検索語「Ruby」でヒット"
  end

  # === レベル（difficulty）: 経験年数の表記揺れ ===

  def test_difficulty_extracts_years_from_keiken_n_nen_wording
    posting = build_posting(title: "Rubyエンジニア募集", description: "実務経験3年以上の方")
    result = classify(posting)

    assert_equal "★★☆ 中級（実務経験3年以上）", result.difficulty
  end

  def test_difficulty_extracts_minimum_years_from_range_wording
    posting = build_posting(title: "Rubyエンジニア募集", description: "経験3〜5年程度の方を募集")
    result = classify(posting)

    assert_equal "★★☆ 中級（実務経験3年以上）", result.difficulty
  end

  def test_difficulty_years_5_or_more_is_advanced_with_year_suffix
    posting = build_posting(title: "Rubyエンジニア募集", description: "実務経験5年以上のエンジニアを募集します。")
    result = classify(posting)

    assert_equal "★★★ 上級（リード・設計／5年以上）", result.difficulty
  end

  def test_difficulty_lead_keyword_without_years_has_no_year_suffix
    posting = build_posting(title: "Rubyテックリード募集", description: "アーキテクチャ設計をお任せします")
    result = classify(posting)

    assert_equal "★★★ 上級（リード・設計）", result.difficulty
  end

  def test_difficulty_beginner_keyword_without_years_or_lead
    posting = build_posting(title: "Ruby未経験からのエンジニア", description: "学習中の方も歓迎、ジュニアポジションです")
    result = classify(posting)

    assert_equal "★☆☆ 初級（未経験・学習中OK）", result.difficulty
  end

  def test_difficulty_defaults_to_mid_level_when_no_signal_present
    posting = build_posting(title: "Rubyエンジニア募集")
    result = classify(posting)

    assert_equal "★★☆ 中級（実務経験あり）", result.difficulty
  end

  # === おすすめ（recommend）加点 ===

  def test_recommend_single_star_for_hourly_reward_at_threshold
    posting = build_posting(title: "Rubyエンジニア募集", reward: "時給5,000円")
    result = classify(posting)

    assert_equal "🌟", result.recommend
  end

  def test_recommend_blank_for_hourly_reward_below_threshold
    posting = build_posting(title: "Rubyエンジニア募集", reward: "時給3,999円")
    result = classify(posting)

    assert_equal "", result.recommend
  end

  def test_recommend_single_star_for_fixed_reward_man_notation_30_man_at_threshold
    posting = build_posting(title: "Rubyエンジニア募集", reward: "30万円")
    result = classify(posting)

    assert_equal "🌟", result.recommend
    assert_includes result.memo, "高単価"
  end

  def test_recommend_single_star_for_fixed_reward_50_man_yen_notation
    posting = build_posting(title: "Rubyエンジニア募集", reward: "50万円")
    result = classify(posting)

    assert_equal "🌟", result.recommend
  end

  def test_recommend_blank_for_fixed_reward_just_below_threshold
    posting = build_posting(title: "Rubyエンジニア募集", reward: "299,999円")
    result = classify(posting)

    assert_equal "", result.recommend
  end

  def test_recommend_double_star_when_two_points_scored
    posting = build_posting(title: "Rubyエンジニア募集", description: "フルリモート・長期継続の案件です")
    result = classify(posting)

    assert_equal "🌟🌟", result.recommend
  end

  def test_recommend_blank_when_no_points_scored
    posting = build_posting(title: "Rubyエンジニア募集")
    result = classify(posting)

    assert_equal "", result.recommend
  end

  # === SUSPICIOUS ===

  def test_suspicious_posting_has_blank_recommend_and_warning_memo
    posting = build_posting(title: "Rubyエンジニア募集、月50万稼げる！まずはLINE登録を")
    result = classify(posting)

    refute_nil result.category
    assert_equal "", result.recommend
    assert_includes result.memo, "⚠ 募集条件に注意（LINE誘導・高収入訴求など）"
  end

  # === memo ===

  def test_memo_joins_multiple_matched_reasons_with_slash
    posting = build_posting(title: "Rubyエンジニア募集", description: "リモート可、長期継続の案件です", reward: "50万円")
    result = classify(posting)

    assert_equal "リモート可／長期・継続あり／高単価", result.memo
  end

  def test_memo_includes_react_native_label_for_react_native_postings
    posting = build_posting(title: "React Native アプリ開発エンジニア募集")
    result = classify(posting)

    assert_includes result.memo, "React Native（モバイル）"
  end

  def test_memo_includes_years_of_experience_when_years_extracted
    posting = build_posting(title: "Rubyエンジニア募集", description: "実務経験3年以上の方")
    result = classify(posting)

    assert_includes result.memo, "実務経験3年以上"
  end

  def test_memo_includes_full_capacity_warning
    posting = build_posting(title: "Rubyエンジニア募集", application_status: "応募 5件 / 契約 2/2人")
    result = classify(posting)

    assert_includes result.memo, "募集人数に達している可能性"
  end

  def test_memo_default_message_when_nothing_matches
    posting = build_posting(title: "Rubyエンジニア募集")
    result = classify(posting)

    assert_equal "条件は案件ページで要確認", result.memo
  end

  # === skills_text ===

  def test_skills_text_uses_ruby_on_rails_label_when_rails_detected
    posting = build_posting(title: "Ruby on Rails エンジニア募集")
    result = classify(posting)

    assert_equal "Ruby on Rails", result.skills_text
  end

  def test_skills_text_uses_ruby_on_rails_label_when_ror_detected
    posting = build_posting(title: "RoR エンジニア募集")
    result = classify(posting)

    assert_equal "Ruby on Rails", result.skills_text
  end

  def test_skills_text_uses_plain_ruby_label_without_rails_or_ror
    posting = build_posting(title: "Rubyエンジニア募集")
    result = classify(posting)

    assert_equal "Ruby", result.skills_text
  end

  def test_skills_text_uses_nextjs_label_for_react_category
    posting = build_posting(title: "Next.js エンジニア募集")
    result = classify(posting)

    assert_equal "React", result.category
    assert_equal "Next.js", result.skills_text
  end

  def test_skills_text_uses_react_native_label_when_space_separated
    posting = build_posting(title: "React Native アプリ開発エンジニア募集")
    result = classify(posting)

    assert_equal "React Native", result.skills_text
  end

  def test_skills_text_uses_react_native_label_when_hyphen_separated
    # REACT_NATIVE_RE(ラベル用)もREACT_RE(主分類用)と同じく空白・ハイフンの両方を許容するため、
    # ハイフン区切りでも"React Native"ラベルになる想定。
    posting = build_posting(title: "React-Nativeアプリ開発エンジニア募集")
    result = classify(posting)

    assert_equal "React", result.category
    assert_equal "React Native", result.skills_text
  end

  def test_skills_text_falls_back_to_plain_react_label_without_native_or_nextjs
    posting = build_posting(title: "Reactエンジニア募集")
    result = classify(posting)

    assert_equal "React", result.category
    assert_equal "React", result.skills_text
  end

  def test_skills_text_appends_additional_skills_joined_by_slash
    posting = build_posting(title: "Rubyエンジニア募集", description: "AWS・Dockerでの開発経験がある方")
    result = classify(posting)

    assert_equal "Ruby / AWS / Docker", result.skills_text
  end

  def test_skills_text_excludes_java_when_only_javascript_is_mentioned
    posting = build_posting(title: "Rubyエンジニア募集", description: "JavaScriptも使った開発です")
    result = classify(posting)

    assert_equal "Ruby", result.skills_text
    refute_includes result.skills_text, "Java"
  end

  def test_skills_text_includes_java_when_java_itself_is_mentioned
    posting = build_posting(title: "Rubyエンジニア募集", description: "Javaでの開発経験も歓迎")
    result = classify(posting)

    assert_includes result.skills_text.split(" / "), "Java"
  end

  def test_skills_text_includes_go_for_golang_mention
    posting = build_posting(title: "Rubyエンジニア募集", description: "Golangでのマイクロサービス開発経験者歓迎")
    result = classify(posting)

    assert_includes result.skills_text.split(" / "), "Go"
  end

  def test_skills_text_excludes_git_and_github_even_when_mentioned
    posting = build_posting(title: "Rubyエンジニア募集", description: "GitHubでのバージョン管理、Gitの基本操作ができる方")
    result = classify(posting)

    assert_equal "Ruby", result.skills_text, "Git/GitHubはノイズのため追加スキルに含めない設計"
  end
  # --- レビュー反映（react-router・リモートNG表記・参加型の非開発案件） ---

  def test_react_re_matches_hyphenated_react_packages
    assert FreelanceJobs::EngineerClassifier::REACT_RE.match?("react-router / react-redux を使用")
  end

  def test_react_native_re_matches_hyphenated_react_native
    assert FreelanceJobs::EngineerClassifier::REACT_NATIVE_RE.match?("React-Native でアプリ開発")
  end

  def test_remote_re_does_not_match_full_remote_ng_or_remote_work_not_allowed
    refute FreelanceJobs::EngineerClassifier::REMOTE_RE.match?("フルリモートNG（月1出社必須）")
    refute FreelanceJobs::EngineerClassifier::REMOTE_RE.match?("リモート勤務不可")
    refute FreelanceJobs::EngineerClassifier::REMOTE_RE.match?("在宅不可")
  end

  def test_remote_re_matches_remote_work_allowed_variants
    assert FreelanceJobs::EngineerClassifier::REMOTE_RE.match?("リモートワーク可")
    assert FreelanceJobs::EngineerClassifier::REMOTE_RE.match?("フルリモ可")
    assert FreelanceJobs::EngineerClassifier::REMOTE_RE.match?("フルリモート歓迎")
  end

  def test_category_nil_for_interview_participation_posting
    posting = build_posting(title: "インタビューを受けていただけるフリーランスエンジニア募集",
                            description: "React経験者のキャリアについてお話を伺います", category_hint: "React")
    result = classify(posting)

    assert_nil result.category
  end

  def test_category_kept_for_survey_system_development_posting
    posting = build_posting(title: "ユーザーアンケートシステムをReactで構築")
    result = classify(posting)

    assert_equal "React", result.category
  end

  def test_category_kept_for_monitor_ui_development_posting
    posting = build_posting(title: "モニターUIのキャリブレーションアプリ開発（Ruby）")
    result = classify(posting)

    assert_equal "Ruby", result.category
  end
end
