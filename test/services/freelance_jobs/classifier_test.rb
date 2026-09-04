# frozen_string_literal: true
# test/services/freelance_jobs/classifier_test.rb

require_relative "../../support/freelance_jobs_loader"
require "date"

class FreelanceJobsClassifierTest < Minitest::Test
  TODAY = Date.new(2026, 9, 4)

  def build_posting(site: "CrowdWorks", title: "", description: "", category_hint: nil, skills: [], tags: [],
                     deadline_on: nil, deadline_text: "-", work_format: "固定報酬制", application_status: "-",
                     reward: "要相談")
    FreelanceJobs::JobPosting.new(
      site: site, url: "https://example.com/jobs/1", title: title, description: description,
      category_hint: category_hint, reward: reward, work_format: work_format,
      application_status: application_status, deadline_text: deadline_text, deadline_on: deadline_on,
      skills: skills, client: "", tags: tags, posted_on: nil
    )
  end

  # --- カテゴリ判定: HTML/CSS ---

  def test_category_html_css_when_title_matches_directly
    posting = build_posting(title: "コーポレートサイトのHTMLコーディングをお願いします")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "HTML/CSS", result.category
  end

  def test_category_html_css_when_only_description_matches_and_excel_does_not
    posting = build_posting(title: "簡単なお仕事です", description: "WordPressのテーマ修正をお願いします")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "HTML/CSS", result.category
  end

  # --- カテゴリ判定: Excel ---

  def test_category_excel_when_body_matches_excel_keywords
    posting = build_posting(title: "スプレッドシートへのデータ入力作業")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "Excel・スプレッドシート", result.category
  end

  # --- カテゴリ判定: 対象外 nil ---

  def test_category_nil_when_neither_regex_matches_and_no_hint
    posting = build_posting(title: "電話営業スタッフ募集", description: "テレアポのお仕事です")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_nil result.category
  end

  def test_category_nil_when_hint_is_excel_but_body_does_not_match
    posting = build_posting(title: "電話営業スタッフ募集", description: "テレアポのお仕事です",
                             category_hint: "Excel・スプレッドシート")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_nil result.category
  end

  # --- ラウンド2 C1: category_hintだけでHTML/CSSにしない ---

  def test_category_hint_alone_is_rejected_when_web_weak_re_does_not_match
    # design_round2.md の実例そのもの。
    posting = build_posting(title: "Instagramバナー制作", description: "SNS用の画像を作成してください",
                             category_hint: "HTML/CSS")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_nil result.category, "hintだけでHTML/CSS扱いにしてはいけない（C1）"
  end

  def test_category_hint_alone_is_accepted_when_web_weak_re_matches
    posting = build_posting(title: "サイトの更新をお願いします", description: "既存ページの文言を差し替えるだけです",
                             category_hint: "HTML/CSS")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "HTML/CSS", result.category
  end

  def test_category_hint_html_css_still_nil_when_hint_is_not_html_css
    posting = build_posting(title: "写真の整理をお願いします", description: "特にキーワードはありません",
                             category_hint: "Excel・スプレッドシート")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_nil result.category
  end

  # --- ラウンド2 C1: EXCEL_REの縮小（リサーチ/情報収集/文字起こし/資料作成は対象外） ---

  def test_category_nil_for_removed_excel_keywords
    ["月次資料作成のお手伝いをお願いします", "海外情報収集のお仕事です", "音声の文字起こし作業", "商品リサーチのお仕事"].each do |title|
      posting = build_posting(title: title)
      result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

      assert_nil result.category, "#{title.inspect} は縮小後のEXCEL_REに一致してはいけない"
    end
  end

  def test_category_still_excel_for_keywords_not_removed
    posting = build_posting(title: "顧客リストのデータ入力とデータ整理")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "Excel・スプレッドシート", result.category
  end

  # --- ラウンド2 C1: シュフティのタグ判定ルールは削除済み ---
  #
  # 注記: 旧ルールは `Array(posting.tags).include?("データ入力")` という完全一致判定だった。
  # tagsはbuild_judgement_textで本文に連結されるため、この文字列は単独でも常にEXCEL_REの
  # 「データ入力」に一致してしまい、旧ルール専用の分岐は事実上到達不能（新旧で観測結果が
  # 変わらない）。そのため旧ルールの有無を単独で判別するテストは書けない。
  # 代わりに、EXCEL_RE縮小（リサーチ除外）とタグルール撤廃の両方が効く実データで検証する。

  def test_shufti_real_posting_with_removed_research_keyword_is_excluded
    # fixtureの実データ（id 386479, リサーチ系タイトル）。EXCEL_RE縮小前はExcelに分類されていた。
    today = Date.new(2026, 9, 4)
    body = File.read(File.expand_path("../../fixtures/files/freelance_jobs/shufti_api_p1.json", __dir__))
    posting = FreelanceJobs::Sources::Shufti.parse(body, today: today).first

    assert_equal "【完全在宅・未経験OK】商品のリサーチ・価格チェック♪長期継続あり", posting.title
    result = FreelanceJobs::Classifier.classify(posting, today: today)

    assert_nil result.category
  end

  # --- 難易度3段階 ---

  def test_difficulty_advanced_when_advanced_keyword_present
    posting = build_posting(title: "VBAマクロで集計を自動化できる方（実務経験者向け）")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "★★★ 経験者向け", result.difficulty
  end

  def test_difficulty_beginner_when_beginner_keyword_present
    posting = build_posting(title: "未経験OK！スプレッドシートへのデータ入力")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "★☆☆ 未経験OK", result.difficulty
  end

  def test_difficulty_default_when_neither_keyword_present
    posting = build_posting(title: "エクセルでの集計業務をお願いします")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "★★☆ 基礎があれば可", result.difficulty
  end

  # --- 要注意 (SUSPICIOUS) ---

  def test_suspicious_posting_has_no_recommend_and_has_warning_memo
    # 分類対象外(nil)だと要注意ロジックまで到達しないため、EXCEL_REにも一致するタイトルにする。
    posting = build_posting(title: "データ入力の副業！月30万円稼げます")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    refute_nil result.category
    assert_equal "", result.recommend
    assert_includes result.memo, "⚠ テンプレ的な高額/誘導系の可能性。詳細と発注者評価を要確認"
  end

  # --- おすすめ 🌟🌟 / 🌟 / 空 ---

  def test_recommend_double_star_for_beginner_difficulty
    posting = build_posting(title: "未経験歓迎のデータ入力スタッフ")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "🌟🌟", result.recommend
  end

  # classify_recommendは「difficulty=='★★☆ 基礎があれば可' かつ SOFT_FRIENDLY_RE一致」で
  # "🌟"（単独）を返す（ラウンド2 C9で追加）。BEGINNER_RE一致はclassify_difficultyの時点で
  # 必ず難易度を「★☆☆ 未経験OK」に確定させるため、BEGINNER_REとSOFT_FRIENDLY_REの双方に
  # 一致する語（例:「マニュアルあり」）が含まれていても、★☆☆側の🌟🌟が優先され
  # 🌟（単独）にはならない。この優先関係（BEGINNER_RE > SOFT_FRIENDLY_RE）を確認する。
  def test_recommend_is_double_star_not_single_star_when_beginner_keyword_present_with_base_category
    posting = build_posting(title: "エクセルでのデータ集計（マニュアルあり）")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "★☆☆ 未経験OK", result.difficulty
    assert_equal "🌟🌟", result.recommend
  end

  # --- ラウンド2 C9: SOFT_FRIENDLY_RE一致による🌟（単独） ---
  # BEGINNER_RE/ADVANCED_REのどちらとも一致しない（→difficultyが★★☆のまま）が、
  # SOFT_FRIENDLY_RE（歓迎|OK|可能|継続|マニュアル|丁寧|サポート|相談|長期）には一致する語だけを含める。

  def test_recommend_single_star_when_base_difficulty_matches_soft_friendly_keyword
    posting = build_posting(title: "データ入力のお仕事（長期のご相談も可能です）")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "★★☆ 基礎があれば可", result.difficulty
    assert_equal "🌟", result.recommend
  end

  def test_recommend_blank_for_base_difficulty_without_beginner_or_soft_friendly_keyword
    posting = build_posting(title: "エクセルでのデータ集計業務")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "★★☆ 基礎があれば可", result.difficulty
    assert_equal "", result.recommend
  end

  # --- skills_text の生成 ---

  def test_skills_text_uses_posting_skills_when_present
    posting = build_posting(title: "HTMLコーディング案件", skills: ["HTML", "CSS", "PHP"])
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "HTML／CSS／PHP", result.skills_text
  end

  def test_skills_text_generated_for_html_css_with_extra_hints
    posting = build_posting(title: "レスポンシブ対応のWordPressサイト修正（jQuery使用）")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "HTML/CSSの基本（タグの読み書き）／JavaScriptの基礎／WordPressの基礎／レスポンシブ対応", result.skills_text
  end

  def test_skills_text_generated_for_excel_with_extra_hints
    posting = build_posting(title: "VBAマクロを使った正確で丁寧なデータ集計")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "Excel／スプレッドシートの基本操作（入力・コピー・簡単な関数）／VBA・マクロ／正確さ・丁寧さ",
                 result.skills_text
  end

  def test_skills_text_generated_for_excel_without_extra_hints
    posting = build_posting(title: "エクセルでのデータ集計業務")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "Excel／スプレッドシートの基本操作（入力・コピー・簡単な関数）", result.skills_text
  end

  # --- 分類対象外(nil)の場合、他フィールドは組み立てられない ---

  def test_classify_returns_category_only_result_when_out_of_scope
    posting = build_posting(title: "電話営業スタッフ募集")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_nil result.category
    assert_nil result.difficulty
    assert_nil result.recommend
    assert_nil result.memo
    assert_nil result.skills_text
  end

  # === ラウンド2 C8: 未経験向けデータ入力なのに高額固定報酬（テンプレ/誘導系の疑い） ===

  def test_high_reward_data_entry_is_suspicious_and_loses_recommend
    posting = build_posting(title: "データ入力スタッフ大募集（未経験歓迎・スキル不要）", reward: "150,000円")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "Excel・スプレッドシート", result.category
    assert_equal "★☆☆ 未経験OK", result.difficulty
    assert_equal "", result.recommend, "高額固定報酬のデータ入力は🌟が付かない"
    assert_includes result.memo, "⚠ 未経験向けなのに高額固定報酬のデータ入力募集。テンプレ/誘導系の可能性、詳細と発注者評価を要確認"
  end

  def test_high_reward_data_entry_rule_is_exempt_for_hourly_reward
    posting = build_posting(title: "データ入力スタッフ大募集（未経験歓迎・スキル不要）", reward: "時給200,000円")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "🌟🌟", result.recommend, "時給表記はC8の対象外"
    refute_includes result.memo, "⚠ 未経験向けなのに高額固定報酬のデータ入力募集"
  end

  def test_high_reward_data_entry_rule_does_not_trigger_below_threshold
    posting = build_posting(title: "データ入力スタッフ大募集（未経験歓迎・スキル不要）", reward: "50,000円")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "🌟🌟", result.recommend, "10万円未満はC8の対象外"
  end

  def test_high_reward_data_entry_rule_requires_data_entry_like_title
    posting = build_posting(title: "スプレッドシート管理のお仕事（未経験歓迎）", reward: "150,000円")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "🌟🌟", result.recommend, "タイトルがデータ入力系でなければC8の対象外"
  end

  def test_high_reward_data_entry_rule_requires_beginner_keyword
    # BEGINNER_RE/ADVANCED_REどちらの語も含まないニュートラルな本文にする
    # （"経験者優遇"にするとADVANCED_REに一致し difficulty が変わってしまうため避ける）。
    posting = build_posting(title: "データ入力スタッフ大募集", description: "在宅ワークです", reward: "150,000円")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "★★☆ 基礎があれば可", result.difficulty
    refute_includes result.memo.to_s, "⚠ 未経験向けなのに高額固定報酬のデータ入力募集"
  end

  # === D2: first_reward_amount の「万」表記対応 ===

  def test_first_reward_amount_parses_man_notation_without_unit
    assert_equal 300_000, FreelanceJobs::Classifier.first_reward_amount("30万")
  end

  def test_first_reward_amount_parses_man_notation_with_yen_unit
    assert_equal 500_000, FreelanceJobs::Classifier.first_reward_amount("50万円")
  end

  def test_first_reward_amount_parses_decimal_man_notation
    assert_equal 15_000, FreelanceJobs::Classifier.first_reward_amount("1.5万")
  end

  def test_first_reward_amount_still_parses_plain_comma_separated_numbers
    assert_equal 150_000, FreelanceJobs::Classifier.first_reward_amount("150,000円")
  end

  def test_first_reward_amount_picks_first_number_in_range_text_without_man_unit
    assert_equal 10_000, FreelanceJobs::Classifier.first_reward_amount("10,000〜30,000円")
  end

  def test_first_reward_amount_returns_nil_when_no_digits_present
    assert_nil FreelanceJobs::Classifier.first_reward_amount("要相談")
  end

  # 既存のbeginner判定（固定報酬10万円以上）が「30万円」のような万表記でも変わらず動くこと
  # （D2で拡張したfirst_reward_amountの結果を使う経路の回帰確認）。
  def test_high_reward_data_entry_suspicious_also_triggers_for_man_notation_reward
    posting = build_posting(title: "データ入力スタッフ大募集（未経験歓迎・スキル不要）", reward: "30万円")
    result = FreelanceJobs::Classifier.classify(posting, today: TODAY)

    assert_equal "", result.recommend, "「30万円」表記でも10万円以上として高額判定される想定"
    assert_includes result.memo, "⚠ 未経験向けなのに高額固定報酬のデータ入力募集。テンプレ/誘導系の可能性、詳細と発注者評価を要確認"
  end
end
