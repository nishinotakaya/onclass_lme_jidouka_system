# frozen_string_literal: true
# test/services/freelance_jobs/profile_test.rb
#
# FreelanceJobs::Profile（取得対象プロファイルの定義）を検証する。
# find/all の振る舞いと、定義（Struct・配列・Hashの入れ子）が deep_freeze されていることを確認する。

require_relative "../../support/freelance_jobs_loader"

class FreelanceJobsProfileTest < Minitest::Test
  # --- find ---

  def test_find_returns_beginner_definition_for_known_key
    definition = FreelanceJobs::Profile.find("beginner")

    assert_equal "beginner", definition.key
    assert_equal 0, definition.sheet_gid
    assert_equal FreelanceJobs::RowBuilder::HEADER, definition.header
    assert_equal ["HTML/CSS", "Excel・スプレッドシート"], definition.category_order
    assert_equal FreelanceJobs::Classifier, definition.classifier
    assert_equal true, definition.new_rows_require_star
  end

  def test_find_returns_engineer_definition_for_known_key
    definition = FreelanceJobs::Profile.find("engineer")

    assert_equal "engineer", definition.key
    assert_equal 1_065_736_587, definition.sheet_gid
    assert_equal ["Ruby", "TypeScript", "React"], definition.category_order
    assert_equal FreelanceJobs::EngineerClassifier, definition.classifier
    assert_equal false, definition.new_rows_require_star
  end

  def test_find_raises_argument_error_for_unknown_key
    error = assert_raises(ArgumentError) { FreelanceJobs::Profile.find("unknown") }

    assert_includes error.message, "unknown"
  end

  def test_find_error_message_includes_candidate_keys
    error = assert_raises(ArgumentError) { FreelanceJobs::Profile.find("nope") }

    assert_includes error.message, "beginner"
    assert_includes error.message, "engineer"
  end

  def test_find_raises_for_nil_key
    assert_raises(ArgumentError) { FreelanceJobs::Profile.find(nil) }
  end

  # --- all ---

  def test_all_returns_beginner_then_engineer_in_order
    assert_equal ["beginner", "engineer"], FreelanceJobs::Profile.all.map(&:key)
  end

  def test_all_returns_the_same_definitions_as_the_named_constants
    assert_equal [FreelanceJobs::Profile::BEGINNER, FreelanceJobs::Profile::ENGINEER], FreelanceJobs::Profile.all
  end

  # --- engineer header: G列・N列だけ差し替え、それ以外はHEADERと同じ ---

  def test_engineer_header_replaces_only_difficulty_and_memo_columns
    header = FreelanceJobs::Profile::ENGINEER.header

    assert_equal 15, header.size
    assert_equal "レベル（求められる経験）", header[6]
    assert_equal "一言メモ（条件・注意点）", header[13]
    FreelanceJobs::RowBuilder::HEADER.each_index do |index|
      next if [6, 13].include?(index)

      assert_equal FreelanceJobs::RowBuilder::HEADER[index], header[index],
                   "index #{index} 列はbeginnerと同じ文言のはず"
    end
  end

  def test_engineer_header_does_not_mutate_the_original_row_builder_header_constant
    FreelanceJobs::Profile::ENGINEER.header # 参照して副作用が無いことを確認

    refute_includes FreelanceJobs::RowBuilder::HEADER, "レベル（求められる経験）"
    assert_equal "難易度", FreelanceJobs::RowBuilder::HEADER[6]
    assert_equal "一言メモ（おすすめ理由・注意点）", FreelanceJobs::RowBuilder::HEADER[13]
  end

  # --- deep_freeze ---

  def test_beginner_definition_and_its_direct_members_are_frozen
    definition = FreelanceJobs::Profile::BEGINNER

    assert definition.frozen?
    assert definition.source_specs.frozen?
    assert definition.category_order.frozen?
    assert definition.header.frozen?
  end

  def test_beginner_source_spec_entries_and_option_hashes_are_frozen
    definition = FreelanceJobs::Profile::BEGINNER

    definition.source_specs.each do |source_spec|
      assert source_spec.frozen?, "[source_class, options]自体もfrozenのはず"
      assert source_spec.last.frozen?, "#{source_spec.first}のoptions Hashがfrozenでない"
    end
  end

  def test_beginner_header_strings_are_frozen
    assert(FreelanceJobs::Profile::BEGINNER.header.all?(&:frozen?), "header内の各文字列がfrozenでない")
  end

  def test_engineer_crowdworks_search_targets_are_deeply_frozen
    crowdworks_options = FreelanceJobs::Profile::ENGINEER.source_specs
                                                          .find { |source_class, _options| source_class == FreelanceJobs::Sources::Crowdworks }
                                                          .last

    assert crowdworks_options.frozen?
    assert crowdworks_options[:search_targets].frozen?
    assert(crowdworks_options[:search_targets].all?(&:frozen?), "search_targets内の各Hashがfrozenでない")
    assert crowdworks_options[:search_targets].first[:keyword].frozen?
  end

  def test_engineer_lancers_keywords_array_and_strings_are_deeply_frozen
    lancers_options = FreelanceJobs::Profile::ENGINEER.source_specs
                                                        .find { |source_class, _options| source_class == FreelanceJobs::Sources::Lancers }
                                                        .last

    assert lancers_options[:keywords].frozen?
    assert(lancers_options[:keywords].all?(&:frozen?), "keywords内の各文字列がfrozenでない")
  end

  def test_mutating_frozen_search_targets_array_raises_frozen_error
    crowdworks_options = FreelanceJobs::Profile::ENGINEER.source_specs
                                                          .find { |source_class, _options| source_class == FreelanceJobs::Sources::Crowdworks }
                                                          .last

    assert_raises(FrozenError) { crowdworks_options[:search_targets] << { keyword: "hack" } }
  end

  def test_mutating_frozen_options_hash_raises_frozen_error
    lancers_options = FreelanceJobs::Profile::ENGINEER.source_specs
                                                        .find { |source_class, _options| source_class == FreelanceJobs::Sources::Lancers }
                                                        .last

    assert_raises(FrozenError) { lancers_options[:keywords] = [] }
  end

  # --- deep_freeze はクラス参照自体は凍結しない（メソッド定義等を壊さないため） ---

  def test_deep_freeze_leaves_source_class_references_unfrozen
    source_class = FreelanceJobs::Profile::BEGINNER.source_specs.first.first

    assert_equal FreelanceJobs::Sources::Crowdworks, source_class
    refute source_class.frozen?, "Classオブジェクト自体はfreezeの対象外という設計"
  end

  # --- deep_freeze はコピーを作る（元の定数を破壊しない） ---

  def test_deep_freeze_does_not_freeze_the_original_row_builder_header_constant_object
    # HEADERは元々frozenだが、Profile.deep_freezeはコピーしてからfreezeするため、
    # header自体は別オブジェクトであることを確認する（同値だが同一オブジェクトではない）。
    refute_same FreelanceJobs::RowBuilder::HEADER, FreelanceJobs::Profile::BEGINNER.header
    assert_equal FreelanceJobs::RowBuilder::HEADER, FreelanceJobs::Profile::BEGINNER.header
  end
end
