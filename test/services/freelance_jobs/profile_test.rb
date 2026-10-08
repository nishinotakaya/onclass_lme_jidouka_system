# frozen_string_literal: true
# test/services/freelance_jobs/profile_test.rb
#
# FreelanceJobs::Profile（取得対象プロファイルの定義）を検証する。
# find/all の振る舞いと、定義（Struct・配列・Hashの入れ子）が deep_freeze されていることを確認する。

require_relative "../../support/freelance_jobs_loader"
require "yaml"
require "date"

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

  def test_all_returns_beginner_then_engineer_then_hokkaido_in_order
    assert_equal ["beginner", "engineer", "hokkaido"], FreelanceJobs::Profile.all.map(&:key)
  end

  def test_all_returns_the_same_definitions_as_the_named_constants
    assert_equal [FreelanceJobs::Profile::BEGINNER, FreelanceJobs::Profile::ENGINEER, FreelanceJobs::Profile::HOKKAIDO],
                 FreelanceJobs::Profile.all
  end

  # --- engineer header: G列・N列だけ差し替え、それ以外はHEADERと同じ ---

  def test_engineer_header_replaces_only_difficulty_and_memo_columns
    header = FreelanceJobs::Profile::ENGINEER.header

    assert_equal 16, header.size
    assert_equal "レベル（求められる経験）", header[6]
    assert_equal "一言メモ（条件・注意点）", header[13]
    assert_equal "追加日", header[15]
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

  # --- AC-02b: ココナラテックは include_closed: true で取得する ---

  def test_engineer_coconala_tech_source_spec_includes_closed_postings
    coconala_tech_options = FreelanceJobs::Profile::ENGINEER.source_specs
                                                             .find { |source_class, _options| source_class == FreelanceJobs::Sources::CoconalaTech }
                                                             .last

    assert_equal({ include_closed: true }, coconala_tech_options)
  end

  # --- Reshine: Itpropartnersの直後・クラウドソーシング群(Crowdworks)より前に置く ---
  # （まだ FreelanceJobs::Sources::Reshine が存在しないため、このテストは
  #   NameError: uninitialized constant で落ちるのが正しいRedの状態）

  def test_engineer_profile_includes_reshine_source
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_includes source_classes, FreelanceJobs::Sources::Reshine
  end

  def test_engineer_source_specs_places_reshine_right_after_itpropartners_and_before_crowdsourcing_group
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    itpropartners_index = source_classes.index(FreelanceJobs::Sources::Itpropartners)
    reshine_index = source_classes.index(FreelanceJobs::Sources::Reshine)
    crowdworks_index = source_classes.index(FreelanceJobs::Sources::Crowdworks)

    refute_nil itpropartners_index
    refute_nil reshine_index
    refute_nil crowdworks_index
    assert_equal itpropartners_index + 1, reshine_index, "ReshineはItpropartnersの直後に置くはず"
    assert(reshine_index < crowdworks_index, "Reshineはクラウドソーシング群(Crowdworks)より前に置くはず")
  end

  # --- AC-08: Midworks・テクフリ(Techcareer)はReshineの直後・クラウドソーシング群(Crowdworks)より前に置く ---
  # （まだ FreelanceJobs::Sources::Midworks / Techcareer が存在しないため、このテストは
  #   NameError: uninitialized constant で落ちるのが正しいRedの状態）

  def test_engineer_profile_includes_midworks_and_techcareer_sources
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_includes source_classes, FreelanceJobs::Sources::Midworks
    assert_includes source_classes, FreelanceJobs::Sources::Techcareer
  end

  def test_engineer_source_specs_places_midworks_and_techcareer_right_after_reshine_and_before_crowdsourcing_group
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    reshine_index = source_classes.index(FreelanceJobs::Sources::Reshine)
    midworks_index = source_classes.index(FreelanceJobs::Sources::Midworks)
    techcareer_index = source_classes.index(FreelanceJobs::Sources::Techcareer)
    crowdworks_index = source_classes.index(FreelanceJobs::Sources::Crowdworks)

    refute_nil reshine_index
    refute_nil midworks_index
    refute_nil techcareer_index
    refute_nil crowdworks_index
    assert_equal reshine_index + 1, midworks_index, "MidworksはReshineの直後に置くはず"
    assert_equal midworks_index + 1, techcareer_index, "テクフリ(Techcareer)はMidworksの直後に置くはず"
    assert(techcareer_index < crowdworks_index, "Midworks・テクフリはクラウドソーシング群(Crowdworks)より前に置くはず")
  end

  # --- AC-05(2026-10-03再調査): Workship・Offers・Forkwell Jobs・DYMテックはTechcareerの直後・Crowdworksより前 ---

  def test_engineer_source_specs_places_four_new_sources_in_order_right_after_techcareer_and_before_crowdsourcing_group
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    techcareer_index = source_classes.index(FreelanceJobs::Sources::Techcareer)
    refute_nil techcareer_index
    assert_equal(
      [
        FreelanceJobs::Sources::Workship,
        FreelanceJobs::Sources::Offers,
        FreelanceJobs::Sources::ForkwellJobs,
        FreelanceJobs::Sources::DymTech
      ],
      source_classes[techcareer_index + 1, 4],
      "Techcareerの直後にWorkship, Offers, ForkwellJobs, DymTechの順で並ぶはず"
    )
    assert_equal FreelanceJobs::Sources::Crowdworks, source_classes[techcareer_index + 10],
                 "4サイトとその直後の5サイト(Remogu・AtEngineer・MijicaFreelance・TechReach・LancersAgent)の後はクラウドソーシング群の先頭(Crowdworks)のはず"
  end

  def test_engineer_four_new_sources_have_empty_options
    specs = FreelanceJobs::Profile::ENGINEER.source_specs
    [
      FreelanceJobs::Sources::Workship,
      FreelanceJobs::Sources::Offers,
      FreelanceJobs::Sources::ForkwellJobs,
      FreelanceJobs::Sources::DymTech
    ].each do |source_class|
      spec = specs.find { |candidate_class, _options| candidate_class == source_class }
      refute_nil spec, "#{source_class}がENGINEERのsource_specsに無い"
      assert_equal({}, spec.last, "#{source_class}のオプションは{}のはず")
    end
  end

  # --- Remogu・アットエンジニア・mijicaフリーランスはDymTechの直後（続けてTechReach・LancersAgent） ---

  def test_engineer_source_specs_places_three_sources_right_after_dym_tech_with_empty_options
    specs = FreelanceJobs::Profile::ENGINEER.source_specs
    source_classes = specs.map(&:first)
    expected_classes = [
      FreelanceJobs::Sources::Remogu,
      FreelanceJobs::Sources::AtEngineer,
      FreelanceJobs::Sources::MijicaFreelance
    ]

    dym_tech_index = source_classes.index(FreelanceJobs::Sources::DymTech)
    refute_nil dym_tech_index
    assert_equal expected_classes, source_classes[dym_tech_index + 1, 3]
    assert_equal FreelanceJobs::Sources::Crowdworks, source_classes[dym_tech_index + 6],
                 "MijicaFreelanceの後にTechReach・LancersAgentが並び、その直後がCrowdworksのはず"
    assert_equal [{}, {}, {}], specs[dym_tech_index + 1, 3].map(&:last)
    assert_equal ["Remogu", "アットエンジニア", "mijicaフリーランス"], expected_classes.map { |klass| klass::SITE_NAME }
  end

  # 2026-10-08追加: テックリーチ・ランサーズエージェントは既定の取得対象（3スキル／2スキル）で足りるためオプション不要。
  def test_engineer_source_specs_places_tech_reach_and_lancers_agent_right_after_mijica_freelance_with_empty_options
    specs = FreelanceJobs::Profile::ENGINEER.source_specs
    source_classes = specs.map(&:first)

    mijica_index = source_classes.index(FreelanceJobs::Sources::MijicaFreelance)
    refute_nil mijica_index
    assert_equal [FreelanceJobs::Sources::TechReach, FreelanceJobs::Sources::LancersAgent],
                 source_classes[mijica_index + 1, 2]
    assert_equal [{}, {}], specs[mijica_index + 1, 2].map(&:last)
  end

  def test_engineer_source_specs_has_thirty_eight_unique_sources
    source_classes = FreelanceJobs::Profile::ENGINEER.source_specs.map(&:first)

    assert_equal 38, source_classes.size
    assert_equal source_classes.size, source_classes.uniq.size, "同じソースが重複登録されている"
  end

  # --- AC-05: 案件数を書き戻す「サイト一覧」タブのgid ---

  def test_engineer_definition_has_site_list_sheet_gid
    assert_equal 969_307_625, FreelanceJobs::Profile::ENGINEER.site_list_sheet_gid
    assert_equal 969_307_625, FreelanceJobs::Profile.find("engineer").site_list_sheet_gid
  end

  def test_beginner_definition_has_no_site_list_sheet_gid
    assert_nil FreelanceJobs::Profile::BEGINNER.site_list_sheet_gid
  end

  # --- AC-06: 北海道 出社・ハイブリッド プロファイル ---

  def test_find_returns_hokkaido_definition_for_known_key
    assert_same FreelanceJobs::Profile::HOKKAIDO, FreelanceJobs::Profile.find("hokkaido")
  end

  def test_hokkaido_definition_basic_attributes
    definition = FreelanceJobs::Profile::HOKKAIDO

    assert_equal "hokkaido", definition.key
    assert_equal "北海道 出社・ハイブリッド", definition.label
    assert_equal 1_565_795_057, definition.sheet_gid
    assert_equal FreelanceJobs::Profile.build_engineer_header, definition.header
    assert_equal ["Ruby", "TypeScript", "React", "その他"], definition.category_order
    assert_equal FreelanceJobs::HokkaidoClassifier, definition.classifier
  end

  def test_hokkaido_definition_sheet_behavior_flags
    definition = FreelanceJobs::Profile::HOKKAIDO

    assert_equal false, definition.new_rows_require_star
    assert_equal true, definition.checkbox_column
    assert_nil definition.hidden_level_marker
    assert_nil definition.site_list_sheet_gid
  end

  # 北海道は都道府県一覧URLを直接たどるため、キーワードではなく都道府県ターゲットで3サイトを指定する。
  # フルリモート案件は分類器側で落とすので提供元は問わず拾うが、PE-BANKは Sources::PeBank で直接取るため
  # フリーランスHub経由の「Pe-BANK フリーランス」提供案件だけ除外する（ボードの北海道一覧に該当案件は無い）。
  def test_hokkaido_source_specs_are_three_prefecture_listings
    assert_equal(
      [
        [FreelanceJobs::Sources::FreelanceBoard,
         { search_targets: [{ prefecture_slug: "hokkaido" }], max_pages: 3, excluded_providers: [] }],
        [FreelanceJobs::Sources::FreelanceHub,
         { search_targets: [{ prefecture_id: 1 }], max_pages: 3, excluded_providers: ["Pe-BANK フリーランス"] }],
        [FreelanceJobs::Sources::PeBank,
         { search_targets: [{ language_slug: "hokkaido", category_hint: nil }], max_pages: 3 }]
      ],
      FreelanceJobs::Profile::HOKKAIDO.source_specs
    )
  end

  def test_hokkaido_definition_is_deeply_frozen
    definition = FreelanceJobs::Profile::HOKKAIDO

    assert definition.frozen?
    assert definition.source_specs.frozen?
    assert definition.category_order.frozen?
    assert definition.header.frozen?
    definition.source_specs.each do |source_spec|
      assert source_spec.frozen?
      assert source_spec.last.frozen?, "#{source_spec.first}のoptions Hashがfrozenでない"
      assert source_spec.last[:search_targets].frozen?
    end
  end

  # オプションのキー名が取得元の initialize と食い違うと本番の朝バッチで初めて ArgumentError になるため、
  # 通信せずに生成だけして実在の引数と合っていることを確認する。
  def test_hokkaido_source_specs_options_are_accepted_by_each_source_initialize
    fetcher = Object.new
    FreelanceJobs::Profile::HOKKAIDO.source_specs.each do |source_class, options|
      source = source_class.new(fetcher: fetcher, today: Date.new(2026, 10, 8), **options)

      assert_instance_of source_class, source
    end
  end

  # --- AC-06: scheduler の description は3シート更新を示す ---

  def test_scheduler_research_job_description_mentions_three_sheets
    %w[scheduler_production.yml scheduler_development.yml].each do |file_name|
      schedule = YAML.load_file(File.expand_path("../../../config/#{file_name}", __dir__))
      description = schedule.fetch("freelance_jobs_research_morning").fetch("description")

      assert_includes description, "3シート", "#{file_name} の副業案件ジョブ説明が3シート更新になっていない"
    end
  end

  # --- deep_freeze はコピーを作る（元の定数を破壊しない） ---

  def test_deep_freeze_does_not_freeze_the_original_row_builder_header_constant_object
    # HEADERは元々frozenだが、Profile.deep_freezeはコピーしてからfreezeするため、
    # header自体は別オブジェクトであることを確認する（同値だが同一オブジェクトではない）。
    refute_same FreelanceJobs::RowBuilder::HEADER, FreelanceJobs::Profile::BEGINNER.header
    assert_equal FreelanceJobs::RowBuilder::HEADER, FreelanceJobs::Profile::BEGINNER.header
  end
end
