# frozen_string_literal: true

module FreelanceJobs
  # 取得対象（プロファイル）の定義を集約する。未経験向け(beginner)とエンジニア向け(engineer)の
  # 2種類があり、それぞれ取得元サイト・分類器・出力シート(gid)・列並びの組み合わせを持つ。
  # ResearchService・RowBuilder・SheetMergerは、この定義を渡されるだけで具体的なサイト名や
  # 分類ロジックを意識しない。
  class Profile
    Definition = Struct.new(
      :key, :label, :sheet_gid, :header, :category_order, :classifier, :source_specs, :new_rows_require_star,
      :highlight_rule,
      keyword_init: true
    )

    # 行全体を色分けする条件付き書式。formula はシートのデータ先頭行（3行目）を基準に書く
    # （SheetsClient::FIRST_DATA_ROW_NUMBER と揃えること）。列は $ 固定、行は相対にすると
    # 1行につき1回評価され、条件に合った行だけが塗られる。
    HighlightRule = Struct.new(:description, :formula, :background_color, keyword_init: true)

    # Struct・配列・Hash・文字列を再帰的にfreezeした値を返す（元の値は変更しない）。
    # Classやシンボルなどそれ以外の値はそのまま返す（sourceクラス・classifierクラスの参照を
    # freezeすると、後からのメソッド定義等が壊れるおそれがあるため対象外にしている）。
    # RowBuilder::HEADERのように既にfrozenな配列・文字列を渡しても、複製してからfreezeする
    # ため破壊的変更にはならない。
    def self.deep_freeze(value)
      case value
      when Struct
        value.each_pair { |name, member| value[name] = deep_freeze(member) }
        value.freeze
      when Array
        value.map { |element| deep_freeze(element) }.freeze
      when Hash
        value.each_with_object({}) { |(key, element), frozen_hash| frozen_hash[key] = deep_freeze(element) }.freeze
      when String
        value.freeze
      else
        value
      end
    end

    # RowBuilder::HEADERの「難易度」列(G列/index 6)と「一言メモ（おすすめ理由・注意点）」列
    # (N列/index 13)を、エンジニア向けの文言に差し替えたヘッダーを作る。
    def self.build_engineer_header
      header = FreelanceJobs::RowBuilder::HEADER.dup
      header[6] = "レベル（求められる経験）"
      header[13] = "一言メモ（条件・注意点）"
      header
    end

    BEGINNER = deep_freeze(
      Definition.new(
        key: "beginner",
        label: "未経験向け HTML/CSS・Excel",
        sheet_gid: 0,
        header: FreelanceJobs::RowBuilder::HEADER,
        category_order: ["HTML/CSS", "Excel・スプレッドシート"],
        classifier: FreelanceJobs::Classifier,
        source_specs: [
          [FreelanceJobs::Sources::Crowdworks, {}],
          [FreelanceJobs::Sources::Lancers, {}],
          [FreelanceJobs::Sources::Coconala, {}],
          [FreelanceJobs::Sources::Shufti, {}],
          [FreelanceJobs::Sources::Mamaworks, {}],
          [FreelanceJobs::Sources::Craudia, {}]
        ],
        new_rows_require_star: true
      )
    )

    ENGINEER = deep_freeze(
      Definition.new(
        key: "engineer",
        label: "Ruby・TypeScript・React",
        sheet_gid: 1_065_736_587,
        header: build_engineer_header,
        category_order: ["Ruby", "TypeScript", "React"],
        classifier: FreelanceJobs::EngineerClassifier,
        source_specs: [
          # フリーランスエージェント・マッチング（案件単価が明示され、分類の当たりが良い順に置く）。
          # 2026-09-12「エンジニア申込サイト一覧」の実地調査で採用推奨(◎/○)となった8サイトを追加した。
          [FreelanceJobs::Sources::Levtech, {}],
          [FreelanceJobs::Sources::CoconalaTech, {}],
          [FreelanceJobs::Sources::Bizlink, {}],
          [FreelanceJobs::Sources::HiproTech, {}],
          [FreelanceJobs::Sources::FindyFreelance, {}],
          [FreelanceJobs::Sources::Fosternet, {}],
          [FreelanceJobs::Sources::Potepan, {}],
          [FreelanceJobs::Sources::TechStock, {}],
          [FreelanceJobs::Sources::LevtechCreator, {}],
          # クラウドソーシング（未経験向けプロファイルと同じ取得元をキーワード検索で流用する）。
          [FreelanceJobs::Sources::Crowdworks, {
            search_targets: [
              { keyword: "Ruby", hint: "Ruby", max_page: 2 },
              { keyword: "Rails", hint: "Ruby", max_page: 1 },
              { keyword: "TypeScript", hint: "TypeScript", max_page: 2 },
              { keyword: "React", hint: "React", max_page: 2 }
            ]
          }],
          [FreelanceJobs::Sources::Lancers, { fixed_paths: [], keywords: ["Ruby", "TypeScript", "React"] }],
          [FreelanceJobs::Sources::Coconala, { keywords: ["Ruby", "TypeScript", "React"] }],
          [FreelanceJobs::Sources::Shufti, { tag_ids: [], keywords: ["Ruby", "TypeScript", "React"] }],
          [FreelanceJobs::Sources::Mamaworks, { category_paths: ["/jobs/engineering"], keyword_filter: nil }],
          [FreelanceJobs::Sources::Craudia, {}]
        ],
        new_rows_require_star: false,
        # 「★★☆ 中級（実務経験3年以上）」より下のレベル、つまり経験3年未満でも応募できる
        # Ruby・TypeScript案件を淡い青で塗る。対象は初級／中級(実務経験あり)／中級(1年以上)／
        # 中級(2年以上)で、上級と3年以上だけを除く。
        # G列の値は EngineerClassifier#classify_difficulty が作る文字列
        #（「★★☆ 中級（実務経験N年以上）」「★★★ 上級（リード・設計／N年以上）」等）。
        # 年数は「3〜9年」と「10年以上（2桁）」をまとめて除外する。年数表記のない
        # 「中級（実務経験あり）」は残す。改行入りの値（"中級\n（実務経験あり）"）でも
        # REGEXMATCHは部分一致なので判定は変わらない。
        highlight_rule: HighlightRule.new(
          description: "Ruby・TypeScriptで実務経験3年未満（未経験・経験あり・1年・2年）でも応募できる案件",
          formula: '=AND(OR($C3="Ruby",$C3="TypeScript"),' \
                    'NOT(REGEXMATCH($G3,"上級")),' \
                    'NOT(REGEXMATCH($G3,"実務経験(\d{2,}|[3-9])年")))',
          background_color: { red: 0.80, green: 0.89, blue: 0.98 }
        )
      )
    )

    DEFINITIONS = [BEGINNER, ENGINEER].freeze

    # keyに対応する定義を返す。未知のkeyはArgumentError（候補keyをメッセージに含める）。
    def self.find(key)
      definition = DEFINITIONS.find { |candidate| candidate.key == key }
      return definition if definition

      candidate_keys = DEFINITIONS.map(&:key).join("、")
      raise ArgumentError, "未知のプロファイルキーです: #{key.inspect}（候補: #{candidate_keys}）"
    end

    # 全プロファイル定義をbeginner, engineerの順で返す。
    def self.all
      DEFINITIONS
    end
  end
end
