# frozen_string_literal: true

module FreelanceJobs
  # JobPosting 1件をRuby/TypeScript/React案件として分類する純粋関数群（通信・時刻取得なし）。
  # 未経験向けのFreelanceJobs::Classifierとは判定軸が異なるため別クラスに分けているが、
  # Result構造体・SUSPICIOUS_RE・first_reward_amount・full_capacity?はClassifierを再利用する。
  class EngineerClassifier
    Result = FreelanceJobs::Classifier::Result

    # 技術検出（単語境界付き）。Latin語は前後に英字が来ない条件を付け、他の単語の一部への
    # 誤爆を防ぐ。TypeScriptは大文字小文字無視、TSは大文字のみ一致させる。
    RUBY_RE = /(?<![A-Za-z])(?:Ruby|Rails|RoR)(?![A-Za-z])|ルビー/i
    TYPESCRIPT_RE = /(?<![A-Za-z])(?i:TypeScript)(?![A-Za-z])|(?<![A-Za-z])TS(?![A-Za-z])|タイプスクリプト/
    REACT_RE = /(?<![A-Za-z])React(?:\.?js|[\s-]*Native)?(?![A-Za-z]|-[A-Za-z0-9-]*\.(?:com|jp|net|io|co))|(?<![A-Za-z])Next\.?js(?![A-Za-z])|リアクト/i

    # 主分類のスコアリング対象。並び順はcategory_order（Ruby > TypeScript > React）と同じで、
    # スコア同点時のタイブレークにそのまま使う。
    TECHNOLOGIES = [
      ["Ruby", RUBY_RE],
      ["TypeScript", TYPESCRIPT_RE],
      ["React", REACT_RE]
    ].freeze

    # 「営業」が募集する仕事そのものではなく、開発するシステムの業務ドメインを指す語
    # （営業支援システム・営業管理ツール等）に続く場合は非開発案件とみなさない。
    # これを入れないと「営業支援系サブシステムの開発」のような純然たる開発案件が
    # NON_DEV_REで落ちる（実測: ビズリンクの実案件105件中2件が該当）。
    SALES_DOMAIN_SUFFIX_RE = /支援|促進|管理|効率化|自動化|DX|システム|ツール|データ|情報|部門|チーム/

    # 記事作成・営業・採用代行等、開発案件ではない募集を除外する。
    NON_DEV_RE = /記事(作成|執筆)|ライター|ライティング|営業(?!#{SALES_DOMAIN_SUFFIX_RE})|スカウト|採用代行|講師|レッスン|家庭教師|翻訳|インタビュー(?:を受け|にご協力|対象|調査)|アンケート(?:回答|に答え|にご協力)|モニター(?:募集|参加|調査)/

    # 技術名が0点のとき、category_hintを採用してよいかの最終ゲート。
    DEV_STRONG_RE = /エンジニア|開発|実装|フロントエンド|バックエンド|Web ?アプリ|システム|API|プログラ/

    # skills_textの検出技術ラベルをRuby/React内で細分するための補助正規表現。
    RAILS_RE = /(?<![A-Za-z])(?:Rails|RoR)(?![A-Za-z])/i
    NEXTJS_RE = /(?<![A-Za-z])Next\.?js(?![A-Za-z])/i
    REACT_NATIVE_RE = /React[\s-]*Native/i

    # 経験年数の抽出。「経験3〜5年」のような範囲表記は小さい方の数値だけを捕捉する
    # （後続の「〜5」は非捕捉グループで読み飛ばす）。
    EXPERIENCE_YEARS_RE = /(?:経験|歴)[^\d]{0,8}(\d+)\s*(?:[〜~～-]\s*\d+\s*)?年|(\d+)\s*年(?:以上|程度)/
    LEAD_RE = /リード|テックリード|CTO|アーキテク|要件定義|上流|プロジェクトマネ|(?<![A-Za-z])PM(?![A-Za-z])/
    BEGINNER_RE = /未経験|初心者|学習中|駆け出し|ジュニア|初級|経験不問|経験浅/

    REMOTE_RE = /(?:リモート|在宅|フルリモ(?!ート))(?!\s*(?:勤務|ワーク)?\s*(?:不可|NG))/
    LONG_TERM_RE = /長期|継続|常駐|週\s*[1-5]\s*日|月\s*\d{2,3}\s*(時間|h)/i

    # skills_textに追加する技術名 => 検出用正規表現（単語境界付き）。Git/GitHubはほぼ全案件で
    # ヒットしノイズになるため含めない。Go/Javaは大文字始まりのみに絞り、一般的な単語
    # （「go」「java=コーヒー」等）やJavaScriptとの誤検出を避ける。
    ADDITIONAL_SKILLS = {
      "Node.js" => /(?<![A-Za-z])Node(?:\.?js)?(?![A-Za-z])/i,
      "Vue" => /(?<![A-Za-z])Vue(?:\.?js)?(?![A-Za-z])/i,
      "Nuxt" => /(?<![A-Za-z])Nuxt(?:\.?js)?(?![A-Za-z])/i,
      "Angular" => /(?<![A-Za-z])Angular(?![A-Za-z])/i,
      "Go" => /(?<![A-Za-z])Go(?:lang)?(?![A-Za-z])/,
      "Python" => /(?<![A-Za-z])Python(?![A-Za-z])/i,
      "PHP" => /(?<![A-Za-z])PHP(?![A-Za-z])/i,
      "Laravel" => /(?<![A-Za-z])Laravel(?![A-Za-z])/i,
      "Java" => /(?<![A-Za-z])Java(?![A-Za-z])/,
      "Kotlin" => /(?<![A-Za-z])Kotlin(?![A-Za-z])/i,
      "Swift" => /(?<![A-Za-z])Swift(?![A-Za-z])/i,
      "AWS" => /(?<![A-Za-z])AWS(?![A-Za-z])/i,
      "GCP" => /(?<![A-Za-z])GCP(?![A-Za-z])/i,
      "Docker" => /(?<![A-Za-z])Docker(?![A-Za-z])/i,
      "Kubernetes" => /(?<![A-Za-z])Kubernetes(?![A-Za-z])/i,
      "GraphQL" => /(?<![A-Za-z])GraphQL(?![A-Za-z])/i,
      "PostgreSQL" => /(?<![A-Za-z])PostgreSQL(?![A-Za-z])/i,
      "MySQL" => /(?<![A-Za-z])MySQL(?![A-Za-z])/i,
      "Redis" => /(?<![A-Za-z])Redis(?![A-Za-z])/i,
      "Supabase" => /(?<![A-Za-z])Supabase(?![A-Za-z])/i,
      "Firebase" => /(?<![A-Za-z])Firebase(?![A-Za-z])/i,
      "Figma" => /(?<![A-Za-z])Figma(?![A-Za-z])/i
    }.freeze

    # todayはFreelanceJobs::Classifierと同じインターフェース(classify(posting, today:))を
    # 保つために受け取るが、エンジニア向け分類では締切系のmemoを出さないため未使用。
    def self.classify(posting, today:)
      title_text = posting.title.to_s
      description_text = posting.description.to_s
      skills_text = Array(posting.skills).join(" ")
      judgement_text = [title_text, description_text, skills_text].join(" ")

      return Result.new(category: nil) if judgement_text.match?(NON_DEV_RE)

      category, hint_memo = classify_category_with_hint_memo(posting, title_text, description_text, skills_text)
      return Result.new(category: nil) if category.nil?

      years = extract_experience_years(judgement_text)
      suspicious = judgement_text.match?(FreelanceJobs::Classifier::SUSPICIOUS_RE)

      Result.new(
        category: category,
        difficulty: classify_difficulty(judgement_text, years),
        recommend: classify_recommend(posting, judgement_text, suspicious),
        memo: build_memo(posting, judgement_text, years, hint_memo, suspicious),
        skills_text: build_skills_text(category, judgement_text)
      )
    end

    # 各技術のスコア（title一致+3、skills一致+2、description一致+1、category_hint一致+2）を
    # 集計し、最高点の技術を採用する（同点はTECHNOLOGIESの並び順）。
    # 「どの技術も0点」の判定はtitle/skills/descriptionの一致のみ（text_score）で行う。
    # category_hintの+2をこの判定に含めてしまうと、hintが常に設定される取得元
    # （例: Crowdworksのキーワード検索）では本文が無関係でもtext_scoreと無関係に必ず
    # 非0になり、"本文で要確認"の下のフォールバック分岐が到達不能になってしまうため。
    # text_scoreが1件でも正であれば、hint込みの合計点(total_score)で通常どおり順位付けする。
    # 全技術のtext_scoreが0のときは、category_hintがありタイトルか説明がDEV_STRONG_REに
    # 当たる場合に限りhintを採用する。戻り値は [category, 検索ヒット時のみ付与するmemo文言 or nil]。
    def self.classify_category_with_hint_memo(posting, title_text, description_text, skills_text)
      scored = TECHNOLOGIES.map do |name, regex|
        text_score = 0
        text_score += 3 if title_text.match?(regex)
        text_score += 2 if skills_text.match?(regex)
        text_score += 1 if description_text.match?(regex)
        hint_bonus = posting.category_hint == name ? 2 : 0
        [name, text_score, text_score + hint_bonus]
      end

      if scored.any? { |(_name, text_score, _total_score)| text_score.positive? }
        highest_total_score = scored.map { |(_name, _text_score, total_score)| total_score }.max
        best_name = scored.find { |(_name, _text_score, total_score)| total_score == highest_total_score }.first
        return [best_name, nil]
      end

      hint = posting.category_hint
      if hint && (title_text.match?(DEV_STRONG_RE) || description_text.match?(DEV_STRONG_RE))
        return [hint, "検索語「#{hint}」でヒット（要約に記載なし・詳細ページで要確認）"]
      end

      [nil, nil]
    end

    # EXPERIENCE_YEARS_REの全一致から最小の年数を返す。一致が無ければnil。
    def self.extract_experience_years(judgement_text)
      years = judgement_text.scan(EXPERIENCE_YEARS_RE).flatten.compact.map(&:to_i)
      years.empty? ? nil : years.min
    end

    def self.classify_difficulty(judgement_text, years)
      if (years && years >= 5) || judgement_text.match?(LEAD_RE)
        return years && years >= 5 ? "★★★ 上級（リード・設計／#{years}年以上）" : "★★★ 上級（リード・設計）"
      end
      return "★★☆ 中級（実務経験#{years}年以上）" if years
      return "★☆☆ 初級（未経験・学習中OK）" if judgement_text.match?(BEGINNER_RE)

      "★★☆ 中級（実務経験あり）"
    end

    def self.classify_recommend(posting, judgement_text, suspicious)
      return "" if suspicious

      score = 0
      score += 1 if high_reward?(posting)
      score += 1 if judgement_text.match?(REMOTE_RE)
      score += 1 if judgement_text.match?(LONG_TERM_RE)

      return "🌟🌟" if score >= 2
      return "🌟" if score == 1

      ""
    end

    # 報酬が高単価かどうか。時給表記は4,000円以上、それ以外（固定・月額）は300,000円以上。
    def self.high_reward?(posting)
      reward_text = posting.reward.to_s
      amount = FreelanceJobs::Classifier.first_reward_amount(reward_text)
      return false if amount.nil?

      reward_text.match?(/時給|時間単価|時間報酬/) ? amount >= 4_000 : amount >= 300_000
    end

    def self.build_memo(posting, judgement_text, years, hint_memo, suspicious)
      parts = []
      parts << "リモート可" if judgement_text.match?(REMOTE_RE)
      parts << "長期・継続あり" if judgement_text.match?(LONG_TERM_RE)
      parts << "高単価" if high_reward?(posting)
      parts << "React Native（モバイル）" if judgement_text.match?(REACT_NATIVE_RE)
      parts << "実務経験#{years}年以上" if years
      parts << hint_memo if hint_memo
      parts << "⚠ 募集条件に注意（LINE誘導・高収入訴求など）" if suspicious
      parts << "募集人数に達している可能性" if FreelanceJobs::Classifier.full_capacity?(posting)

      return "条件は案件ページで要確認" if parts.empty?

      parts.join("／")
    end

    # 検出技術ラベル（Rails/RoR検出時は"Ruby on Rails"、Reactは"Next.js"/"React Native"に
    # 細分）＋追加スキルを" / "で連結する。
    def self.build_skills_text(category, judgement_text)
      parts = [detected_technology_label(category, judgement_text)]

      ADDITIONAL_SKILLS.each do |name, regex|
        parts << name if judgement_text.match?(regex)
      end

      parts.join(" / ")
    end

    def self.detected_technology_label(category, judgement_text)
      case category
      when "Ruby"
        judgement_text.match?(RAILS_RE) ? "Ruby on Rails" : "Ruby"
      when "TypeScript"
        "TypeScript"
      when "React"
        if judgement_text.match?(NEXTJS_RE)
          "Next.js"
        elsif judgement_text.match?(REACT_NATIVE_RE)
          "React Native"
        else
          "React"
        end
      end
    end
  end
end
