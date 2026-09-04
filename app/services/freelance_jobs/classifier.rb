# frozen_string_literal: true

module FreelanceJobs
  # JobPosting 1件を分類する純粋関数群（通信・時刻取得なし）。
  class Classifier
    Result = Struct.new(:category, :difficulty, :recommend, :memo, :skills_text, keyword_init: true)

    HTML_CSS_RE = /HTML|CSS|コーディング|LP\b|ランディングページ|Web ?サイト|ウェブサイト|ホームページ|WordPress|ワードプレス|静的サイト|レスポンシブ|Bootstrap|Tailwind|Figma|バナー修正/i
    EXCEL_RE = /Excel|エクセル|スプレッドシート|Spreadsheet|Google ?Sheets|データ入力|データ整理|データ収集|データ作成|リスト(作成|化|アップ)|集計|表計算|VBA|マクロ|ピボット|CSV|転記|入力作業/i

    # category_hintが"HTML/CSS"でも本文がHTML_CSS_RE/EXCEL_RE双方に一致しない場合の最終ゲート。
    # （実例: 「Instagramバナー制作」「マーケティングディレクター大募集」等の誤爆防止。ラウンド2 C1）
    WEB_WEAK_RE = /ページ|サイト|HP|ホームページ|LP|コーディング|HTML|CSS|Web|ウェブ|WordPress|ランディング/i

    BEGINNER_RE = /初心者|未経験|簡単|かんたん|カンタン|誰でも|スキル不要|経験不問|マニュアル(あり|完備)|スマホ(OK|可)|丁寧に(教え|説明)/
    ADVANCED_RE = /実務経験|経験者|上級|React|Vue|Next\.js|PHP|Laravel|VBA|マクロ|API|設計|要件定義|Python|GAS|Apps Script|SQL|自動化|Shopify|フルスタック/i

    # ★★☆(基礎があれば可)のうち🌟を出す対象を判定する（ラウンド2 C9）。
    # classify_difficultyはBEGINNER_RE一致を必ず★☆☆にするため、★★☆側でBEGINNER_RE一致を
    # 条件にすると到達不能になる。代わりに「取っつきやすさ」を示す別の緩い表現で判定する。
    SOFT_FRIENDLY_RE = /歓迎|OK|可能|継続|マニュアル|丁寧|サポート|相談|長期/

    SUSPICIOUS_RE = /月\s*[0-9０-９]+\s*万|高収入|副業で稼|誰でも.*万円|LINE(登録|追加|に)|ライン(登録|追加)|外部(サイト|ツール)(に|へ)登録|まずは(LINE|ライン)|モニター/

    # 未経験向けデータ入力なのに固定報酬が高額（テンプレ/誘導系の可能性）を検出する（ラウンド2 C8）。
    # 時給表記（reward に"時給"を含む）は対象外。
    HIGH_REWARD_DATA_ENTRY_TITLE_RE = /データ入力|入力|チェック|確認|事務/
    HIGH_REWARD_DATA_ENTRY_THRESHOLD = 100_000

    MEMO_TAG_CANDIDATES = ["初心者歓迎", "スキル不要", "マニュアルあり", "継続発注あり"].freeze
    MEMO_MAX_LENGTH = 140
    DEADLINE_SOON_DAYS = 3

    SITE_MEMOS = {
      "ママワークス" => "求人型（選考あり）・長期向き",
      "ココナラ（公開依頼）" => "公開依頼は提案制（実績ゼロでも提案可）",
      "クラウディア" => "手数料低め・競争少なめ",
      "シュフティ" => "単価は低めだが実績作り向き"
    }.freeze

    def self.classify(posting, today:)
      judgement_text = build_judgement_text(posting)
      category = classify_category(posting, judgement_text)
      return Result.new(category: nil) if category.nil?

      difficulty = classify_difficulty(judgement_text)
      suspicious = judgement_text.match?(SUSPICIOUS_RE)
      high_reward_data_entry_suspicious = high_reward_data_entry_suspicious?(posting, category, judgement_text)

      Result.new(
        category: category,
        difficulty: difficulty,
        recommend: classify_recommend(difficulty, suspicious || high_reward_data_entry_suspicious, judgement_text),
        memo: build_memo(posting, difficulty, suspicious, high_reward_data_entry_suspicious, today),
        skills_text: build_skills_text(posting, category, judgement_text)
      )
    end

    def self.build_judgement_text(posting)
      [posting.title, posting.description, Array(posting.skills).join(" "), Array(posting.tags).join(" ")].join(" ")
    end

    def self.classify_category(posting, judgement_text)
      title = posting.title.to_s
      return "HTML/CSS" if title.match?(HTML_CSS_RE)
      return "HTML/CSS" if judgement_text.match?(HTML_CSS_RE) && !judgement_text.match?(EXCEL_RE)
      return "Excel・スプレッドシート" if judgement_text.match?(EXCEL_RE)

      # ここに来るのは本文(タイトル+説明+スキル+タグ)がHTML_CSS_RE/EXCEL_REのどちらにも
      # 一致しなかった場合。category_hintだけを根拠にHTML/CSS判定すると誤爆するため、
      # タイトル＋説明がWEB_WEAK_REに一致するときだけ許可する（ラウンド2 C1）。
      return nil unless posting.category_hint == "HTML/CSS"

      title_and_description = "#{posting.title} #{posting.description}"
      title_and_description.match?(WEB_WEAK_RE) ? "HTML/CSS" : nil
    end

    def self.classify_difficulty(judgement_text)
      return "★★★ 経験者向け" if judgement_text.match?(ADVANCED_RE)
      return "★☆☆ 未経験OK" if judgement_text.match?(BEGINNER_RE)

      "★★☆ 基礎があれば可"
    end

    def self.classify_recommend(difficulty, suspicious, judgement_text)
      return "" if suspicious
      return "🌟🌟" if difficulty == "★☆☆ 未経験OK"
      return "🌟" if difficulty == "★★☆ 基礎があれば可" && judgement_text.match?(SOFT_FRIENDLY_RE)

      ""
    end

    # Excel・スプレッドシート分類 かつ 固定報酬10万円以上 かつ タイトルがデータ入力系
    # かつ BEGINNER_RE一致、の場合にテンプレ/誘導系の疑いありとして扱う（ラウンド2 C8）。
    # 時給表記（rewardに"時給"を含む）は対象外。
    def self.high_reward_data_entry_suspicious?(posting, category, judgement_text)
      return false unless category == "Excel・スプレッドシート"

      reward_text = posting.reward.to_s
      return false if reward_text.include?("時給")

      amount = first_reward_amount(reward_text)
      return false if amount.nil? || amount < HIGH_REWARD_DATA_ENTRY_THRESHOLD
      return false unless posting.title.to_s.match?(HIGH_REWARD_DATA_ENTRY_TITLE_RE)

      judgement_text.match?(BEGINNER_RE)
    end

    # reward文字列から先頭の金額を取り出す（3桁区切りカンマは除去）。数値が無ければnil。
    def self.first_reward_amount(reward_text)
      match = reward_text.delete(",").match(/\d+/)
      match ? match[0].to_i : nil
    end

    def self.build_memo(posting, difficulty, suspicious, high_reward_data_entry_suspicious, today)
      parts = []

      matched_tags = MEMO_TAG_CANDIDATES & Array(posting.tags)
      parts << matched_tags.join("・") unless matched_tags.empty?

      if posting.deadline_on && today && posting.deadline_on <= today + DEADLINE_SOON_DAYS
        parts << "締切間近（#{posting.deadline_text}）"
      end

      parts << "募集枠は埋まっている可能性あり" if posting.site == "CrowdWorks" && full_capacity?(posting)
      parts << "タスク形式：応募不要ですぐ作業できる" if posting.work_format == "タスク"

      site_memo = SITE_MEMOS[posting.site]
      parts << site_memo if site_memo

      parts << "⚠ テンプレ的な高額/誘導系の可能性。詳細と発注者評価を要確認" if suspicious
      if high_reward_data_entry_suspicious
        parts << "⚠ 未経験向けなのに高額固定報酬のデータ入力募集。テンプレ/誘導系の可能性、詳細と発注者評価を要確認"
      end
      parts << "PR枠（広告）" if Array(posting.tags).include?("PR")

      parts.join("／")[0, MEMO_MAX_LENGTH]
    end

    def self.full_capacity?(posting)
      match = posting.application_status.to_s.match(/契約\s*(\d+)\/(\d+)人/)
      return false unless match

      num_contracts, hope_number = match.captures.map(&:to_i)
      hope_number.positive? && num_contracts >= hope_number
    end

    def self.build_skills_text(posting, category, judgement_text)
      return Array(posting.skills).join("／") unless Array(posting.skills).empty?

      if category == "HTML/CSS"
        parts = ["HTML/CSSの基本（タグの読み書き）"]
        parts << "JavaScriptの基礎" if judgement_text.match?(/JavaScript|jQuery/i)
        parts << "WordPressの基礎" if judgement_text.match?(/WordPress/i)
        parts << "レスポンシブ対応" if judgement_text.match?(/レスポンシブ/)
        parts.join("／")
      else
        parts = ["Excel／スプレッドシートの基本操作（入力・コピー・簡単な関数）"]
        parts << "VBA・マクロ" if judgement_text.match?(/VBA|マクロ/)
        parts << "Web検索・情報収集" if judgement_text.match?(/リサーチ|情報収集/)
        parts << "正確さ・丁寧さ" if judgement_text.match?(/正確|丁寧/)
        parts.join("／")
      end
    end
  end
end
