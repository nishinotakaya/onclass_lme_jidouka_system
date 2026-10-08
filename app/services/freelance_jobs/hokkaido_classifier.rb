# frozen_string_literal: true

module FreelanceJobs
  # 北海道の出社・ハイブリッドのエンジニア案件だけを残す分類器（純粋関数。通信・時刻取得なし）。
  # 技術はRuby/TypeScript/Reactに限らず「エンジニア案件なら何でも」残す方針（2026-10-08 社長回答）なので、
  # 技術名の検出・難易度・おすすめ度・スキル欄はEngineerClassifierの既存ロジックをそのまま再利用し、
  # ここでは勤務地・勤務形態の判定と、Ruby/TS/Reactに当たらない案件の「その他」分類だけを足す。
  class HokkaidoClassifier
    Result = FreelanceJobs::Classifier::Result

    # 北海道内の主要市は市名付きで足す。「千歳」単独は千歳船橋・千歳烏山・千葉県の千歳駅と衝突するため禁止。
    # 伊達市は福島県にも存在し「北海道」表記が無いと区別できないため、あえて入れない（誤って残すより取りこぼす方を選ぶ）。
    HOKKAIDO_RE = /北海道|札幌|函館市|旭川市|苫小牧市|帯広市|小樽市|千歳市|釧路市|北見市|江別市|室蘭市|北広島市|石狩市|恵庭市|登別市|岩見沢市|北斗市|網走市|稚内市|名寄市|根室市|滝川市/

    # 否定・条件付きの出社表現と「フルリモート案件の場合」節は、判定前に取り除く
    # 節の終端は次の「【」、一覧パーサが付ける「 / ラベル:」、文字列末尾のいずれか。終端を「【」だけにすると、
    # 後ろに「【」が無い案件で末尾の「勤務形態: フルリモート」まで消え、フルリモート案件が出社として混入する。
    # （実フィクスチャに「出社後フルリモートが可能」「【フルリモート案件の場合】必要に応じて都内の出社を…」が実在）。
    # 取り除かないと「出社不要」が出社扱い、「出社後フルリモート」がフルリモート扱いになる。
    NEGATED_ONSITE_RE = /出社(?:は)?(?:不要|なし|無し)|常駐(?:は)?(?:不要|なし|無し)|出社後(?:に)?フルリモート|【フルリモート案件の場合】[\s\S]*?(?=【| \/ (?:必須スキル|歓迎スキル|使用技術|募集職種|勤務地|勤務形態|稼働)[:：]|\z)/

    # フルリモート表記。単独で当たったら「北海道在住でも出社が無い」案件として除外する。
    FULL_REMOTE_RE = /フルリモート|フルリモ|完全リモート|フル在宅|全国どこでも/
    # フルリモート表記と同居していても、出社の要素があれば除外せず残す
    # （例: 「フルリモート（週1日出社）」は実質ハイブリッド）。
    ONSITE_MARKER_RE = /出社|常駐|ハイブリッド|リモート併用|一部リモート|週[\d０-９]日出社/
    # 出社を前提にリモートも認める表記。当たればハイブリッド、当たらなければ出社のみとみなす。
    # 「基本リモート、月1出社」のように出社頻度が少ない表記も実態はハイブリッドなので足している。
    HYBRID_RE = /リモート可|一部リモート|リモート併用|ハイブリッド|週[\d０-９]日出社|併用|基本リモート|リモート中心|リモート主体|月[\d０-９]+回出社|月[\d０-９]+日出社/

    # 勤務地ラベルが無いときのフォールバックで、出張先としての地名は勤務地とみなさない
    # （「東京本社。北海道出張あり」を北海道案件として誤って残さないため）。
    BUSINESS_TRIP_RE = /(?:北海道|札幌)(?:へ|に|への)?出張/

    ONSITE_LABEL = "出社"
    HYBRID_LABEL = "ハイブリッド（リモート併用）"
    OTHER_CATEGORY = "その他"

    # 一覧パーサが description 末尾に置く「勤務地: 北海道 さっぽろ駅 / 勤務形態: …」の値の部分。
    # 区切りは " / " か改行（フリーランスボード・PE-BANK・Hub 共通の慣例）。
    LOCATION_VALUE_RE = %r{勤務地[:：]\s*([^/\n]+?)\s*(?:/|\n|\z)}

    def self.classify(posting, today:)
      title_text = posting.title.to_s
      description_text = posting.description.to_s
      skills_text = Array(posting.skills).join(" ")
      tags_text = Array(posting.tags).join(" ")
      judgement_text = [title_text, description_text, skills_text].join(" ")

      return Result.new(category: nil) if judgement_text.match?(EngineerClassifier::NON_DEV_RE)

      # 勤務形態はタグにも書かれうる（Hubの勤務形態タグ等）ので、タグも含めて見る。
      location_text = [title_text, description_text, tags_text].join(" ")
      return Result.new(category: nil) unless hokkaido_posting?(description_text, location_text)

      work_style_label = work_style_label(location_text.gsub(NEGATED_ONSITE_RE, " "))
      return Result.new(category: nil) if work_style_label.nil?
      return Result.new(category: nil) unless engineer_posting?(judgement_text)

      build_result(posting, title_text, description_text, skills_text, judgement_text, work_style_label)
    end

    # 「勤務地: …」の値が取れればその値だけで判定する。Hub一覧は勤務地が東京都の推薦枠が混ざり、
    # 本文の「北海道・東北地域」等に釣られて誤って残るため（2026-10-08 フィクスチャで東京都7件）。
    # 値が取れないときだけ title/description/tags 全体にフォールバックする。
    def self.hokkaido_posting?(description_text, location_text)
      location_value = extract_location_value(description_text)
      return location_value.match?(HOKKAIDO_RE) if location_value

      location_text.gsub(BUSINESS_TRIP_RE, " ").match?(HOKKAIDO_RE)
    end

    # 「勤務地: …」の値は最後の一致を採る。本文中の「勤務地：都内」より、
    # 一覧パーサが description 末尾に付けたラベルのほうが正確なため。
    def self.extract_location_value(description_text)
      description_text.scan(LOCATION_VALUE_RE).flatten.last
    end

    # 出社 / ハイブリッド / 対象外(nil) の3値。フルリモートは出社要素が無い場合だけ対象外にする。
    def self.work_style_label(location_text)
      if location_text.match?(FULL_REMOTE_RE) && !location_text.match?(ONSITE_MARKER_RE)
        return nil
      end

      location_text.match?(HYBRID_RE) ? HYBRID_LABEL : ONSITE_LABEL
    end

    # 開発系の語・追加スキル・主力3技術のいずれかが本文にあればエンジニア案件とみなす。
    def self.engineer_posting?(judgement_text)
      return true if judgement_text.match?(EngineerClassifier::DEV_STRONG_RE)
      return true if EngineerClassifier::ADDITIONAL_SKILLS.values.any? { |skill_regex| judgement_text.match?(skill_regex) }

      EngineerClassifier::TECHNOLOGIES.any? { |_name, technology_regex| judgement_text.match?(technology_regex) }
    end

    def self.build_result(posting, title_text, description_text, skills_text, judgement_text, work_style_label)
      detected_category, hint_memo = EngineerClassifier.classify_category_with_hint_memo(
        posting, title_text, description_text, skills_text
      )
      category = detected_category || OTHER_CATEGORY

      years = EngineerClassifier.extract_experience_years(judgement_text)
      suspicious = judgement_text.match?(FreelanceJobs::Classifier::SUSPICIOUS_RE)
      engineer_memo = EngineerClassifier.build_memo(posting, judgement_text, years, hint_memo, suspicious)

      Result.new(
        category: category,
        difficulty: EngineerClassifier.classify_difficulty(judgement_text, years),
        recommend: EngineerClassifier.classify_recommend(posting, judgement_text, suspicious),
        memo: build_memo(work_style_label, description_text, engineer_memo),
        skills_text: build_skills_text(posting, category, judgement_text)
      )
    end

    # category が「その他」だと EngineerClassifier は先頭が空のまま " / " で連結する（" / Java"、""）ので、
    # 先頭の区切りを除き、空なら取得元のスキル配列で補う。
    def self.build_skills_text(posting, category, judgement_text)
      detected_text = EngineerClassifier.build_skills_text(category, judgement_text).sub(%r{\A\s*/\s*}, "").strip
      return detected_text unless detected_text.empty?

      Array(posting.skills).join(" / ")
    end

    # 先頭に勤務形態（と取れれば勤務地）を置き、続けて既存のエンジニア向けmemoを連結する。
    # 既存memoが「条件は案件ページで要確認」だけの場合は、勤務形態の後ろに付けても情報が無いので省く。
    def self.build_memo(work_style_label, description_text, engineer_memo)
      location = extract_location_value(description_text)
      head = location ? "#{work_style_label}（#{location}）" : work_style_label
      return head if engineer_memo == "条件は案件ページで要確認"

      "#{head}／#{engineer_memo}"
    end
  end
end
