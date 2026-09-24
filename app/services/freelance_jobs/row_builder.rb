# frozen_string_literal: true

module FreelanceJobs
  # JobPosting + Classifier::Result から、スプレッドシート16列分の配列を作る純粋関数。
  class RowBuilder
    HEADER = [
      "🌟おすすめ", "No.", "分類", "案件名", "掲載サイト", "案件URL", "難易度", "内容（要約）",
      "必要スキル", "報酬", "形式", "応募状況（応募数 / 契約状況）", "締切", "一言メモ（おすすめ理由・注意点）", "取得日時",
      "追加日"
    ].freeze

    SUMMARY_MAX_LENGTH = 160

    # 各列の文字数上限（超過は"…"を付けて切る。ラウンド2 C5: ママワークスの報酬欄が
    # 200文字を超える等、サイト側の表示崩れがそのままシートに出るのを防ぐ）。
    TITLE_MAX_LENGTH = 120
    SKILLS_MAX_LENGTH = 120
    REWARD_MAX_LENGTH = 60
    APPLICATION_STATUS_MAX_LENGTH = 40
    DEADLINE_TEXT_MAX_LENGTH = 40
    MEMO_MAX_LENGTH = 140

    # No.列はSheetMergerで採番するため0を入れておく。
    def self.build(posting, classification, now:)
      [
        classification.recommend.to_s,
        0,
        classification.category,
        truncate(posting.title, TITLE_MAX_LENGTH),
        posting.site,
        posting.url,
        classification.difficulty,
        summarize(posting),
        truncate(classification.skills_text, SKILLS_MAX_LENGTH),
        truncate(posting.reward, REWARD_MAX_LENGTH),
        posting.work_format,
        truncate(posting.application_status, APPLICATION_STATUS_MAX_LENGTH),
        truncate(posting.deadline_text, DEADLINE_TEXT_MAX_LENGTH),
        truncate(classification.memo, MEMO_MAX_LENGTH),
        now.strftime("%Y-%m-%d %H:%M"),
        # AC-03: 追加日（時刻を含まない日付のみ）。既存行はSheetMergerが上書きせず保持する。
        now.strftime("%Y-%m-%d")
      ]
    end

    # 内容（要約）: descriptionを160文字＋「…」に整形する。空ならtitleを使う。
    def self.summarize(posting)
      text = posting.description.to_s.strip
      text = posting.title.to_s.strip if text.empty?
      truncate(text, SUMMARY_MAX_LENGTH)
    end

    def self.truncate(text, max_length)
      value = text.to_s
      return value if value.length <= max_length

      "#{value[0, max_length]}…"
    end
  end
end
