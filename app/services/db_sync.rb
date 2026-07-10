# frozen_string_literal: true

# シート運用と並行してDB(Supabase)へ差分書き込みするための共通ヘルパ。
# 既存レコードと属性を比較し、「新規(INSERT) or 値が変わった行(UPDATE)」だけ upsert する。
# 変わっていない行は書かない → updated_at が「実際にデータが変わった日時」になる。
module DbSync
  module_function

  # model:  ApplicationRecord のクラス
  # records: upsert したい属性ハッシュの配列（キーはシンボル）
  # key:    一意キー。単一(:video_id) or 複合([:user_id, :course_id])
  # unique_by: upsert_all に渡す一意制約（省略時は key をそのまま使用）
  # 戻り値: 実際に書き込んだ件数
  def diff_upsert(model, records, key:, unique_by: nil)
    records = records.reject { |record| Array(key).any? { |k| record[k].blank? } }
    return 0 if records.empty?

    changed = changed_records(model, records, key: key)
    return 0 if changed.empty?

    model.upsert_all(changed, unique_by: unique_by || key, record_timestamps: true)
    changed.size
  end

  def changed_records(model, records, key:)
    key_columns = Array(key)
    scope = model.all
    key_columns.each do |column|
      scope = scope.where(column => records.map { |record| record[column] }.uniq)
    end
    existing = scope.index_by { |db_row| key_columns.map { |column| db_row.public_send(column).to_s } }

    records.select do |record|
      db_row = existing[key_columns.map { |column| record[column].to_s }]
      next true if db_row.nil? # 新規 → INSERT

      record.any? { |column, value| normalize(db_row.public_send(column)) != normalize(value) }
    end
  end

  # DB値と比較用の正規化（Date/Time/数値/nil空文字の揺れを吸収）
  def normalize(value)
    case value
    when Date, Time, DateTime then value.to_time.utc.iso8601
    when Numeric then value.to_s
    else value.presence.to_s.presence
    end
  rescue
    value.to_s
  end
end
