# frozen_string_literal: true

# オンクラス受講生データのDB版（受講生シートへの書き込みは従来どおり継続）。
# 1受講生×1コースで1行。差分がある時だけ更新される。
class CreateOnclassStudents < ActiveRecord::Migration[7.1]
  def change
    create_table :onclass_students do |t|
      t.string  :user_id,   null: false # オンクラス側のユーザーID
      t.string  :course_id, null: false # 学習コースID
      t.string  :name
      t.string  :email
      t.string  :motivation
      t.string  :status                  # 日本語ラベル（要フォロー等）
      t.date    :course_join_date
      t.string  :latest_login_at
      t.string  :course_login_rate
      t.string  :current_category
      t.string  :current_block
      t.string  :current_category_started_at
      t.string  :current_category_scheduled_at
      t.string  :extension_study_date
      t.string  :pdca_url
      t.string  :new_pdca_url
      t.string  :pdca_latest_report
      t.string  :line_url
      t.integer :nishino_mentions_count
      t.integer :kato_mentions_count

      t.timestamps
    end
    add_index :onclass_students, [:user_id, :course_id], unique: true
    add_index :onclass_students, :email
  end
end
