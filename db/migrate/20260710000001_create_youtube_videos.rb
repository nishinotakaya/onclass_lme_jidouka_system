# frozen_string_literal: true

# YouTube動画とLMEランディングの紐付けを持つ中心テーブル。
# 管理シート「LMEランディング管理」のDB版（シートへの書き込みは従来どおり継続）。
class CreateYoutubeVideos < ActiveRecord::Migration[7.1]
  def change
    create_table :youtube_videos do |t|
      t.string  :video_id, null: false
      t.string  :title
      t.date    :published_on
      t.string  :privacy_status
      t.string  :uland_code       # 概要欄のLME URLのuLand
      t.string  :performer        # 西野/加藤/小松 等
      t.string  :landing_id       # LMEランディングの数値ID
      t.string  :landing_url      # s.lmes.jp/landing-qr/...?uLand=...
      t.string  :status           # seeded/done/not_target 等（管理シートと同じ）
      t.bigint  :view_count
      t.bigint  :like_count

      t.timestamps
    end
    add_index :youtube_videos, :video_id, unique: true
    add_index :youtube_videos, :uland_code
    add_index :youtube_videos, :landing_id
  end
end
