# frozen_string_literal: true

# LMEのQRコードアクション（ランディング）一覧のDB版。
# /ajax/v2/landing の取得結果を同期し、動画・流入との紐付けに使う。
class CreateLmeLandings < ActiveRecord::Migration[7.1]
  def change
    create_table :lme_landings do |t|
      t.string  :landing_id, null: false # LME側の数値ID
      t.string  :name                    # 管理名（例: 西野- <動画タイトル>）
      t.string  :code                    # uLandコード
      t.string  :category_id             # フォルダID（youtube nishino=5464631 等）
      t.string  :link_qr_code            # LMEが返すフルURL
      t.bigint  :total_user_click
      t.bigint  :total_user_friend

      t.timestamps
    end
    add_index :lme_landings, :landing_id, unique: true
    add_index :lme_landings, :code
    add_index :lme_landings, :category_id
  end
end
