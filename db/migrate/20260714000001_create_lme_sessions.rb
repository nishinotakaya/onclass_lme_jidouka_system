# frozen_string_literal: true

# LMEログインセッション（Cookie/XSRF/CSRF）の永続保存。
# ログイン成功時に保存し、次回は再利用（有効性はHTTPで検証、無効/期限切れなら再ログイン）。
# これにより毎回のSelenium+2Captchaログインを回避する。
class CreateLmeSessions < ActiveRecord::Migration[7.1]
  def change
    create_table :lme_sessions do |t|
      t.string   :bot_id, null: false
      t.text     :cookie_header
      t.text     :xsrf_header
      t.text     :csrf_meta
      t.string   :basic_url
      t.jsonb    :login_cookies
      t.datetime :last_login_at
      t.datetime :expires_at

      t.timestamps
    end
    add_index :lme_sessions, :bot_id, unique: true
  end
end
