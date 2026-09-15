# frozen_string_literal: true

# メール送信（SMTP）設定。
#
# 用途は「バッチが止まったことを知らせる通知」。とくに YouTube の Google OAuth
# 失効通知は、OAuth が死んでいる状況で送る必要があるため、Google OAuth に
# 依存しない SMTP（Gmail のアプリパスワード等）を使う。
#
#   SMTP_USERNAME : 送信元アカウント（例: xxx@gmail.com）
#   SMTP_PASSWORD : アプリパスワード（Googleアカウントの2段階認証で発行）
#
# 未設定の環境では送信を行わない（起動やバッチを壊さないため）。
unless Rails.env.test?
  Rails.application.config.action_mailer.delivery_method     = :smtp
  Rails.application.config.action_mailer.raise_delivery_errors = true
  Rails.application.config.action_mailer.perform_deliveries   = ENV["SMTP_USERNAME"].present?
  Rails.application.config.action_mailer.smtp_settings = {
    address:              ENV.fetch("SMTP_ADDRESS", "smtp.gmail.com"),
    port:                 ENV.fetch("SMTP_PORT", "587").to_i,
    domain:               ENV.fetch("SMTP_DOMAIN", "gmail.com"),
    user_name:            ENV["SMTP_USERNAME"],
    password:             ENV["SMTP_PASSWORD"],
    authentication:       :plain,
    enable_starttls_auto: true
  }
end
