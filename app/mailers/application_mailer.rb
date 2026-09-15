class ApplicationMailer < ActionMailer::Base
  # 差出人は SMTP の認証ユーザー（Gmail は認証ユーザー以外を From にできない）
  default from: -> { ENV.fetch("MAIL_FROM", ENV["SMTP_USERNAME"]) }
  layout "mailer"
end
