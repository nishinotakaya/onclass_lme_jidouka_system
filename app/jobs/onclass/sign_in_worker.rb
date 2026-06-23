# frozen_string_literal: true

module Onclass
  class SignInWorker
    include Sidekiq::Worker
    sidekiq_options queue: :default, retry: 3

    # デフォルト資格情報でサインイン（Cookie セッションを Redis に保存）
    def perform
      client  = Onclass::AuthClient.new
      session = client.sign_in!
      Rails.logger.info(
        "[Onclass::SignInWorker] login success cookie=#{mask(session['cookie'])} csrf=#{mask(session['csrf'])}"
      )
      true
    rescue Faraday::Error => e
      Rails.logger.error("[Onclass::SignInWorker] HTTP error: #{e.class} #{e.message}")
      raise
    rescue => e
      Rails.logger.error("[Onclass::SignInWorker] unexpected error: #{e.class} #{e.message}")
      raise
    end

    # 任意アカウントでサインインして認証ヘッダ(Cookie + CSRF)を取得
    def self.sign_in_headers_for(email:, password:)
      client = Onclass::AuthClient.new(email: email, password: password)
      client.auth_headers(with_csrf: true)
    rescue Faraday::Error => e
      Rails.logger.warn("[Onclass::SignInWorker] sign_in_headers_for(#{email}) error: #{e.class} #{e.message}")
      nil
    rescue => e
      Rails.logger.warn("[Onclass::SignInWorker] sign_in_headers_for(#{email}) error: #{e.class} #{e.message}")
      nil
    end

    private

    def mask(str)
      return str if str.nil? || str.length < 8
      "#{str[0,4]}...#{str[-4,4]}"
    end
  end
end
