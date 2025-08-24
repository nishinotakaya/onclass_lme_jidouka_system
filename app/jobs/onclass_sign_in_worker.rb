# frozen_string_literal: true

class OnclassSignInWorker
  include Sidekiq::Worker
  sidekiq_options queue: :default, retry: 3

  # 使い方:
  # OnclassSignInWorker.perform_async
  # OnclassSignInWorker.new.perform
  def perform
    client = OnclassAuthClient.new
    headers = client.sign_in!
    masked = headers.merge(
      "access-token" => mask(headers["access-token"]),
      "client"       => mask(headers["client"]),
      "uid"          => headers["uid"]
    )
    Rails.logger.info("[OnclassSignInWorker] login success headers=#{masked.inspect}")
    true
  rescue Faraday::Error => e
    Rails.logger.error("[OnclassSignInWorker] HTTP error: #{e.class} #{e.message}")
    raise
  rescue => e
    Rails.logger.error("[OnclassSignInWorker] unexpected error: #{e.class} #{e.message}")
    raise
  end

  private

  def mask(str)
    return str if str.nil? || str.length < 8
    "#{str[0,4]}...#{str[-4,4]}"
  end
end
