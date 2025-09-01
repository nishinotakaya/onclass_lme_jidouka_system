# frozen_string_literal: true
class LmeSessionWarmupWorker
  include Sidekiq::Worker
  sidekiq_options queue: :default, retry: 1

  # 使い方:
  # LmeSessionWarmupWorker.perform_async
  # LmeSessionWarmupWorker.new.perform
  def perform
    auth = LmeAuthClient.new
    # 1) 既存（Redis or ENV）の Cookie で生存確認
    if auth.valid_cookie?
      Rails.logger.info("[LME] cookie OK & warmed up")
      return true
    end

    # 2) ダメなら ENV(LME_COOKIE) を再投入して再試行
    if ENV["LME_COOKIE"].present?
      auth.manual_set!(ENV["LME_COOKIE"])
      ok = auth.valid_cookie?
      raise "LME cookie invalid even after manual_set!" unless ok
      Rails.logger.info("[LME] cookie refreshed from ENV and valid")
      return true
    end

    # 3) それでもダメなら、人間でログインして Cookie を入れ直す必要あり
    raise "LME cookie expired. Please paste a fresh cookie to ENV LME_COOKIE or via LmeAuthClient#manual_set!"
  rescue Faraday::Error => e
    Rails.logger.error("[LmeSessionWarmupWorker] HTTP error: #{e.class} #{e.message}")
    raise
  end
end
