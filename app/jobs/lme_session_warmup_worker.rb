# 例: app/jobs/lme_session_warmup_worker.rb
class LmeSessionWarmupWorker
  include Sidekiq::Worker
  sidekiq_options queue: 'lme_session_warmup', retry: 0

  def perform
    auth = LmeAuthClient.new
    
    # 自動リフレッシュ機能付きでクッキー検証
    ok = auth.valid_cookie_with_refresh?
    return true if ok

    # 環境変数で「厳格モード」切替（デフォルト true）
    strict = ENV.fetch('LME_WARMUP_STRICT', 'true') == 'true'
    if strict
      raise "LME cookie expired and automatic refresh failed. Please paste a fresh cookie to ENV LME_COOKIE or via LmeAuthClient#manual_set!"
    else
      Rails.logger.warn("[LME] cookie check failed, but continuing due to LME_WARMUP_STRICT=false")
      true
    end
  end
end
