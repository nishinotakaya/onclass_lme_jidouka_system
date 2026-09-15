# frozen_string_literal: true

module Youtube
  # YouTube の Google OAuth が失効してバッチが止まったことを、メールで「1回だけ」知らせる。
  #
  # Google のアプリがテストモードのままなので refresh_token は約7日で失効し、
  # そのたびに全 YouTube 系バッチが invalid_grant で止まる。失敗はリトライで
  # 何十回も繰り返されるため、素直に通知すると同じメールが大量に届く。
  # そこで「失効を検知したら1通だけ送り、再認証が通ったら次の1通を解禁する」。
  class OauthAlertNotifier
    # 通知済みフラグ（Redis 共有キャッシュ。web と worker のどちらから送っても効く）
    ALERT_SENT_CACHE_KEY = "youtube_oauth_alert_sent"

    # 通知済みフラグの保持期間。これを過ぎても再認証されていなければ改めて1通送る。
    ALERT_SENT_RETENTION = 30.days

    # APP_BASE_URL も YOUTUBE_REDIRECT_URI も無い環境で使う既定のアプリURL。
    DEFAULT_APP_BASE_URL = "https://onclass-lme-jidouka-app-857ffde75fc4.herokuapp.com"

    class << self
      # 失効を検知したときに呼ぶ。送ったら :sent、既に送信済みなら :already_sent。
      def notify_once(reason:)
        new(reason: reason).notify_once
      end

      # 再認証に成功したときに呼ぶ。次に失効したらまた1通送れるようにする。
      def reset!
        Rails.cache.delete(ALERT_SENT_CACHE_KEY)
      end
    end

    def initialize(reason:)
      @reason = reason
    end

    def notify_once
      return :not_configured unless mail_configured?

      # SET NX 相当。複数 worker が同時に失敗しても送信は1通に収束する。
      return :already_sent unless claim_alert_slot!

      YoutubeOauthMailer.expired(authorize_url: authorize_url, reason: @reason).deliver_now
      Rails.logger.warn("[YouTubeOAuth] 失効通知メールを送信しました reason=#{@reason}")
      :sent
    rescue => e
      # 送信に失敗したらフラグを戻し、次の失敗で再挑戦できるようにする。
      # また、通知の失敗で本来の認証エラーを覆い隠さない（例外は投げ直さない）。
      Rails.cache.delete(ALERT_SENT_CACHE_KEY)
      Rails.logger.error("[YouTubeOAuth] 失効通知メールの送信に失敗: #{e.class}: #{e.message}")
      :failed
    end

    private

    def claim_alert_slot!
      Rails.cache.write(
        ALERT_SENT_CACHE_KEY,
        Time.current.iso8601,
        expires_in: ALERT_SENT_RETENTION,
        unless_exist: true
      )
    end

    def mail_configured?
      return true if ENV["SMTP_USERNAME"].present?

      Rails.logger.error("[YouTubeOAuth] SMTP_USERNAME が未設定のため失効通知メールを送れません")
      false
    end

    # 例: YOUTUBE_REDIRECT_URI="https://example.herokuapp.com/oauth2callback"
    #     → "https://example.herokuapp.com/youtube/oauth/authorize"
    def authorize_url
      "#{app_base_url}/youtube/oauth/authorize"
    end

    # このメールは「再認証URLを届けること」が目的なので、URLが作れないと通知の意味がなくなる。
    # ENVが未設定でも本番URLへフォールバックし、パスだけのURLは絶対に送らない。
    def app_base_url
      base_url = ENV["APP_BASE_URL"].presence ||
                 ENV["YOUTUBE_REDIRECT_URI"].to_s.sub(%r{/oauth2callback\z}, "").presence ||
                 DEFAULT_APP_BASE_URL
      base_url.chomp("/")
    end
  end
end
