# frozen_string_literal: true

module Youtube
  # YouTube の Google OAuth が失効してバッチが止まったことを、メールで「1回だけ」知らせる。
  #
  # Google のアプリがテストモードのままなので refresh_token は約7日で失効し、
  # そのたびに全 YouTube 系バッチが invalid_grant で止まる。送信の重複抑止は
  # BatchAlerts::OnceNotifier に任せ、ここは「何を・どのURLで直すか」だけを持つ。
  class OauthAlertNotifier
    LOG_TAG = "[YouTubeOAuth]"

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
        BatchAlerts::OnceNotifier.release_delivery_slot(ALERT_SENT_CACHE_KEY)
      end
    end

    def initialize(reason:)
      @reason = reason
    end

    def notify_once
      BatchAlerts::OnceNotifier.deliver_once(
        cache_key: ALERT_SENT_CACHE_KEY,
        retention: ALERT_SENT_RETENTION,
        log_tag: LOG_TAG,
        description: "失効通知メール"
      ) do
        YoutubeOauthMailer.expired(authorize_url: authorize_url, reason: @reason)
      end
    end

    private

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
