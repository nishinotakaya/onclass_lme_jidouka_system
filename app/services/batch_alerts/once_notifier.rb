# frozen_string_literal: true

module BatchAlerts
  # 「バッチが止まった」通知メールを、同じ原因で何通も送らないようにする送信ゲート。
  #
  # 失敗はリトライで何度も繰り返されるため、検知するたびに送ると同じメールが大量に届く。
  # そこで Redis 共有キャッシュの SET NX（Rails.cache の unless_exist）で送信枠を1つだけ確保し、
  # 枠を取れた呼び出しだけが実際に送る。枠は expires_in で自然に開放され、復旧を検知したときは
  # release_delivery_slot で即座に開放して次の1通を解禁する。
  class OnceNotifier
    class << self
      # cache_key   : 通知をまとめる単位（原因ごとに変える）
      # retention   : この期間は同じ cache_key の通知を送らない
      # log_tag     : ログ行の先頭に付ける識別子（例: "[YouTubeOAuth]"）
      # description : ログに出す通知の呼び名（例: "失効通知メール"）
      # ブロックは ActionMailer のメールオブジェクトを返すこと。
      def deliver_once(cache_key:, retention:, log_tag:, description:)
        return :not_configured unless smtp_configured?(log_tag)
        return :already_sent unless claim_delivery_slot(cache_key, retention)

        yield.deliver_now
        Rails.logger.warn("#{log_tag} #{description}を送信しました")
        :sent
      rescue StandardError => error
        # 送信に失敗したら枠を返し、次の検知で送り直せるようにする。
        # 通知の失敗で本来のバッチ障害を覆い隠さないよう、例外は投げ直さない。
        release_delivery_slot(cache_key)
        Rails.logger.error("#{log_tag} #{description}の送信に失敗: #{error.class}: #{error.message}")
        :failed
      end

      # 送信枠を開放する（復旧したとき・送信に失敗したとき）。
      def release_delivery_slot(cache_key)
        Rails.cache.delete(cache_key)
      end

      private

      def claim_delivery_slot(cache_key, retention)
        Rails.cache.write(cache_key, Time.current.iso8601, expires_in: retention, unless_exist: true)
      end

      # 通知経路を Google OAuth に依存させないため、送信は SMTP（アプリパスワード）で行う。
      # 未設定の環境では送らない（バッチ本体を壊さないため例外にはしない）。
      def smtp_configured?(log_tag)
        return true if ENV["SMTP_USERNAME"].present?

        Rails.logger.error("#{log_tag} SMTP_USERNAME が未設定のため通知メールを送れません")
        false
      end
    end
  end
end
