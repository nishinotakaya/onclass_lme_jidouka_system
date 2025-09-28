# app/jobs/onclass_mentions_worker.rb
module Onclass
  class MentionsWorker
    include Sidekiq::Worker
    sidekiq_options queue: :onclass_comunity_Mentions, retry: 3

    TARGET_CHANNEL_ID   = ENV.fetch("ONCLASS_CHANNEL_ID", "oyIDI6g2Y").freeze
    MANAGER_COMMUNITY_URL = "https://manager.the-online-class.com/community".freeze

    def perform
      creds = OnclassAuthClient.credentials_from_env
      if creds.empty?
        Rails.logger.warn("[OnclassMentionsWorker] No credentials found in ENV.")
        return
      end

      total = 0

      creds.each do |cred|
        client  = OnclassAuthClient.new(email: cred[:email], password: cred[:password])
        headers = client.headers

        mentions = get_mentions(headers)

        # メンションの未読のみ抽出
        unread = mentions.select { |m|
          m["is_read"] == false && m.dig("chat", "channel", "id")
        }

        # ← ここを追加：created_at 昇順（古い→新しい）
        unread.sort_by! do |m|
          s = m["created_at"].to_s
          (Time.zone ? Time.zone.parse(s) : Time.parse(s))
        rescue
          Time.at(0)
        end

        if unread.any?
          unread.each { |m| notify_line_mention(m, account: cred[:email]) }
          Rails.logger.info("[OnclassMentionsWorker] account=#{cred[:email]} sent #{unread.size} unread mention(s) to LINE")
          total += unread.size
        else
          Rails.logger.info("[OnclassMentionsWorker] account=#{cred[:email]}: No unread mentions found.")
        end
      end

      Rails.logger.info("[OnclassMentionsWorker] total sent #{total} mention(s) across #{creds.size} account(s)")

    rescue Faraday::Error => e
      Rails.logger.error("[OnclassMentionsWorker] HTTP error: #{e.class} #{e.message}")
      raise
    rescue => e
      Rails.logger.error("[OnclassMentionsWorker] unexpected error: #{e.class} #{e.message}")
      raise
    end

    private

    def get_mentions(headers)
      res = Faraday.get("https://api.the-online-class.com/v1/enterprise_manager/communities/activity/mentions") do |req|
        req.headers.merge!(headers)
        req.headers["Accept"]       = "application/json"
        req.headers["Content-Type"] = "application/json"
      end
      JSON.parse(res.body)["data"] || []
    end

    def notify_line_mention(mention, account: nil)
      ch_name   = mention.dig("chat", "channel", "name")
      user_name = mention.dig("chat", "user_name")
      text      = mention.dig("chat", "text").to_s
      created   = mention["created_at"]

      title = "【チャンネル名: #{ch_name}】投稿者: #{user_name}"

      ts  = (Time.zone ? Time.zone.parse(created) : Time.parse(created)).strftime("%Y-%m-%d %H:%M")
      to_names = Array(mention.dig("chat", "mention_targets")).map { |t| t["name"] }.join(", ")

      meta = "投稿日時: #{ts}" \
            + (to_names.empty? ? "" : "\nメンション: #{to_names}") \
            + "\nURL: #{MANAGER_COMMUNITY_URL}"

      body = <<~MSG
        #{title}
        #{meta}

        #{text}
      MSG

      Lme::LineNotifier.push(body)
    end
  end
end
