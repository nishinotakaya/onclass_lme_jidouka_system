class OnclassMentionsWorker
  include Sidekiq::Worker
  sidekiq_options queue: :default, retry: 3

  def perform
    OnclassSignInWorker.new.perform
    client  = OnclassAuthClient.new
    headers = client.headers

    mentions = get_mentions(headers)
    unread   = mentions.select { |m| m["is_read"] == false }
    if unread.any?
      unread.each { |m| notify_line(m) }
      Rails.logger.info("[OnclassMentionsWorker] sent #{unread.size} unread mention(s) to LINE")
    else
      Rails.logger.info("[OnclassMentionsWorker] No unread mentions found.")
    end
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

  # ---- ここから送信用 ----
  def notify_line(mention)
    ch_name   = mention.dig("chat", "channel", "name")
    user_name = mention.dig("chat", "user_name")
    text      = mention.dig("chat", "text").to_s # 改行そのまま
    created   = mention["created_at"]

    # タイトル行：チャンネル名＋投稿者名
    title = "【チャンネル名: #{ch_name}】投稿者: #{user_name}"

    # 日時とメンションターゲット（任意）
    ts  = (Time.zone ? Time.zone.parse(created) : Time.parse(created)).strftime("%Y-%m-%d %H:%M")
    to_names = Array(mention.dig("chat", "mention_targets")).map { |t| t["name"] }.join(", ")
    meta = "投稿日時: #{ts}" \
       + (to_names.empty? ? "" : "\nメンション: #{to_names}") \
       + "\nURL: https://manager.the-online-class.com/community"

    body = <<~MSG
      #{title}
      #{meta}

      #{text}
    MSG

    LineNotifier.push(body)
  end
end
