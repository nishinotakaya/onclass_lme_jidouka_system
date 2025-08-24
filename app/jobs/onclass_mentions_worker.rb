# app/jobs/onclass_mentions_worker.rb
class OnclassMentionsWorker
  include Sidekiq::Worker
  sidekiq_options queue: :onclass_comunity_Mentions, retry: 3

  TARGET_CHANNEL_ID = ENV.fetch("ONCLASS_CHANNEL_ID", "oyIDI6g2Y").freeze
  MANAGER_COMMUNITY_URL = "https://manager.the-online-class.com/community".freeze

  def perform
    OnclassSignInWorker.new.perform

    client  = OnclassAuthClient.new
    headers = client.headers

    mentions = get_mentions(headers)

    # ★ 対象チャンネル(oyIDI6g2Y)のメンションは除外（この後 全投稿を別で送るため）
    unread = mentions.select { |m|
      m["is_read"] == false && m.dig("chat", "channel", "id") != TARGET_CHANNEL_ID
    }

    if unread.any?
      unread.each { |m| notify_line_mention(m) }
      Rails.logger.info("[OnclassMentionsWorker] sent #{unread.size} unread mention(s) to LINE")
    else
      Rails.logger.info("[OnclassMentionsWorker] No unread mentions found (excluding #{TARGET_CHANNEL_ID}).")
    end

    # === ★ 対象チャンネルの「全投稿」を2時間窓で → LINE ===
    from, to = compute_channel_window_jst 
    
    chats = get_recent_chats(headers, channel_id: TARGET_CHANNEL_ID, from: from, to: to)
    if chats.any?
      chats.each { |c| notify_line_chat(c) }
      Rails.logger.info("[OnclassMentionsWorker] sent #{chats.size} recent chat(s) to LINE for #{TARGET_CHANNEL_ID} (#{from}..#{to})")
    else
      Rails.logger.info("[OnclassMentionsWorker] No recent chats in window for #{TARGET_CHANNEL_ID} (#{from}..#{to}).")
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

  def notify_line_mention(mention)
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

    LineNotifier.push(body)
  end

  # 2時間窓（常に2h）
  def compute_channel_window_jst(now: Time.current)
    jst = ActiveSupport::TimeZone["Asia/Tokyo"] || Time.zone || ActiveSupport::TimeZone["UTC"]
    t   = now.in_time_zone(jst)
    if t.hour == 7
      from = (t - 1.day).beginning_of_day
      to   = (t - 1.day).end_of_day
    else
      from = t - 2.hours
      to   = t
    end
    [from, to]
  end

  # 指定チャンネルの投稿をページングしながら取得し、時間窓でフィルタ
  def get_recent_chats(headers, channel_id:, from:, to:, max_pages: 5)
    results = []
    1.upto(max_pages) do |page|
      res = Faraday.get("https://api.the-online-class.com/v1/enterprise_manager/communities/chats") do |req|
        req.params["channel_id"] = channel_id
        req.params["page"]       = page
        req.headers.merge!(headers)
        req.headers["Accept"]       = "application/json"
        req.headers["Content-Type"] = "application/json"
      end

      json  = JSON.parse(res.body)
      items = json["data"] || []
      break if items.empty?

      # created_at で 2時間窓抽出
      filtered = items.select do |chat|
        ts = parse_time(chat["created_at"])
        ts && ts >= from && ts <= to
      end
      results.concat(filtered)

      # これ以上古いページは不要なら打ち切り
      oldest_ts = items.map { |c| parse_time(c["created_at"]) }.compact.min
      break if oldest_ts && oldest_ts < from
    end
    results
  end

  def parse_time(str)
    return nil if str.blank?
    Time.zone ? Time.zone.parse(str) : Time.parse(str)
  rescue
    nil
  end

  def notify_line_chat(chat)
    ch_name   = chat.dig("channel", "name")
    user_name = chat["user_name"] || chat["sender_id"]
    text      = chat["text"].to_s
    created   = chat["created_at"]

    title = "【チャンネル名: #{ch_name}】投稿者: #{user_name}"

    ts  = (Time.zone ? Time.zone.parse(created) : Time.parse(created)).strftime("%Y-%m-%d %H:%M")
    channel_id = chat.dig("channel", "id") || TARGET_CHANNEL_ID
    url = "#{MANAGER_COMMUNITY_URL}?channel_id=#{channel_id}"

    meta = "投稿日時: #{ts}\nURL: #{url}"

    body = <<~MSG
      #{title}
      #{meta}

      #{text}
    MSG

    LineNotifier.push(body)
  end
end
