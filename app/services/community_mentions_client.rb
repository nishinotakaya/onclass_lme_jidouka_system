# frozen_string_literal: true
require "faraday"
require "json"
require "time"

class CommunityMentionsClient
  BASE_PATH = "/v1/enterprise_manager/communities".freeze

  def initialize(auth: OnclassAuthClient.new)
    @auth = auth
    @conn = Faraday.new(url: ENV.fetch("ONLINE_CLASS_API_BASE", "https://api.the-online-class.com")) do |f|
      f.request :url_encoded
      f.response :raise_error
      f.adapter Faraday.default_adapter
    end
  end

  def fetch_mentions
    with_auth do |headers|
      @conn.get("#{BASE_PATH}/activity/mentions", {}, headers)
    end
  end

  # 401の時は一度だけ再ログインして再試行
  def with_auth
    headers = base_headers.merge(@auth.headers)
    begin
      res = yield(headers)
      @auth.refresh_from_response!(res) # トークン更新対応
      JSON.parse(res.body, symbolize_names: true)[:data] || []
    rescue Faraday::UnauthorizedError
      headers = base_headers.merge(@auth.sign_in!)
      res = yield(headers)
      @auth.refresh_from_response!(res)
      JSON.parse(res.body, symbolize_names: true)[:data] || []
    end
  end

  def filter_mentions(since_time:)
    fetch_mentions.filter_map do |m|
      created_at = Time.parse(m[:created_at].to_s)
      next unless (m[:is_read] == false) || (created_at > since_time)
      chat = m[:chat] || {}
      channel = chat[:channel] || {}
      {
        user_name: chat[:user_name],
        mention_targets: (chat[:mention_targets] || []).map { |t| t[:name] },
        text: chat[:text],
        created_at: created_at,
        is_read: m[:is_read],
        channel_name: channel[:name],
        chat_id: chat[:id]
      }
    end.sort_by { _1[:created_at] }
  end

  private

  def base_headers
    { "Accept" => "application/json, text/plain, */*", "User-Agent" => "onclass-mentions-bot/1.0" }
  end
end
