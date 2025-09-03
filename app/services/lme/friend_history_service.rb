# app/services/lme/friend_history_service.rb
# frozen_string_literal: true
module Lme
  class FriendHistoryService < BaseService
    FRIEND_HISTORY_REFERER = "#{LmeAuthClient::BASE_URL}/basic/friendlist/friend-history"

    def overview(conn, start_on:, end_on:)
      apply_json_headers!(conn, referer: FRIEND_HISTORY_REFERER)
      body = { data: { start: start_on, end: end_on }.to_json }
      resp = conn.post("/ajax/init-data-history-add-friend") { |req| req.body = body.to_json }
      auth.refresh_from_response_cookies!(resp.headers)
      json = safe_json(resp.body)
      (json["data"] || json["result"] || json["records"] || []) # 配列想定
    end

     def day_details(conn, date:)
      apply_json_headers!(conn, referer: FRIEND_HISTORY_REFERER)
      body = { date: date, tab: 1 }
      resp = conn.post("/ajax/init-data-history-add-friend-by-date") { |req| req.body = body.to_json }
      auth.refresh_from_response_cookies!(resp.headers)
      json = safe_json(resp.body)
      rv   = json["result"] || json["data"] || json
      rv.is_a?(Array) ? rv : Array(rv)
    end
  end
end
