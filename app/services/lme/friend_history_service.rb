# app/services/lme/friend_history_service.rb
# frozen_string_literal: true
module Lme
  class FriendHistoryService < BaseService
    FRIEND_HISTORY_REFERER = "#{LmeAuthClient::BASE_URL}/basic/friendlist/friend-history"

    def overview(conn, start_on:, end_on:)
      apply_json_headers!(conn, referer: FRIEND_HISTORY_REFERER)
      body = { data: { start: start_on, end: end_on }.to_json }
      resp = conn.post('/ajax/init-data-history-add-friend', body.to_json)
      auth.refresh_from_response_cookies!(resp.headers)
      json = safe_json(resp.body)
      arr  = json['data'] || json['result'] || json['records'] || []
      arr  = arr['data'] if arr.is_a?(Hash) && arr['data'].is_a?(Array)
      arr
    rescue Faraday::Error => e
      Rails.logger.warn("[LME] overview failed: #{e.class} #{e.message}")
      []
    end

    def day_details(conn, date:)
      referer = "#{LmeAuthClient::BASE_URL}/basic/overview/friendlist/view-date?date=#{date}"
      apply_json_headers!(conn, referer: referer)

      body = { date: date, tab: 1 }
      resp = conn.post('/ajax/init-data-history-add-friend-by-date', body.to_json)
      auth.refresh_from_response_cookies!(resp.headers)

      json = safe_json(resp.body)
      rv = json['result'] || json['data'] || json
      rv.is_a?(Array) ? rv : Array(rv)
    rescue Faraday::Error => e
      Rails.logger.warn("[LME] day_details(#{date}) failed: #{e.class} #{e.message}")
      []
    end
  end
end
