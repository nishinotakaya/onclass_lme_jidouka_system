# app/services/lme/friend_history_service.rb
# frozen_string_literal: true
require "uri"
require "set"

module Lme
  class FriendHistoryService < BaseService
    include ProakaConfig
    
    FRIEND_HISTORY_REFERER = "#{LmeAuthClient::BASE_URL}/basic/friendlist/friend-history"
    CHAT_REFERER_PATH = "/basic/chat-v3?lastTimeUpdateFriend=0"

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

    # 🚀 新機能：フレンド履歴 + タグ情報を統合取得
    def overview_with_tags(conn, start_on:, end_on:, bot_id:)
      Rails.logger.info("[FriendHistoryService] Starting integrated data fetch...")
      
      # 1. フレンド履歴を取得
      overview_data = overview(conn, start_on: start_on, end_on: end_on)
      
      # 2. 日付ごとの詳細データを取得
      days = extract_days_with_follows(overview_data)
      Rails.logger.info("[FriendHistoryService] Found #{days.size} days with follows")
      
      # 3. 各日の詳細データを取得
      all_rows = []
      days.each do |date|
        detail = day_details(conn, date: date)
        Array(detail).each do |r|
          rec = r.respond_to?(:with_indifferent_access) ? r.with_indifferent_access : r
          lu  = (rec['line_user'] || {})
          lu  = lu.with_indifferent_access if lu.respond_to?(:with_indifferent_access)
          
          all_rows << {
            'date'         => date,
            'followed_at'  => rec['followed_at'],
            'landing_name' => rec['landing_name'],
            'name'         => lu['name'],
            'line_user_id' => rec['line_user_id'],
            'line_id'      => lu['line_id'],
            'is_blocked'   => (rec['is_blocked'] || 0).to_i
          }
        end
      end
      
      # 4. 重複除去
      all_rows.uniq! { |r| [r['line_user_id'], r['followed_at']] }
      
      # 5. タグ情報を並列取得
      Rails.logger.info("[FriendHistoryService] Fetching tags for #{all_rows.size} users...")
      user_ids = all_rows.map { |r| r['line_user_id'] }.compact.uniq
      tags_cache = build_tags_cache(conn, user_ids, bot_id: bot_id)
      
      Rails.logger.info("[FriendHistoryService] ✅ Integrated fetch completed: #{all_rows.size} rows, #{tags_cache.size} users tagged")
      
      {
        rows: all_rows,
        tags_cache: tags_cache
      }
    end

    # タグ情報を並列取得（UserTagsServiceから移植）
    def build_tags_cache(conn, line_user_ids, bot_id:, threads: Integer(ENV["LME_TAG_THREADS"] || 6))
      ids   = Array(line_user_ids).compact.uniq
      cache = {}
      q     = Queue.new
      ids.each { |i| q << i }

      workers = Array.new([threads, 1].max) do
        Thread.new do
          local_conn = auth.conn # スレッド専用
          while (uid = q.pop(true) rescue nil)
            cache[uid] = flags_for_user(local_conn, uid, bot_id: bot_id)
          end
        rescue ThreadError
        end
      end
      workers.each(&:join)
      cache
    end

    # ユーザーのタグフラグを取得
    def flags_for_user(conn, line_user_id, bot_id:)
      flags_from_categories(categories_tags_for_user(conn, line_user_id, bot_id: bot_id))
    end

    # ユーザーのカテゴリタグを取得
    def categories_tags_for_user(conn, line_user_id, bot_id:, is_all_tag: 0)
      # CSRF トークンを取得
      csrf = auth.csrf_token_for(CHAT_REFERER_PATH)
      csrf ||= CGI.unescape(extract_cookie(auth.cookie, "XSRF-TOKEN").to_s)

      conn.headers["Cookie"]           = auth.cookie
      conn.headers["Accept"]           = "*/*"
      conn.headers["x-requested-with"] = "XMLHttpRequest"
      conn.headers["x-csrf-token"]     = csrf
      conn.headers["Referer"]          = "#{LmeAuthClient::BASE_URL}#{CHAT_REFERER_PATH}"

      form = URI.encode_www_form(
        line_user_id: line_user_id,
        is_all_tag:   is_all_tag,
        botIdCurrent: bot_id.to_s
      )

      resp = conn.post("/basic/chat/get-categories-tags") do |req|
        req.headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
        req.body = form
      end
      auth.refresh_from_response_cookies!(resp.headers)

      json = safe_json(resp.body)
      arr  = json["data"] || json["result"] || []
      arr.is_a?(Array) ? arr : []
    rescue Faraday::Error => e
      Rails.logger.warn("[FriendHistoryService] categories_tags(#{line_user_id}) failed: #{e.class} #{e.message}")
      []
    end

    # カテゴリからフラグを抽出
    def flags_from_categories(categories)
      target = Array(categories).find { |c| (c["id"] || c[:id]).to_i == PROAKA_CATEGORY_ID }
      return empty_flags unless target

      tag_list  = Array(target["tags"] || target[:tags]).compact
      tag_ids   = tag_list.map  { |t| (t["tag_id"] || t[:tag_id]).to_i }.to_set
      tag_names = tag_list.map { |t| (t["name"]   || t[:name]).to_s }.to_set
      selected  = RICHMENU_SELECT_NAMES.find { |nm| tag_names.include?(nm) }

      {
        v1:  tag_ids.include?(PROAKA_TAGS[:v1]),
        v2:  tag_ids.include?(PROAKA_TAGS[:v2]),
        v3:  tag_ids.include?(PROAKA_TAGS[:v3]),
        v4:  tag_ids.include?(PROAKA_TAGS[:v4]),
        dv1: tag_names.include?(PROAKA_DIGEST_NAMES[:dv1]),
        dv2: tag_names.include?(PROAKA_DIGEST_NAMES[:dv2]),
        dv3: tag_names.include?(PROAKA_DIGEST_NAMES[:dv3]),
        select: selected
      }
    end

    def empty_flags
      { v1: false, v2: false, v3: false, v4: false, dv1: false, dv2: false, dv3: false, select: nil }
    end

    private

    def extract_days_with_follows(overview)
      case overview
      when Hash
        overview.each_with_object([]) { |(date, stats), acc| acc << date if stats.to_h['followed'].to_i > 0 }.sort
      when Array
        overview.filter_map { |row|
          next unless row.is_a?(Hash)
          (row['followed'] || row[:followed]).to_i > 0 ? (row['date'] || row[:date]).to_s : nil
        }.sort
      else
        []
      end
    end

    def extract_cookie(cookie_str, key)
      return nil if cookie_str.blank?
      cookie_str.split(";").map(&:strip).each do |pair|
        k, v = pair.split("=", 2)
        return v if k == key
      end
      nil
    end
  end
end
