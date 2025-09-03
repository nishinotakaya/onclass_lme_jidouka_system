# frozen_string_literal: true
require "uri"
require "set"

module Lme
  class UserTagsService < BaseService
    include ProakaConfig

    CHAT_REFERER = "#{LmeAuthClient::BASE_URL}/basic/chat-v3?lastTimeUpdateFriend=0"

    def categories_tags(conn, line_user_id, bot_id:, is_all_tag: 0)
      apply_form_headers!(conn, referer: CHAT_REFERER)

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
      Rails.logger.warn("[LME] categories_tags(#{line_user_id}) failed: #{e.class} #{e.message}")
      []
    end

    def flags_for(conn, line_user_id, bot_id:)
      flags_from_categories(categories_tags(conn, line_user_id, bot_id: bot_id))
    end

    def build_flags_cache(line_user_ids, bot_id:, threads: Integer(ENV["LME_TAG_THREADS"] || 6))
      ids   = Array(line_user_ids).compact.uniq
      cache = {}
      q     = Queue.new
      ids.each { |i| q << i }

      workers = Array.new([threads, 1].max) do
        Thread.new do
          local_conn = auth.conn
          while (uid = q.pop(true) rescue nil)
            cache[uid] = flags_for(local_conn, uid, bot_id: bot_id)
          end
        rescue ThreadError
        end
      end
      workers.each(&:join)
      cache
    end

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
  end
end
