# frozen_string_literal: true
require "faraday"
require "json"

module Lme
  class LineNotifier
    MAX_TEXT = 2000 # LINEテキスト上限に合わせて分割送信

    def self.push(text, to: ENV["LINE_PUSH_TO"])
      messages = chunk(text) # [{type:"text", text:"..."}] の配列
      headers  = {
        "Authorization" => "Bearer #{ENV["LINE_CHANNEL_ACCESS_TOKEN"]}",
        "Content-Type"  => "application/json"
      }

      if to.to_s.strip.empty?
        # フォールバック: 全員宛て broadcast
        payload  = { messages: messages }
        endpoint = "https://api.line.me/v2/bot/message/broadcast"
      else
        payload  = { to: to, messages: messages }
        endpoint = "https://api.line.me/v2/bot/message/push"
      end

      Faraday.post(endpoint, payload.to_json, headers)
    end

    def self.chunk(text)
      # 改行保持のまま 2000 文字で分割
      text.scan(/.{1,#{MAX_TEXT}}/m).map { |t| { type: "text", text: t } }
    end
  end
end
