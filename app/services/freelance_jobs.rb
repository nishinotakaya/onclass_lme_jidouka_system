# frozen_string_literal: true

require "logger"

# 副業案件リサーチ Sidekiq バッチのルート名前空間。
# ローカルPC（Rails未起動）でも require だけで動くよう、
# Rails.logger や ActiveSupport の autoload には依存しない。
module FreelanceJobs
  class FetchError < StandardError; end

  # WAF等のCAPTCHA/チャレンジ応答でアクセス元IPが拒否された場合。リトライしても解消しない。
  class AccessBlockedError < FetchError; end

  def self.logger
    @logger ||= if defined?(::Rails) && ::Rails.respond_to?(:logger) && ::Rails.logger
                  ::Rails.logger
                else
                  Logger.new($stdout)
                end
  end

  # 金額を3桁区切りの文字列にする（小数は切り捨てて整数化）。nil はそのまま nil を返す。
  def self.format_number(value)
    return nil if value.nil?

    integer_value = value.to_i
    integer_value.to_s.reverse.gsub(/(\d{3})(?=\d)/, '\1,').reverse
  end
end
