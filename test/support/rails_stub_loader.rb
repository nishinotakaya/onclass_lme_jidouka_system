# frozen_string_literal: true

# Rails を起動せずに通知まわり（BatchAlerts / 各 AlertNotifier）を読むための土台。
# （bundler 経由の Rails 起動はローカルで mysql2 が入らず動かないため、
#  必要な依存だけを差し替えて単体で読む。）
require "active_support/all"

Time.zone ||= "UTC"

# Rails.cache / Rails.logger だけを使うので、その2つを差し替え可能な形で用意する。
module Rails
  class << self
    attr_accessor :cache, :logger
  end
end

# Rails.cache の代役。unless_exist の「既にあれば書かない」だけ再現できればよい。
class MemoryCacheStub
  def initialize
    @entries = {}
  end

  def read(key)
    @entries[key]
  end

  def write(key, value, unless_exist: false, **_options)
    return false if unless_exist && @entries.key?(key)

    @entries[key] = value
    true
  end

  def delete(key)
    @entries.delete(key)
    true
  end

  def key?(key)
    @entries.key?(key)
  end
end

# ログは内容を検証しないので捨てる。
class NullLoggerStub
  def warn(*) = nil
  def error(*) = nil
end

Rails.cache = MemoryCacheStub.new
Rails.logger = NullLoggerStub.new

require File.expand_path("../../app/services/batch_alerts/once_notifier", __dir__)
