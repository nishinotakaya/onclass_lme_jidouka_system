# config/initializers/sidekiq.rb
require "erb"
require "yaml"
require "uri"
require "sidekiq"
require "sidekiq/web"
require "sidekiq-scheduler"
require "sidekiq-scheduler/web"

schedule_file = Rails.root.join('config/sidekiq-cron.yml')
if File.exist?(schedule_file)
  Sidekiq::Cron::Job.load_from_hash YAML.load_file(schedule_file)
end

# ENV から URL を拾う（SIDEKIQ_REDIS_URL 優先、なければ REDIS_URL）
def sidekiq_redis_options
  url = ENV["SIDEKIQ_REDIS_URL"].presence || ENV["REDIS_URL"]
  url ? { url: url } : {}
end

Sidekiq.configure_server do |config|
  config.redis = sidekiq_redis_options

  config.on(:startup) do
    schedule_file = Rails.root.join("config", "scheduler_#{Rails.env}.yml")

    unless File.exist?(schedule_file)
      Sidekiq.logger.warn "Schedule file not found: #{schedule_file}"
      next
    end

    begin
      yaml     = ERB.new(File.read(schedule_file)).result
      schedule = YAML.safe_load(yaml, permitted_classes: [Symbol], aliases: true) || {}

      if schedule.is_a?(Hash) && schedule.any?
        Sidekiq.schedule = schedule
        Sidekiq::Scheduler.reload_schedule!
        Sidekiq.logger.info "Loaded schedule entries: #{schedule.keys.join(', ')}"
      else
        Sidekiq.logger.warn "Schedule file present but empty or invalid: #{schedule_file}"
      end
    rescue => e
      Sidekiq.logger.error "Failed to load schedule: #{e.class} #{e.message}"
    end
  end
end

Sidekiq.configure_client do |config|
  config.redis = sidekiq_redis_options
end
