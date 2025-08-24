# config/initializers/sidekiq.rb
require "sidekiq"
require "sidekiq-scheduler" # サーバ側で必須
require "sidekiq-scheduler/web" if defined?(Sidekiq::Web)

Sidekiq.configure_server do |config|
  config.redis = { url: ENV.fetch("SIDEKIQ_REDIS_URL", ENV["REDIS_URL"]) }

  config.on(:startup) do
    schedule_file = Rails.root.join("config", "scheduler_#{Rails.env}.yml")
    if File.exist?(schedule_file)
      yaml = ERB.new(File.read(schedule_file)).result
      schedule = YAML.safe_load(yaml, permitted_classes: [Symbol], aliases: true) || {}

      if schedule.is_a?(Hash) && schedule.any?
        Sidekiq.schedule = schedule
        Sidekiq::Scheduler.reload_schedule!
        Sidekiq.logger.info "Loaded schedule entries: #{schedule.keys.join(', ')}"
      else
        Sidekiq.logger.warn "Schedule file present but empty or invalid: #{schedule_file}"
      end
    else
      Sidekiq.logger.warn "Schedule file not found: #{schedule_file}"
    end
  end
end

Sidekiq.configure_client do |config|
  config.redis = { url: ENV.fetch("SIDEKIQ_REDIS_URL", ENV["REDIS_URL"]) }
end
