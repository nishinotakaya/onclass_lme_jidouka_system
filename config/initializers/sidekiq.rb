# config/initializers/sidekiq.rb
require "erb"
require "yaml"
require "sidekiq"
require "sidekiq/web"
require "sidekiq/cron/web"  # UIでCronタブを見たい場合

def sidekiq_redis_options
  url = ENV["SIDEKIQ_REDIS_URL"].presence || ENV["REDIS_URL"]
  url ? { url: url } : {}
end

Sidekiq.configure_server do |config|
  config.redis = sidekiq_redis_options

  config.on(:startup) do
    cron_path = Rails.root.join("config", "sidekiq-cron.yml")
    if File.exist?(cron_path)
      yaml  = ERB.new(File.read(cron_path)).result
      jobs  = YAML.safe_load(yaml, permitted_classes: [Symbol], aliases: true) || {}
      Sidekiq::Cron::Job.load_from_hash(jobs)
      Sidekiq.logger.info "Loaded cron jobs: #{jobs.keys.join(', ')}"
    else
      Sidekiq.logger.warn "Cron file not found: #{cron_path}"
    end
  end
end

Sidekiq.configure_server do |config|
  config.redis = {
    url: ENV.fetch('SIDEKIQ_REDIS_URL', ENV.fetch('REDIS_URL', nil)),
    ssl_params: { verify_mode: OpenSSL::SSL::VERIFY_NONE }
  }
end

Sidekiq.configure_client do |config|
  config.redis = {
    url: ENV.fetch('SIDEKIQ_REDIS_URL', ENV.fetch('REDIS_URL', nil)),
    ssl_params: { verify_mode: OpenSSL::SSL::VERIFY_NONE }
  }
end

Sidekiq.configure_client do |config|
  config.redis = sidekiq_redis_options
end
