# config/initializers/sidekiq.rb
require "erb"
require "yaml"
require "openssl"
require "sidekiq"
require "sidekiq/web"
begin
  require "sidekiq/cron/web" # cronUIタブ
rescue LoadError
end

module SidekiqRedisConfig
  def self.opts
    url = ENV["REDIS_URL"]&.strip
    raise "ENV REDIS_URL is not set" if url.nil? || url.empty?

    opts = { url: url }
    if url.start_with?("rediss://") && ENV["REDIS_SSL_SKIP_VERIFY"] == "1"
      opts[:ssl_params] = { verify_mode: OpenSSL::SSL::VERIFY_NONE }
    end
    opts
  end
end

Sidekiq.configure_server do |config|
  config.redis = SidekiqRedisConfig.opts

  config.on(:startup) do
    # ── sidekiq-scheduler: 環境別スケジュールファイルを読み込む ──
    env_file  = Rails.root.join("config", "scheduler_#{Rails.env}.yml")
    prod_file = Rails.root.join("config", "scheduler_production.yml")
    schedule_file = File.exist?(env_file) ? env_file : prod_file

    if File.exist?(schedule_file)
      yaml     = ERB.new(File.read(schedule_file)).result
      schedule = YAML.safe_load(yaml, permitted_classes: [Symbol], aliases: true) || {}
      Sidekiq.schedule = schedule
      SidekiqScheduler::Scheduler.instance.reload_schedule!
      Sidekiq.logger.info "Loaded sidekiq-scheduler from #{schedule_file} (#{schedule.keys.size} jobs)"
    else
      Sidekiq.logger.warn "Schedule file not found: #{schedule_file}"
    end

    # ── sidekiq-cron: LOAD_SIDEKIQ_CRON=1 の時だけ有効 ──
    if ENV["LOAD_SIDEKIQ_CRON"] == "1"
      cron_path = Rails.root.join("config", "sidekiq-cron.yml")
      if File.exist?(cron_path)
        yaml = ERB.new(File.read(cron_path)).result
        jobs = YAML.safe_load(yaml, permitted_classes: [Symbol], aliases: true) || {}
        Sidekiq::Cron::Job.load_from_hash(jobs)
        Sidekiq.logger.info "Loaded sidekiq-cron jobs: #{jobs.keys.join(', ')}"
      else
        Sidekiq.logger.warn "Cron file not found: #{cron_path}"
      end
    end
  end
end

Sidekiq.configure_client do |config|
  config.redis = SidekiqRedisConfig.opts
end
