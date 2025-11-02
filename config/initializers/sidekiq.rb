# config/initializers/sidekiq.rb
require "erb"
require "yaml"
require "openssl"
require "sidekiq"
require "sidekiq/web"
begin
  require "sidekiq/cron/web" # UIタブだけ必要な場合は残してOK
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

  # ← Scheduler運用デフォルトは読み込まない。
  #   Sidekiq-Cronを使うときだけ LOAD_SIDEKIQ_CRON=1 を設定して有効化。
  if ENV["LOAD_SIDEKIQ_CRON"] == "1"
    config.on(:startup) do
      cron_path = Rails.root.join("config", "sidekiq-cron.yml")
      if File.exist?(cron_path)
        yaml = ERB.new(File.read(cron_path)).result
        jobs = YAML.safe_load(yaml, permitted_classes: [Symbol], aliases: true) || {}
        Sidekiq::Cron::Job.load_from_hash(jobs)
        Sidekiq.logger.info "Loaded cron jobs: #{jobs.keys.join(', ')}"
      else
        Sidekiq.logger.warn "Cron file not found: #{cron_path}"
      end
    end
  else
    Sidekiq.logger.info "LOAD_SIDEKIQ_CRON != 1 → skip loading sidekiq-cron"
  end
end

Sidekiq.configure_client do |config|
  config.redis = SidekiqRedisConfig.opts
end
