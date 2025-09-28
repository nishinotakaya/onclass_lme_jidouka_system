require "sidekiq/web"
require "sidekiq-scheduler/web"

# config/routes.rb
Rails.application.routes.draw do
  # ヘルスチェック用
  get "/" => proc { [200, {"Content-Type" => "text/plain"}, ["ok"]] }

  # （Sidekiq Web UI を使うなら）
  require "sidekiq/web"
  mount Sidekiq::Web => "/sidekiq"
end
