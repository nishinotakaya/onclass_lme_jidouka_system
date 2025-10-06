Rails.application.routes.draw do
  # Health check

  # Sidekiq Web UI
  require "sidekiq/web"
  mount Sidekiq::Web => "/sidekiq"

  resource :dashboard, only: :show
  post "/jobs/run", to: "dashboard#run_job"
  root "dashboard#show"
  get "/jobs/statuses", to: "dashboard#statuses"

  if Rails.env.development?
    get '/.well-known/appspecific/com.chrome.devtools.json',
      to: proc { [204, { 'Content-Type' => 'application/json' }, ['']] }
  end

end
