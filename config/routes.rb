Rails.application.routes.draw do
  # Sidekiq Web UI
  require "sidekiq/web"
  mount Sidekiq::Web => "/sidekiq"

  # ダッシュボード
  resource :dashboard, only: :show
  post "/jobs/run",     to: "dashboard#run_job"
  get  "/jobs/statuses", to: "dashboard#statuses"
  root "dashboard#show"

  # LME
  namespace :lme do
    resources :broadcasts, only: %i[new create]
  end

  # === YouTube OAuth ===
  namespace :youtube do
    # /youtube/oauth/authorize → Youtube::OauthController#authorize
    get "oauth/authorize", to: "oauth#authorize"
  end

  # Google のリダイレクトURI（GCP コンソールに登録してるやつ）
  # http://localhost:3008/oauth2callback
  get "/oauth2callback", to: "youtube/oauth#callback"

  if Rails.env.development?
    get "/.well-known/appspecific/com.chrome.devtools.json",
      to: proc { [204, { "Content-Type" => "application/json" }, [""]] }
  end
end
