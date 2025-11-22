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
    get  "oauth",          to: "oauth#index",     as: :oauth
    get  "oauth/authorize", to: "oauth#authorize"
    post "oauth/run_analytics",   to: "oauth#run_analytics"
    post "oauth/run_competitors", to: "oauth#run_competitors"
  end

  # リダイレクトURI
  get "/oauth2callback", to: "youtube/oauth#callback"

  # Google のリダイレクトURI（GCP コンソールに登録してるやつ）
  # http://localhost:3008/oauth2callback
  get "/oauth2callback", to: "youtube/oauth#callback"

  if Rails.env.development?
    get "/.well-known/appspecific/com.chrome.devtools.json",
      to: proc { [204, { "Content-Type" => "application/json" }, [""]] }
  end
end
