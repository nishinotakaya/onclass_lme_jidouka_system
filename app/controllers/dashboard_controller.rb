# 先頭で必要なら
require "sidekiq/api"

class DashboardController < ApplicationController
  FRIENDLY_TITLES = {
    "onclass_students_front"   => "受講生データ取得（フロント）",
    "onclass_students_backend" => "受講生データ取得（バックエンド）",
    "lme_line_inflows_daily"   => "LINE流入数を集計",
    "lme_line_counts_every"    => "LINEダッシュボード更新"
  }.freeze

  def show
    @jobs = fetch_jobs
  end

  # ▶ クリック実行：JID を Redis に保存
  def run_job
    key = params[:key].to_s
    job = fetch_jobs.find { |j| j[:key] == key }
    return render json: { status: "指定したタスクが見つかりません（key=#{key}）" }, status: :not_found unless job

    klass_name = job[:klass]
    args       = job[:args] || []

    begin
      klass = klass_name.constantize
    rescue NameError
      return render json: { status: "実行クラス未定義: #{klass_name}" }, status: :unprocessable_entity
    end

    jid = klass.perform_async(*args)

    Sidekiq.redis do |r|
      r.set(redis_key(key), { jid: jid, started_at: Time.now.to_i, queue: job[:queue] }.to_json, ex: 3600)
    end

    render json: { status: "成功: 起動しました", jid: jid }
  rescue => e
    render json: { status: "エラー: #{e.message}" }, status: :internal_server_error
  end

  def statuses
    jobs = fetch_jobs
    states = {}

    Sidekiq.redis do |r|
      jobs.each do |job|
        key  = job[:key]
        json = r.get(redis_key(key))

        if json.present?
          data = JSON.parse(json) rescue {}
          jid  = data["jid"]
          queue_name = (data["queue"].presence || job[:queue]).to_s

          state = determine_state(jid, queue_name)
          states[key] = { state: state, jid: jid }

          # ✅ 状態に応じて保持期間を調整（完了も少し残す）
          case state
          when "busy", "enqueued", "scheduled", "retry"
            r.expire(redis_key(key), 3600)  # 実行中は長め
          when "done"
            r.expire(redis_key(key), 60)    # 完了後もしばらく見せる
          when "dead"
            r.expire(redis_key(key), 300)   # 失敗は少し長め
          else
            r.expire(redis_key(key), 300)
          end
        else
          states[key] = { state: "idle" }
        end
      end
    end

    render json: { statuses: states }
  end

  # ▶ 再読込時の状態復元API
  def statuses
    jobs = fetch_jobs
    states = {}

    Sidekiq.redis do |r|
      jobs.each do |job|
        key = job[:key]
        json = r.get(redis_key(key))

        if json.present?
          data = JSON.parse(json) rescue {}
          jid  = data["jid"]
          queue_name = data["queue"] || job[:queue]

          state = determine_state(jid, queue_name)
          states[key] = { state: state, jid: jid }

          # 完了/死亡を検知したらキーを掃除
          if %w[done dead].include?(state)
            r.del(redis_key(key))
          else
            # 進捗が続く場合はTTL延長（任意）
            r.expire(redis_key(key), 3600)
          end
        else
          states[key] = { state: "idle" }
        end
      end
    end

    render json: { statuses: states }
  end

  private
  def redis_key(key)
    "jobdash:running:#{Rails.env}:#{key}"
  end

   # ✅ Sidekiqのバージョン差異に耐える busy 判定
  def determine_state(jid, queue_name)
    return "idle" if jid.blank?

    # 実行中（Workersを総当り）
    Sidekiq::Workers.new.each do |_pid, _tid, work|
      wjid = work.dig("payload", "jid") || work.dig("job", "jid") || work["jid"]
      return "busy" if wjid == jid
    end

    # キュー待ち（指定キュー優先）
    if queue_name.present?
      q = Sidekiq::Queue.new(queue_name)
      return "enqueued" if q.find_job(jid)
    end
    Sidekiq::Queue.all.each { |q| return "enqueued" if q.find_job(jid) }

    # 予約・リトライ・Dead
    return "scheduled" if Sidekiq::ScheduledSet.new.find_job(jid)
    return "retry"     if Sidekiq::RetrySet.new.find_job(jid)
    return "dead"      if Sidekiq::DeadSet.new.find_job(jid)

    # どこにも無ければ完了とみなす
    "done"
  rescue
    "unknown"
  end

  # 既存の fetch_jobs はそのまま（Sidekiq::Cron or YAMLから）
  def fetch_jobs
    if defined?(Sidekiq::Cron::Job) && Sidekiq::Cron::Job.all.any?
      Sidekiq::Cron::Job.all.map do |j|
        { key: j.name, name: FRIENDLY_TITLES[j.name] || j.name, klass: j.klass, cron: j.cron,
          queue: (j.queue_name rescue nil), desc: (j.description rescue nil), args: (j.args.presence rescue nil) }
      end
    else
      path = Rails.root.join("config", "scheduler_#{Rails.env}.yml")
      raw  = File.exist?(path) ? YAML.load_file(path) : {}
      raw.map do |key, v|
        { key: key, name: FRIENDLY_TITLES[key] || (v["description"].presence || key), klass: v["class"],
          cron: v["cron"], queue: v["queue"], desc: v["description"], args: v["args"] }
      end
    end
  end
end
