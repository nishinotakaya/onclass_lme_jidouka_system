# frozen_string_literal: true
require 'selenium-webdriver'

module Lme
  # アプリ体験会（セミナー）申込フォームの「日程」セレクトボックスを、
  # 常に「次の第2木曜日」に保つワーカー。
  #
  # 仕様:
  #   - 表示すべき日付 = 次の第2木曜日
  #       今月の第2木曜より前 → 今月の第2木曜
  #       今月の第2木曜(20:00)を過ぎたら → 翌月の第2木曜
  #   - 発火: 毎週木曜 20:00 (cron "0 20 * * 4")。ワーカー側で「今日が第2木曜か」を判定し、
  #           第2木曜の回だけ翌月の第2木曜へ更新する（cron単体では「第2木曜」を正確に表せないため）。
  #
  # フォーム更新の実体(update_selectbox!)は、LMEの保存リクエスト or 編集画面DOMの特定が必要。
  # 特定できるまでは対象日付をログ出力し、更新メソッドで実行する。
  class SeminarDateUpdateWorker
    include Sidekiq::Worker
    sidekiq_options queue: :lme_seminar, retry: 2

    ORIGIN  = 'https://step.lme.jp'
    UA      = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
    CH_UA   = %Q("Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140")
    CUTOFF_HOUR = (ENV['LME_SEMINAR_CUTOFF_HOUR'].presence || '20').to_i # 第2木曜の何時を過ぎたら翌月へ

    # 体験会フォームの識別子（form-answer/edit/<id>）。新規165004・既存168865の2枚。
    FORM_IDS = (ENV['LME_SEMINAR_FORM_IDS'].presence || '165004,168865').split(',').map(&:strip).reject(&:blank?)

    # 毎週木曜20:00に発火するが、フォームが常に「次の第2木曜」を指すよう
    # 毎回 target_date を計算して設定する（第2木曜20:00で翌月へ切替。他の木曜は同値=無害）。
    def perform(_force = false)
      Time.zone = 'Asia/Tokyo'
      now = Time.zone.now
      target = self.class.target_date(now)
      Rails.logger.info("[SeminarDateUpdate] now=#{now.strftime('%Y-%m-%d %H:%M')} → 表示すべき日程=#{target} (#{target.strftime('%-m月%-d日(%a)')})")
      update_selectbox!(target)
    rescue => e
      Rails.logger.error("[SeminarDateUpdate] #{e.class}: #{e.message}")
      raise
    end

    # --- 第2木曜日の算出 -----------------------------------------------------
    # その月の 8〜14日のうち木曜(wday==4)が第2木曜。
    def self.second_thursday(year, month)
      (8..14).each do |d|
        date = Date.new(year, month, d)
        return date if date.wday == 4
      end
      nil
    end

    # 「今フォームが表示すべき日程」= 次の第2木曜日。
    #   今月の第2木曜より前            → 今月の第2木曜
    #   今月の第2木曜だが CUTOFF_HOUR 前 → 今月の第2木曜（当日は締めまで表示）
    #   今月の第2木曜の CUTOFF_HOUR 以降 → 翌月の第2木曜
    #   今月の第2木曜より後            → 翌月の第2木曜
    def self.target_date(now, cutoff_hour = CUTOFF_HOUR)
      today = now.to_date
      st = second_thursday(today.year, today.month)
      return st if today < st
      return st if today == st && now.hour < cutoff_hour
      nm = today >> 1 # 翌月
      second_thursday(nm.year, nm.month)
    end

    private

    # 日程セレクトの日付「M月D日（木）」の URLエンコード正規表現（label/value 両方に出る）。
    #   例: 12月11日（木） = 12%E6%9C%8811%E6%97%A5%EF%BC%88%E6%9C%A8%EF%BC%89
    DATE_ENC_RE = /\d+%E6%9C%88\d+%E6%97%A5%EF%BC%88%E6%9C%A8%EF%BC%89/.freeze

    # LMEの各体験会フォームの「日程」セレクトを target(次の第2木曜)へ更新する。
    # ①Seleniumログイン(1回) ②各フォームで保存XHRを発火し実UIが送る正確なpayload+csrfを捕捉
    # ③日付部分だけ target へ正規表現置換 ④同セッションで save-v3 へ再POST。
    # ブラウザが送る現フォーム全体を使うため他設定(help/タグ等)を壊さない。
    def update_selectbox!(target)
      ctx = Lme::ApiContext.new(origin: ORIGIN, ua: UA, accept_lang: 'ja', ch_ua: CH_UA, logger: Rails.logger, bot_id: (ENV['LME_BOT_ID'].presence || '17106'))
      ctx.login_with_google!(email: ENV['GOOGLE_EMAIL'], password: ENV['GOOGLE_PASSWORD'], api_key: ENV['API2CAPTCHA_KEY'])
      d = ctx.driver
      raise 'Selenium driver が取得できませんでした' unless d

      results = FORM_IDS.map { |fid| update_one_form!(d, ctx.bot_id, fid, target) }
      { status: 'done', target: target.to_s, results: results }
    ensure
      begin; ctx&.driver&.quit; rescue; end
    end

    # 1フォーム分の 捕捉→置換→再POST
    def update_one_form!(d, bot_id, form_id, target)
      d.navigate.to("#{ORIGIN}/basic/form-answer/edit/#{form_id}?botIdCurrent=#{bot_id}&isOtherBot=1")
      sleep 10
      d.execute_script(<<~JS)
        window.__cap=[]; window.__csrf=null;
        var os=XMLHttpRequest.prototype.send, oo=XMLHttpRequest.prototype.open, oh=XMLHttpRequest.prototype.setRequestHeader;
        XMLHttpRequest.prototype.open=function(m,u){this.__u=u;return oo.apply(this,arguments);};
        XMLHttpRequest.prototype.setRequestHeader=function(k,v){try{if((''+k).toLowerCase()=='x-csrf-token')window.__csrf=v;}catch(e){}return oh.apply(this,arguments);};
        XMLHttpRequest.prototype.send=function(b){try{if(this.__u&&(''+this.__u).indexOf('save-v3')>=0){window.__cap.push(b?(''+b):'');}}catch(e){}return os.apply(this,arguments);};
      JS
      btn = d.find_elements(css: 'button,a').find { |x| x.text.to_s.strip.include?('保存') }
      raise "[#{form_id}] 保存ボタンが見つかりません" unless btn
      d.execute_script('arguments[0].click();', btn)
      sleep 6

      body = d.execute_script('return (window.__cap && window.__cap[0]) || ""').to_s
      csrf = d.execute_script('return window.__csrf').to_s
      raise "[#{form_id}] 保存payloadを捕捉できませんでした" if body.empty?
      raise "[#{form_id}] 日程(M月D日（木）)がpayloadに見つかりません" unless body.match?(DATE_ENC_RE)

      target_md = "#{target.month}%E6%9C%88#{target.day}%E6%97%A5" # 「M月D日」
      new_body  = body.gsub(DATE_ENC_RE, "#{target_md}%EF%BC%88%E6%9C%A8%EF%BC%89")
      Rails.logger.info("[SeminarDateUpdate] form=#{form_id} payload捕捉(#{body.length}B)→日程 #{target.strftime('%-m月%-d日')} へ置換")

      d.manage.timeouts.script_timeout = 40
      res = d.execute_async_script(<<~JS, new_body, csrf, form_id)
        var body=arguments[0], csrf=arguments[1], fid=arguments[2], cb=arguments[arguments.length-1];
        fetch('/ajax/form-answer/save-v3/'+fid,{method:'POST',headers:{'content-type':'application/x-www-form-urlencoded; charset=UTF-8','x-csrf-token':csrf,'x-requested-with':'XMLHttpRequest'},body:body,credentials:'same-origin'})
          .then(function(r){return r.text().then(function(t){cb({status:r.status,body:t.slice(0,250)});});})
          .catch(function(e){cb({error:''+e});});
      JS
      ok = res.is_a?(Hash) && res['status'] == 200 && res['body'].to_s.include?('"status":true')
      Rails.logger.info("[SeminarDateUpdate] form=#{form_id} save-v3 result=#{res.inspect} ok=#{ok}")
      raise "[#{form_id}] 保存APIが失敗: #{res.inspect}" unless ok
      { form_id: form_id, status: 'updated', response: res }
    end
  end
end
