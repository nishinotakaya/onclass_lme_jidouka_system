# frozen_string_literal: true

require 'selenium-webdriver'

class LmeCookieRefresher
  def self.run_with_fallback
    Rails.logger.info("[LmeCookieRefresher] Starting automatic cookie refresh with fallback...")
    
    # まず既存のクッキーが有効かチェック
    auth = LmeAuthClient.new
    if auth.valid_cookie?
      Rails.logger.info("[LmeCookieRefresher] ✅ Existing cookie is still valid, no refresh needed")
      return true
    end
    
    # 自動ログインを試行
    result = run
    return result if result
    
    # 失敗した場合は手動クッキー設定を促す
    Rails.logger.warn("[LmeCookieRefresher] ⚠️  Automatic login failed. Please set cookie manually.")
    puts "⚠️  Automatic login failed. Please copy cookie from browser and run:"
    puts "auth = LmeAuthClient.new"
    puts "auth.manual_set!('your_cookie_string_here')"
    false
  end

  def self.run
    Rails.logger.info("[LmeCookieRefresher] Starting automatic cookie refresh...")
    
    options = Selenium::WebDriver::Chrome::Options.new
    options.add_argument('--headless=false') # reCAPTCHA対策のため非ヘッドレス
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-web-security')
    options.add_argument('--disable-features=VizDisplayCompositor')
    
    driver = Selenium::WebDriver.for(:chrome, options: options)
    
    begin
      # LME ログインページへ
      Rails.logger.info("[LmeCookieRefresher] Navigating to login page...")
      driver.navigate.to("https://step.lme.jp/login")
      
      # Google ログインボタンをクリック
      Rails.logger.info("[LmeCookieRefresher] Clicking Google login button...")
      driver.find_element(css: 'a[href*="google"]').click
      
      # Google ログインページでメールアドレス入力
      Rails.logger.info("[LmeCookieRefresher] Filling Google email...")
      driver.find_element(css: 'input[type="email"]').send_keys(ENV["GOOGLE_EMAIL"])
      driver.find_element(id: "identifierNext").click
      
      # パスワード入力
      Rails.logger.info("[LmeCookieRefresher] Filling Google password...")
      driver.find_element(css: 'input[type="password"]').send_keys(ENV["GOOGLE_PASSWORD"])
      driver.find_element(id: "passwordNext").click
      
      # reCAPTCHA対策: 人間らしい待機時間
      Rails.logger.info("[LmeCookieRefresher] Waiting for potential reCAPTCHA...")
      sleep(3)
      
      # reCAPTCHAが表示されているかチェック
      if driver.find_elements(css: 'iframe[src*="recaptcha"]').any?
        Rails.logger.warn("[LmeCookieRefresher] ⚠️  reCAPTCHA detected! Manual intervention required.")
        puts "⚠️  reCAPTCHA detected! Please solve it manually in the browser window."
        puts "Press Enter after solving reCAPTCHA to continue..."
        gets
      end
      
      # LMEダッシュボードにリダイレクトされるまで待機
      Rails.logger.info("[LmeCookieRefresher] Waiting for redirect to LME dashboard...")
      wait = Selenium::WebDriver::Wait.new(timeout: 30)
      wait.until { driver.current_url.include?("/basic/") }
      
      # Cookie を回収
      Rails.logger.info("[LmeCookieRefresher] Collecting cookies...")
      cookies = driver.manage.all_cookies
      cookie_str = cookies.map { |c| "#{c[:name]}=#{c[:value]}" }.join("; ")
      
      Rails.logger.info("[LmeCookieRefresher] Cookie length: #{cookie_str.length} characters")
      
      # LmeAuthClientに保存
      auth = LmeAuthClient.new
      auth.manual_set!(cookie_str)
      
      # クッキーが有効かテスト
      if auth.valid_cookie?
        Rails.logger.info("[LmeCookieRefresher] ✅ Cookie refreshed successfully and is valid!")
        puts "✅ Cookie refreshed successfully and is valid!"
        return true
      else
        Rails.logger.error("[LmeCookieRefresher] ❌ Cookie refresh failed - validation failed")
        puts "❌ Cookie refresh failed - validation failed"
        return false
      end
      
    rescue => e
      Rails.logger.error("[LmeCookieRefresher] Error during cookie refresh: #{e.class} #{e.message}")
      puts "❌ Error during cookie refresh: #{e.class} #{e.message}"
      return false
    ensure
      driver.quit
    end
  end
end