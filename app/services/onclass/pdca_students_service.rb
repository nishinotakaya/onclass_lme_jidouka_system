# frozen_string_literal: true

require 'net/http'
require 'uri'
require 'nokogiri'
require 'date'

module Onclass
  # PDCAアプリ（https://pdca-app-475677fd481e.herokuapp.com）の
  # /students ページから生徒ごとの「最新報告日」を取得するサービス。
  #
  # テーブル列構成:
  #   0:名前 1:メール 2:MTG周期 3:チーム 4:コース 5:ステータス 6:最新報告 ...
  # 最新報告の日付フォーマット: "YY/MM/DD" (例: "26/03/10")
  #
  # 返り値:
  #   {
  #     by_email: { "email@example.com" => "2026年3月10日", ... },
  #     by_name:  { "正規化済み名前"     => "2026年3月10日", ... }
  #   }
  class PdcaStudentsService
    PDCA_BASE_URL = 'https://pdca-app-475677fd481e.herokuapp.com'.freeze
    SIGN_IN_PATH  = '/users/sign_in'.freeze
    STUDENTS_PATH = '/students'.freeze

    USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36'.freeze

    # .env から認証情報を定数化
    EMAIL    = ENV.fetch('PDCA_APP_EMAIL', '').freeze
    PASSWORD = ENV.fetch('PDCA_APP_PASSWORD', '').freeze

    # Returns { by_email: {}, by_name: {} }
    def fetch_latest_reports
      if EMAIL.blank? || PASSWORD.blank?
        Rails.logger.warn('[Onclass::PdcaStudentsService] PDCA_APP_EMAIL / PDCA_APP_PASSWORD 未設定')
        return { by_email: {}, by_name: {} }
      end

      cookie = sign_in
      unless cookie
        Rails.logger.warn('[Onclass::PdcaStudentsService] ログイン失敗 – セッションクッキー取得不可')
        return { by_email: {}, by_name: {} }
      end

      html = get_students_page(cookie)
      return { by_email: {}, by_name: {} } if html.blank?

      parse_students_table(html)
    end

    private

    # ── ログイン ──────────────────────────────────────────────
    def sign_in
      uri = URI("#{PDCA_BASE_URL}#{SIGN_IN_PATH}")

      # Step1: GET でCSRFトークン + 初期クッキー取得
      get_resp = http_get(uri)
      return nil unless get_resp

      csrf   = extract_csrf(get_resp.body)
      cookie = collect_cookies(get_resp)

      Rails.logger.debug("[Onclass::PdcaStudentsService] sign_in GET ok, csrf=#{csrf.to_s[0..15]}…")

      # Step2: POST でログイン（Devise標準フォーム）
      form = {
        'user[email]'        => EMAIL,
        'user[password]'     => PASSWORD,
        'authenticity_token' => csrf.to_s,
        'commit'             => 'ログイン'
      }
      post_resp = http_post(uri, form, cookie: cookie)
      return nil unless post_resp

      code = post_resp.code.to_i
      Rails.logger.debug("[Onclass::PdcaStudentsService] sign_in POST -> #{code}")

      case code
      when 302
        # リダイレクト = ログイン成功
        collect_cookies(post_resp).presence || cookie
      when 200
        # 200 = ログイン失敗（エラーメッセージが返る）
        doc = Nokogiri::HTML(post_resp.body)
        msg = doc.at_css('.alert, [class*="error"], #error_explanation')&.text&.strip
        Rails.logger.warn("[Onclass::PdcaStudentsService] ログイン失敗 msg=#{msg}")
        nil
      else
        collect_cookies(post_resp).presence || cookie
      end
    end

    # ── /students 取得 ─────────────────────────────────────────
    def get_students_page(cookie)
      uri  = URI("#{PDCA_BASE_URL}#{STUDENTS_PATH}")
      resp = http_get(uri, cookie: cookie)
      code = resp&.code.to_i
      unless code == 200
        Rails.logger.warn("[Onclass::PdcaStudentsService] /students -> #{code}")
        return nil
      end
      resp.body
    end

    # ── HTMLパース ────────────────────────────────────────────
    # テーブル列: 名前(0) メール(1) 最新報告(6)
    def parse_students_table(html)
      doc     = Nokogiri::HTML(html)
      by_email = {}
      by_name  = {}

      doc.css('table tbody tr').each do |tr|
        cells = tr.css('td')
        next if cells.size < 7

        name_raw   = cells[0].text.strip
        email_raw  = cells[1].text.strip
        report_raw = cells[6].text.strip   # "26/03/10"

        next if report_raw.blank?

        date_jp = parse_yymm_dd(report_raw)
        next if date_jp.nil?

        by_email[email_raw]                   = date_jp if email_raw.present?
        by_name[normalize_name(name_raw)]     = date_jp if name_raw.present?
      end

      Rails.logger.info("[Onclass::PdcaStudentsService] parsed #{by_email.size} students (by email)")
      { by_email: by_email, by_name: by_name }
    end

    # "26/03/10" (YY/MM/DD) → "2026年3月10日"
    def parse_yymm_dd(str)
      return nil if str.blank?
      raw = str.strip

      # YY/MM/DD 形式
      if raw.match?(/\A\d{2}\/\d{2}\/\d{2}\z/)
        d = Date.strptime(raw, '%y/%m/%d') rescue nil
        return d ? d.strftime('%Y年%-m月%-d日') : nil
      end

      # YYYY/MM/DD 形式
      if raw.match?(/\A\d{4}\/\d{2}\/\d{2}\z/)
        d = Date.strptime(raw, '%Y/%m/%d') rescue nil
        return d ? d.strftime('%Y年%-m月%-d日') : nil
      end

      # YYYY-MM-DD 形式
      if raw.match?(/\A\d{4}-\d{2}-\d{2}\z/)
        d = Date.parse(raw) rescue nil
        return d ? d.strftime('%Y年%-m月%-d日') : nil
      end

      # すでに日本語形式
      return raw if raw.match?(/\A\d{4}年\d{1,2}月\d{1,2}日\z/)

      nil
    end

    # ── HTML ヘルパ ───────────────────────────────────────────
    def extract_csrf(html)
      return nil if html.blank?
      doc = Nokogiri::HTML(html)
      doc.at_css('meta[name="csrf-token"]')&.attr('content') ||
        doc.at_css('input[name="authenticity_token"]')&.attr('value')
    end

    # Set-Cookie ヘッダ全行を "name=value; name=value" に結合
    def collect_cookies(response)
      raw = response.get_fields('set-cookie') || []
      raw.map { |c| c.split(';').first.strip }.join('; ')
    end

    # ── HTTP ─────────────────────────────────────────────────
    def http_get(uri, cookie: nil)
      Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == 'https', read_timeout: 30) do |http|
        req = Net::HTTP::Get.new(uri)
        set_headers(req, cookie: cookie)
        http.request(req)
      end
    rescue => e
      Rails.logger.warn("[Onclass::PdcaStudentsService] GET #{uri} failed: #{e.class} #{e.message}")
      nil
    end

    def http_post(uri, form, cookie: nil)
      Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == 'https', read_timeout: 30) do |http|
        req = Net::HTTP::Post.new(uri)
        set_headers(req, cookie: cookie)
        req['Content-Type'] = 'application/x-www-form-urlencoded'
        req.set_form_data(form)
        http.request(req)
      end
    rescue => e
      Rails.logger.warn("[Onclass::PdcaStudentsService] POST #{uri} failed: #{e.class} #{e.message}")
      nil
    end

    def set_headers(req, cookie: nil)
      req['Accept']          = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
      req['Accept-Language'] = 'ja,en-US;q=0.9,en;q=0.8'
      req['Cache-Control']   = 'max-age=0'
      req['Connection']      = 'keep-alive'
      req['User-Agent']      = USER_AGENT
      req['Referer']         = 'https://manager.the-online-class.com/'
      req['Upgrade-Insecure-Requests'] = '1'
      req['Cookie'] = cookie if cookie.present?
    end

    def normalize_name(str)
      str.to_s
         .gsub(/（.*?）|\(.*?\)/, '')
         .tr('　', ' ')
         .gsub(/\s+/, '')
    end
  end
end
