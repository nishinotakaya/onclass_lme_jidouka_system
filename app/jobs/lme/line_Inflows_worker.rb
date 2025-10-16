# frozen_string_literal: true
require 'json'
require 'cgi'
require 'uri'
require 'set'
require 'faraday'
require 'active_support'
require 'active_support/core_ext'
require 'google/apis/sheets_v4'
require 'googleauth'
require 'selenium-webdriver'
require 'net/http'
require 'selenium/devtools'

module Lme
  class LineInflowsWorker
    include Sidekiq::Worker
    sidekiq_options queue: 'lme_line_inflows', retry: 3, lock: :until_executed

    GOOGLE_SCOPE = [Google::Apis::SheetsV4::AUTH_SPREADSHEETS].freeze

    PROAKA_CATEGORY_ID       = 5_180_568
    PROAKA_SEMINAR_CATEGORY  = 5_238_317
    CUM_SINCE                = ENV['LME_CUM_SINCE'].presence || '2023-01-01'

    PROAKA_TAGS = { v1: 1_394_734, v2: 1_394_736, v3: 1_394_737, v4: 1_394_738 }.freeze
    PROAKA_DIGEST_NAMES = { dv1: '動画①_ダイジェスト', dv2: '動画②_ダイジェスト', dv3: '動画③_ダイジェスト' }.freeze
    RICHMENU_SELECT_NAMES = [
      '月収40万円のエンジニアになれる方法を知りたい',
      'プログラミング無料体験したい',
      '現役エンジニアに質問したい'
    ].freeze
    # 追加: 入会関連/個別相談
    JOIN_CATEGORY_ID                = 88_647
    JOIN_PROAKA_PAID_TAG_NAME       = 'プロアカ決済完了'
    JOIN_FREELANCE_PAID_TAG_NAME    = 'フリーエンジニア決済完了'
    PROAKA_KOBETSU_TAG_NAME         = 'プロアカ個別相談'

    ORIGIN      = 'https://step.lme.jp'
    UA          = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
    ACCEPT_LANG = 'ja,en-US;q=0.9,en;q=0.8'
    CH_UA       = %Q("Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140")

    def perform(start_date=nil, end_date=nil)
      Time.zone = 'Asia/Tokyo'

      # ---- Context（ログイン→CSRF）
      bot_id = (ENV['LME_BOT_ID'].presence || '17106').to_s
      ctx = Lme::ApiContext.new(origin: ORIGIN, ua: UA, accept_lang: ACCEPT_LANG, ch_ua: CH_UA, logger: Rails.logger, bot_id: bot_id)
      ctx.login_with_google!(email: ENV['GOOGLE_EMAIL'], password: ENV['GOOGLE_PASSWORD'], api_key: ENV['API2CAPTCHA_KEY'])
         .ensure_csrf_meta!

      # ---- 期間
      start_on  = (start_date.presence || default_start_on)
      end_on    = (end_date.presence   || Time.zone.today.to_s)
      start_cut = (Date.parse(start_on) rescue Date.today)
      end_cut   = (Date.parse(end_on)   rescue Date.today)
      Rails.logger.info("[Inflows] range=#{start_on}..#{end_on}")

      # ---- ウォームアップ
      Lme::InitHistoryService.new(ctx: ctx).warmup!(start_on: start_on, end_on: end_on)

      # ---- 友だち一覧（v2）
      v2_rows = Lme::FriendlistService.new(ctx: ctx).fetch_between(start_on: start_on, end_on: end_on)

      # filter & normalize
      raw_rows = []
      v2_rows.each do |row|
        blocked  = row['is_blocked'].to_i == 1
        followed = row['followed_at'].present? &&
                   ((Date.parse(row['followed_at']) rescue Date.new(1900,1,1)) >= start_cut) &&
                   ((Date.parse(row['followed_at']) rescue Date.new(2999,1,1)) <= end_cut)
        next unless (blocked || followed)
        normalize_row!(row)
        raw_rows << row
      end
      # ---- ブロック専用API
      blocked_rows = Lme::BlockListService.new(ctx: ctx).fetch(start_on: start_on, end_on: end_on)
      Rails.logger.info("[blocked-api] fetched=#{blocked_rows.size}")
      raw_rows = merge_block_info!(raw_rows, blocked_rows)
      # ---- UIDマージ
      merged_by_uid = {}
      Array(raw_rows).each do |r|
        uid = extract_line_user_id_from_link(r['link_my_page']).to_i
        next if uid <= 0
        cur = merged_by_uid[uid]
        if cur.nil?
          merged_by_uid[uid] = r.dup
        else
          fa = cur['followed_at']; fb = r['followed_at']
          cur['followed_at'] =
            begin
              if fa.present? && fb.present?
                (Time.parse(fa) >= Time.parse(fb)) ? fa : fb
              else
                fa.presence || fb.presence
              end
            rescue
              fa.presence || fb.presence
            end

          ba = cur['blocked_at']; bb = r['blocked_at']
          cur['blocked_at'] =
            begin
              [ba, bb].compact.max_by { |x| Time.parse(x) }
            rescue
              ba.presence || bb.presence
            end

          cur['is_blocked']   = [cur['is_blocked'].to_i, r['is_blocked'].to_i].max
          cur['name']         = cur['name'].presence         || r['name'].presence         || cur['view_name'].presence || r['view_name'].presence
          cur['landing_name'] = cur['landing_name'].presence || r['landing_name'].presence
          cur['link_my_page'] = cur['link_my_page'].presence || r['link_my_page'].presence
        end
      end
      rows = merged_by_uid.values
      Rails.logger.info("[v2] merged rows=#{rows.size}")

      # ---- chat-v3 を踏んで安定化（任意）
      if rows.present?
        sample_uid = extract_line_user_id_from_link(rows.first['link_my_page'])
        ctx.bake_chat_context_for!(sample_uid)
      end

      # ---- タグ/セミナー & my_page 補完
      tags_svc   = Lme::ChatTagsService.new(ctx: ctx)
      mypage_svc = Lme::MyPageService.new(ctx: ctx)

      seminar_dates_set = Set.new
      rows.each_with_index do |r, i|
        uid = extract_line_user_id_from_link(r['link_my_page'])

        # tags
        res_body = tags_svc.fetch_for(line_user_id: uid)
        proaka_flags, seminar_map = extract_proaka_and_seminar_from_payload(res_body)
        r['tags_flags']  = proaka_flags
        r['seminar_map'] = seminar_map || {}
        r['seminar_map'].keys.each { |ymd| seminar_dates_set << ymd }

        # 追加: 入会関連（成約）＆ 個別相談
        contracts, kobetsu = extract_join_and_kobetsu_from_payload(res_body)
        r['contracts'] = contracts
        r['kobetsu']   = kobetsu

        # my_page
        info = fetch_user_basic_info_via_service(mypage_svc, uid)
        q = info[:qr_code].to_s.strip
        r['qr_code'] = q unless blankish?(q)

        tf = info[:time_follow].to_s.strip
        if r['followed_at'].blank? && !blankish?(tf)
          r['followed_at'] = tf
        end

        inf = info[:inflow].to_s.strip
        if blankish?(r['landing_name']) && !blankish?(inf)
          r['landing_name'] = inf
        end
        sleep 0.12
        Rails.logger.debug("[tags] #{i+1}/#{rows.size}") if (i % 200).zero?
      end
      Rails.logger.info("[tags] seminar dates unique=#{seminar_dates_set.size}")

      # ---- GSheets 反映（ソース）
      spreadsheet_id = ENV.fetch('ONCLASS_SPREADSHEET_ID')
      sheet_name     = ENV.fetch('LME_SHEET_NAME', 'Line流入者')
      anchor_name    = ENV.fetch('ONCLASS_SHEET_NAME', 'フロントコース受講生')

      service = build_sheets_service
      ensure_sheet_exists_adjacent!(service, spreadsheet_id, sheet_name, anchor_name)
      upload_to_gsheets!(
        service: service,
        rows: to_sheet_rows(rows),
        spreadsheet_id: spreadsheet_id,
        sheet_name: sheet_name,
        end_on: end_on,
        seminar_dates: seminar_dates_set.to_a.sort
      )

      # ---- 別ブックへコピー（ターゲット）
      target_spreadsheet_id = ENV['LME_YAMADA_INFLOWS_SPREADSHEET_ID']
      target_sheet_name     = ENV['LME_YAMADA_INFLOWS_SPREADSHEET_NAME']
      copy_to_yamada_sheet!(
        service,
        source_spreadsheet_id: spreadsheet_id,
        source_sheet_name:     sheet_name,
        target_spreadsheet_id: target_spreadsheet_id,
        target_sheet_name:     target_sheet_name
      )

      Rails.logger.info("[LmeLineInflowsWorker] wrote #{rows.size} rows to #{sheet_name}")
      Lme::LineCountsWorker.new.perform
      { count: rows.size, sheet: sheet_name, range: [start_on, end_on] }
    rescue Faraday::Error => e
      Rails.logger.error("[LmeLineInflowsWorker] HTTP error: #{e.class} #{e.message}")
      raise
    rescue => e
      Rails.logger.error("[LmeLineInflowsWorker] unexpected error: #{e.class} #{e.message}")
      raise
    end

    # === MyPage（サービス経由）/抽出 ===

    def extract_proaka_and_seminar_from_payload(payload)
      categories =
        case payload
        when String
          j = JSON.parse(payload) rescue nil
          if j.is_a?(Hash)
            j['data'] || j['result'] || j['items'] || j['list'] || []
          elsif j.is_a?(Array)
            j
          else
            []
          end
        when Hash
          payload['data'] || payload['result'] || payload['items'] || payload['list'] || []
        when Array
          payload
        else
          []
        end

      proaka  = proaka_flags_from_categories(categories)
      seminar = seminar_map_from_categories(categories)
      [proaka, seminar]
    rescue => e
      Rails.logger.debug("[extract_tags] #{e.class}: #{e.message}")
      [{ v1: false, v2: false, v3: false, v4: false, dv1: false, dv2: false, dv3: false, select: nil }, {}]
    end

    def to_sheet_rows(v2_rows)
      v2_rows.map do |rec|
        uid = extract_line_user_id_from_link(rec['link_my_page'])
        {
          'date'         => rec['followed_at'].to_s[0,10],
          'followed_at'  => rec['followed_at'],
          'blocked_at'   => rec['blocked_at'],
          'landing_name' => safe_landing_name(rec),
          'name'         => rec['name'],
          'line_user_id' => uid,
          'is_blocked'   => (rec['is_blocked'] || 0).to_i,
          'tags_flags'   => rec['tags_flags'] || {},
          'seminar_map'  => rec['seminar_map'] || {},
          'qr_code'      => rec['qr_code'],
          'contracts'    => rec['contracts'],
          'kobetsu'      => rec['kobetsu']
        }
      end
    end

    def proaka_flags_from_categories(categories)
      target = Array(categories).find { |c| (c['id'] || c[:id]).to_i == PROAKA_CATEGORY_ID }
      return { v1: false, v2: false, v3: false, v4: false, dv1: false, dv2: false, dv3: false, select: nil } unless target

      tag_list  = Array(target['tags'] || target[:tags])
      tag_ids   = tag_list.map  { |t| (t['tag_id'] || t[:tag_id]).to_i }.to_set
      tag_names = tag_list.map  { |t| (t['name']   || t[:name]).to_s }.to_set
      selected  = RICHMENU_SELECT_NAMES.find { |nm| tag_names.include?(nm) }

      {
        v1:  tag_ids.include?(PROAKA_TAGS[:v1]),
        v2:  tag_ids.include?(PROAKA_TAGS[:v2]),
        v3:  tag_ids.include?(PROAKA_TAGS[:v3]),
        v4:  tag_ids.include?(PROAKA_TAGS[:v4]),
        dv1: tag_names.include?(PROAKA_DIGEST_NAMES[:dv1]),
        dv2: tag_names.include?(PROAKA_DIGEST_NAMES[:dv2]),
        dv3: tag_names.include?(PROAKA_DIGEST_NAMES[:dv3]),
        select: selected
      }
    end

    def seminar_map_from_categories(categories)
      cat = Array(categories).find { |c| (c['id'] || c[:id]).to_i == PROAKA_SEMINAR_CATEGORY }
      return {} unless cat
      tag_list = Array(cat['tags'] || cat[:tags])

      result = Hash.new { |h, k| h[k] = { hope: false, attend: false } }

      tag_list.each do |t|
        raw_name = (t['name'] || t[:name]).to_s

        # 全角→半角や互換文字の統一（例: ［／－］, 全角数字）
        name = raw_name.dup
        if name.respond_to?(:unicode_normalize)
          name = name.unicode_normalize(:nfkc)
        else
          name = name.tr('０-９／－．', '0-9/-.')
        end
        name = name.strip

        # 例: "参加希望 2025/10/09", "参加 2025-10-09", "参加希望2025年10月9日（木）"
        if name =~ /(参加希望|参加)\s*([0-9]{4})[\/\-\.\s年]([0-9]{1,2})[\/\-\.\s月]([0-9]{1,2})(?:日|[^0-9].*)?\s*\z/
          what, y, m, d = $1, $2.to_i, $3.to_i, $4.to_i
          begin
            ymd = Date.new(y, m, d).strftime('%Y-%m-%d')
            if what == '参加希望'
              result[ymd][:hope] = true
            else
              result[ymd][:attend] = true
            end
          rescue ArgumentError
            # 無効日付はスキップ
          end
          next
        end

        # フォールバック: 「キーワード + 日付っぽいもの」を緩めに抽出
        if name =~ /(参加希望|参加)\s*([0-9]{4}[\-\/\.][0-9]{1,2}[\-\/\.][0-9]{1,2})/
          what, datestr = $1, $2
          begin
            ymd = Date.parse(datestr).strftime('%Y-%m-%d')
            if what == '参加希望'
              result[ymd][:hope] = true
            else
              result[ymd][:attend] = true
            end
          rescue ArgumentError
            # だめなら無視
          end
        end
      end

      result
    end

    # ==== Helpers: セミナー参加率（参加希望→参加） ===============================
    # 分母：その日付の「参加希望」人数
    # 分子：その日付で「参加希望」かつ「参加」の両方が付いた人数（同一UIDの積集合）
    def seminar_conversion_rates(rows, seminar_dates:, month: nil, since: nil, until_date: nil)
      counts = seminar_dates.each_with_object({}) { |ymd, h| h[ymd] = { hope: 0, matched: 0 } }

      Array(rows).each do |r|
        sem = r['seminar_map'] || {}

        seminar_dates.each do |ymd|
          flags = sem[ymd]
          next unless flags # その日付のタグが一切ない人はスキップ

          # 月フィルタ（開催日の月で判定）
          if month
            ymd_month = (Date.parse(ymd).strftime('%Y-%m') rescue nil)
            next unless ymd_month == month
          end

          # 累計の起点
          if since
            next unless (Date.parse(ymd) rescue Date.new(1900,1,1)) >= (Date.parse(since) rescue Date.new(1900,1,1))
          end

          # 上限日（未来日を除外したいとき用）
          if until_date
            next unless (Date.parse(ymd) rescue Date.new(2999,1,1)) <= (Date.parse(until_date) rescue Date.new(2999,1,1))
          end

          # 分母：希望
          if flags[:hope]
            counts[ymd][:hope] += 1
            # 分子：希望かつ参加
            counts[ymd][:matched] += 1 if flags[:attend]
          end
        end
      end

      # { "YYYY-MM-DD" => 66.7, ... } 小数1桁%（分母0は nil）
      counts.transform_values do |c|
        c[:hope] > 0 ? ((c[:matched].to_f / c[:hope]) * 100).round(1) : nil
      end
    end

    # 追加: 入会関連（成約）＆ 個別相談の抽出
    def extract_join_and_kobetsu_from_payload(payload)
      categories =
        case payload
        when String
          j = JSON.parse(payload) rescue nil
          if j.is_a?(Hash)
            j['data'] || j['result'] || j['items'] || j['list'] || []
          elsif j.is_a?(Array)
            j
          else
            []
          end
        when Hash
          payload['data'] || payload['result'] || payload['items'] || payload['list'] || []
        when Array
          payload
        else
          []
        end

      contracts = join_contracts_from_categories(categories)
      kobetsu  = kobetsu_from_categories(categories)
      [contracts, kobetsu]
    rescue => e
      Rails.logger.debug("[extract_join] #{e.class}: #{e.message}")
      ['', '']
    end

    def join_contracts_from_categories(categories)
      cat = Array(categories).find { |c| (c['id'] || c[:id]).to_i == JOIN_CATEGORY_ID }
      return '' unless cat

      tags  = Array(cat['tags'] || cat[:tags])
      names = tags.map { |t| (t['name'] || t[:name]).to_s }

      vals = []
      vals << JOIN_PROAKA_PAID_TAG_NAME    if names.any? { |n| n.include?(JOIN_PROAKA_PAID_TAG_NAME) }
      vals << JOIN_FREELANCE_PAID_TAG_NAME if names.any? { |n| n.include?(JOIN_FREELANCE_PAID_TAG_NAME) }
      months = extract_join_months_from_tags(names)
      (vals + months).uniq.join('/')
    end

    def kobetsu_from_categories(categories)
      cat = Array(categories).find { |c| (c['id'] || c[:id]).to_i == PROAKA_CATEGORY_ID }
      return '' unless cat
      tag = Array(cat['tags'] || cat[:tags]).find { |t| (t['name'] || t[:name]).to_s.include?(PROAKA_KOBETSU_TAG_NAME) }
      tag ? (tag['name'] || tag[:name]).to_s : ''
    end

    def find_value_by_key(obj, key)
      return nil if obj.nil?
      return obj[key] if obj.is_a?(Hash) && obj.key?(key)
      if obj.is_a?(Hash)
        obj.each_value do |v|
          found = find_value_by_key(v, key)
          return found unless found.nil?
        end
      elsif obj.is_a?(Array)
        obj.each do |v|
          found = find_value_by_key(v, key)
          return found unless found.nil?
        end
      end
      nil
    end

    # =========================================================
    # Google Sheets
    # =========================================================
    def build_sheets_service
      keyfile = ENV['GOOGLE_APPLICATION_CREDENTIALS']
      raise 'ENV GOOGLE_APPLICATION_CREDENTIALS is not set.' if keyfile.blank?
      raise "Service account key not found: #{keyfile}" unless File.exist?(keyfile)

      json = JSON.parse(File.read(keyfile)) rescue nil
      unless json && json['type'] == 'service_account' && json['private_key'] && json['client_email']
        raise 'Invalid service account JSON: missing private_key/client_email/type=service_account'
      end

      auth = Google::Auth::ServiceAccountCredentials.make_creds(
        json_key_io: File.open(keyfile),
        scope: GOOGLE_SCOPE
      )
      auth.fetch_access_token!

      service = Google::Apis::SheetsV4::SheetsService.new
      service.client_options.application_name = 'LME Line Inflows Uploader'
      service.authorization = auth
      service
    end

    def ensure_sheet_exists_adjacent!(service, spreadsheet_id, new_sheet_name, after_sheet_name)
      ss      = service.get_spreadsheet(spreadsheet_id)
      sheets  = ss.sheets
      target  = sheets.find { |s| s.properties&.title == new_sheet_name }
      anchor  = ss.sheets.find { |s| s.properties&.title == after_sheet_name }

      return ensure_sheet_exists!(service, spreadsheet_id, new_sheet_name) if anchor.nil?

      desired_index = anchor.properties.index.to_i + 1

      if target.nil?
        add_req = Google::Apis::SheetsV4::AddSheetRequest.new(
          properties: Google::Apis::SheetsV4::SheetProperties.new(
            title: new_sheet_name, index: desired_index
          )
        )
        batch = Google::Apis::SheetsV4::BatchUpdateSpreadsheetRequest.new(
          requests: [Google::Apis::SheetsV4::Request.new(add_sheet: add_req)]
        )
        service.batch_update_spreadsheet(spreadsheet_id, batch)
      else
        cur = target.properties.index.to_i
        if cur != desired_index
          update_req = Google::Apis::SheetsV4::UpdateSheetPropertiesRequest.new(
            properties: Google::Apis::SheetsV4::SheetProperties.new(
              sheet_id: target.properties.sheet_id, index: desired_index
            ),
            fields: 'index'
          )
          batch = Google::Apis::SheetsV4::BatchUpdateSpreadsheetRequest.new(
            requests: [Google::Apis::SheetsV4::Request.new(update_sheet_properties: update_req)]
          )
          service.batch_update_spreadsheet(spreadsheet_id, batch)
        end
      end
    end

    def ensure_sheet_exists!(service, spreadsheet_id, sheet_name)
      ss = service.get_spreadsheet(spreadsheet_id)
      exists = ss.sheets.any? { |s| s.properties&.title == sheet_name }
      return if exists

      add_req = Google::Apis::SheetsV4::AddSheetRequest.new(
        properties: Google::Apis::SheetsV4::SheetProperties.new(title: sheet_name)
      )
      batch = Google::Apis::SheetsV4::BatchUpdateSpreadsheetRequest.new(
        requests: [Google::Apis::SheetsV4::Request.new(add_sheet: add_req)]
      )
      service.batch_update_spreadsheet(spreadsheet_id, batch)
    end

    def upload_to_gsheets!(service:, rows:, spreadsheet_id:, sheet_name:, end_on:, seminar_dates:)
      clear_req = Google::Apis::SheetsV4::ClearValuesRequest.new
      %w[B2 B3 B4 B5 B6 B7].each do |r|
        service.clear_values(spreadsheet_id, "#{sheet_name}!#{r}:ZZZ", clear_req)
      end

      meta_values = [['バッチ実行タイミング', jp_timestamp]]
      service.update_spreadsheet_value(
        spreadsheet_id, "#{sheet_name}!B2",
        Google::Apis::SheetsV4::ValueRange.new(values: meta_values),
        value_input_option: 'USER_ENTERED'
      )

      end_d  = (parse_date(end_on).to_date rescue Date.today)
      this_m = end_d.strftime('%Y-%m')
      prev_m = end_d.prev_month.strftime('%Y-%m')

      tags_cache = rows.each_with_object({}) { |r, h| h[r['line_user_id']] = (r['tags_flags'] || {}) }
      monthly_rates, cumulative_rates = calc_rates(rows, tags_cache, month: this_m, since: CUM_SINCE)
      prev_month_rates = month_rates(rows, tags_cache, month: prev_m)

      seminar_headers = seminar_dates.map do |ymd|
        md = Date.parse(ymd).strftime('%-m/%-d') rescue ymd
        ["#{md}参加希望", "#{md}参加"]
      end.flatten

      headers = [
        '友達追加時刻', 'ブロック日時',
        '流入元', 'line_user_id', '名前', 'ブロック?',
        '動画①_ダイジェスト', 'プロアカ_動画①',
        '動画②_ダイジェスト', 'プロアカ_動画②',
        '動画③_ダイジェスト', 'プロアカ_動画③',
        '', 'プロアカ_動画④', '選択肢',
        '個別相談', '成約'
      ] + seminar_headers

      cols = headers.size

      row3 = Array.new(cols, '')
      row4 = Array.new(cols, '')
      row5 = Array.new(cols, '')
      row3[0] = "今月%（#{this_m}）"
      row4[0] = "前月%（#{prev_m}）"
      row5[0] = "累計%（#{Date.parse(CUM_SINCE).strftime('%Y/%-m')}〜）" rescue row5[0] = "累計%"

      put_percentages_dynamic!(row3, monthly_rates, headers)
      put_percentages_dynamic!(row4, prev_month_rates, headers)
      put_percentages_dynamic!(row5, cumulative_rates, headers)

      # ===== セミナー転換率%（参加希望→参加：積集合 / 希望）を「参加」列の5行目だけに出力 =====
      seminar_rates_cum = seminar_conversion_rates(
        rows,
        seminar_dates: seminar_dates,
        month: nil,              # 月指定なし（全期間）
        since: CUM_SINCE,        # 累計の起点
        until_date: end_on       # 未来日の列を除外したい場合に効く
      )

      base_len = headers.size - seminar_headers.size  # 先頭の固定カラム数
      seminar_dates.each_with_index do |ymd, idx|
        attend_col = base_len + (2 * idx) + 1  # [希望, 参加]のうち「参加」列

        # 今月/前月は空欄のまま（誤読防止）
        row3[attend_col] = ''
        row4[attend_col] = ''

        # 累計%のみ表示
        r_cum = seminar_rates_cum[ymd]
        row5[attend_col] = r_cum ? "#{r_cum}%" : ''
      end

      service.update_spreadsheet_value(
        spreadsheet_id, "#{sheet_name}!B3:#{a1_col(1 + headers.size)}3",
        Google::Apis::SheetsV4::ValueRange.new(values: [row3]),
        value_input_option: 'USER_ENTERED'
      )
      service.update_spreadsheet_value(
        spreadsheet_id, "#{sheet_name}!B4:#{a1_col(1 + headers.size)}4",
        Google::Apis::SheetsV4::ValueRange.new(values: [row4]),
        value_input_option: 'USER_ENTERED'
      )
      service.update_spreadsheet_value(
        spreadsheet_id, "#{sheet_name}!B5:#{a1_col(1 + headers.size)}5",
        Google::Apis::SheetsV4::ValueRange.new(values: [row5]),
        value_input_option: 'USER_ENTERED'
      )

      header_range = "#{sheet_name}!B6:#{a1_col(1 + headers.size)}6"
      service.update_spreadsheet_value(
        spreadsheet_id, header_range,
        Google::Apis::SheetsV4::ValueRange.new(values: [headers]),
        value_input_option: 'USER_ENTERED'
      )

      sorted = Array(rows).sort_by do |r|
        t_follow = (Time.zone.parse(r['followed_at'].to_s) rescue Time.parse(r['followed_at'].to_s) rescue nil)
        [t_follow ? t_follow.to_i : 0, r['line_user_id'].to_i]
      end.reverse

      data_values = sorted.map do |r|
        t = (r['tags_flags'] || {})
        landing = pick_landing_value(r)
        row = [
          to_jp_ymdhm(r['followed_at']),
          to_jp_ymdhm(r['blocked_at']),
          landing.to_s,
          r['line_user_id'],
          hyperlink_line_user(r['line_user_id'], r['name']),
          r['is_blocked'].to_i,
          (t[:dv1] ? 'タグあり' : ''),
          (t[:v1]  ? 'タグあり' : ''),
          (t[:dv2] ? 'タグあり' : ''),
          (t[:v2]  ? 'タグあり' : ''),
          (t[:dv3] ? 'タグあり' : ''),
          (t[:v3]  ? 'タグあり' : ''),
          '',
          (t[:v4]  ? 'タグあり' : ''),
          (t[:select] || ''),
          (r['kobetsu'].presence || ''),
          (r['contracts'].presence || '')
        ]
        sem = r['seminar_map'] || {}
        seminar_dates.each do |ymd|
          flags = sem[ymd] || {}
          row << (flags[:hope]   ? '◯' : '')
          row << (flags[:attend] ? '◯' : '')
        end
        row
      end

      if data_values.any?
        service.update_spreadsheet_value(
          spreadsheet_id, "#{sheet_name}!B7",
          Google::Apis::SheetsV4::ValueRange.new(values: data_values),
          value_input_option: 'USER_ENTERED'
        )
      end
    end

    # ==== Helpers: 集計 ========================================================
    def calc_rates(rows, tags_cache, month:, since:)
      month_rows = Array(rows).select { |r| month_key(r['date']) == month }
      cum_rows   = Array(rows).select { |r| r['date'].to_s >= since.to_s }

      monthly = {
        v1: pct_for(month_rows, tags_cache, :v1),
        v2: pct_for(month_rows, tags_cache, :v2),
        v3: pct_for(month_rows, tags_cache, :v3),
        v4: pct_for(month_rows, tags_cache, :v4)
      }
      cumulative = {
        v1: pct_for(cum_rows, tags_cache, :v1),
        v2: pct_for(cum_rows, tags_cache, :v2),
        v3: pct_for(cum_rows, tags_cache, :v3),
        v4: pct_for(cum_rows, tags_cache, :v4)
      }
      [monthly, cumulative]
    end

    def month_rates(rows, tags_cache, month:)
      month_rows = Array(rows).select { |r| month_key(r['date']) == month }
      {
        v1: pct_for(month_rows, tags_cache, :v1),
        v2: pct_for(month_rows, tags_cache, :v2),
        v3: pct_for(month_rows, tags_cache, :v3),
        v4: pct_for(month_rows, tags_cache, :v4)
      }
    end

    def pct_for(rows, tags_cache, key)
      denom = rows.size
      return nil if denom.zero?
      numer = rows.count { |r| !!(tags_cache[r['line_user_id']] || {})[key] }
      ((numer.to_f / denom) * 100).round(1)
    end

    def month_key(ymd_str)
      Date.parse(ymd_str.to_s).strftime('%Y-%m') rescue nil
    end

    def put_percentages_dynamic!(row_array, rates, headers)
      idx = {
        v1: headers.index('プロアカ_動画①'),
        v2: headers.index('プロアカ_動画②'),
        v3: headers.index('プロアカ_動画③'),
        v4: headers.index('プロアカ_動画④')
      }
      idx.each do |k, i|
        next unless i
        v = rates[k]
        row_array[i] = v.nil? ? '' : "#{v}%"
      end
    end

    # =========================================================
    # 小物ユーティリティ
    # =========================================================
    def normalize_row!(row)
      row['landing_name'] ||= begin
        cands = [
          row['landing_name'],
          row['landing'],
          row['landing_source'],
          row['from'],
          (row.dig('landing', 'name') rescue nil),
          (row.dig('utm', 'source')   rescue nil)
        ].compact_blank
        cands.first.to_s
      end
      row['date'] ||= row['followed_at'].to_s[0,10]
      row
    end

    def safe_landing_name(row)
      row['landing_name'].presence ||
        row['landing'].presence ||
        row['landing_source'].presence ||
        row['from'].presence ||
        (row.dig('landing', 'name') rescue nil).presence ||
        (row.dig('utm', 'source')   rescue nil).presence ||
        ''
    end

    def extract_line_user_id_from_link(path)
      path.to_s.split('/').last.to_i
    end

    def hyperlink_line_user(id, name)
      return name.to_s if id.to_s.strip.empty?
      label = name.to_s.gsub('"', '""')
      url   = "#{ORIGIN}/basic/friendlist/my_page/#{id}"
      %Q(=HYPERLINK("#{url}","#{label}"))
    end

    def jp_timestamp
      Time.now.in_time_zone('Asia/Tokyo').strftime('%Y年%-m月%-d日 %H時%M分')
    end

    def parse_date(str)
      Time.zone.parse(str.to_s) || Time.parse(str.to_s)
    end

    def to_jp_ymdhm(str)
      return '' if str.blank?
      t = (Time.zone.parse(str.to_s) rescue Time.parse(str.to_s) rescue nil)
      return '' unless t
      t.in_time_zone('Asia/Tokyo').strftime('%Y年%-m月%-d日 %H時%M分')
    end

    # A1 形式: 1=>A, 26=>Z, 27=>AA...
    def a1_col(n)
      s = String.new
      while n && n > 0
        n, r = (n - 1).divmod(26)
        s.prepend((65 + r).chr)
      end
      s
    end

    # "'シート名'!B2" のような A1 文字列を生成
    def a1(sheet_name, range)
      name = sheet_name.to_s.gsub("'", "''")
      "'#{name}'!#{range}"
    end

    # === 空欄同等判定 & 流入元の最終決定（landing_name優先） ====================
    def blankish?(v)
      s = v.to_s.strip
      s.empty? || %w[- ー — null NULL Null 未設定].include?(s)
    end

    def pick_landing_value(row)
      ln = row['landing_name']
      return ln unless blankish?(ln)
      qr = row['qr_code']
      return nil if blankish?(qr)
      qr
    end

    # === ブロック情報のマージ（名前・blocked_at を維持/補完） ====================
    def merge_block_info!(raw_rows, blocked_rows)
      # 既存 uid -> 行
      by_uid = {}
      Array(raw_rows).each do |r|
        uid = extract_line_user_id_from_link(r['link_my_page']).to_i
        next if uid <= 0
        by_uid[uid] ||= r
      end

      # uid -> 最新 blocked_at / 名前
      latest_blocked_at = {}
      names_map = {}

      Array(blocked_rows).each do |br|
        uid = (br['line_user_id'] || extract_line_user_id_from_link(br['link_my_page'])).to_i
        next if uid <= 0

        names_map[uid] = (br['name'] || br['view_name'] || names_map[uid] || '')

        b = br['blocked_at'] || br['block_at'] || br['updated_at'] || br['created_at']
        next if b.blank?
        begin
          cur = latest_blocked_at[uid]
          latest_blocked_at[uid] = [cur, b].compact.max_by { |x| Time.parse(x) }
        rescue
          latest_blocked_at[uid] ||= b
        end
      end

      # 既存行に反映／無ければ行を新規作成
      latest_blocked_at.each do |uid, b|
        if by_uid.key?(uid)
          by_uid[uid]['is_blocked'] = 1
          by_uid[uid]['blocked_at'] = b
          if by_uid[uid]['name'].to_s.strip.empty? && names_map[uid].present?
            by_uid[uid]['name'] = names_map[uid]
          end
        else
          newr = {
            'link_my_page' => "#{ORIGIN}/basic/friendlist/my_page/#{uid}",
            'name'         => names_map[uid].to_s,
            'followed_at'  => nil,
            'landing_name' => '',
            'is_blocked'   => 1,
            'blocked_at'   => b
          }
          raw_rows << newr
          by_uid[uid] = newr
        end
      end

      raw_rows
    end

    private

    # === MyPage（サービス経由） -------------------------------------------------
    def fetch_user_basic_info_via_service(mypage_svc, uid)
      json = mypage_svc.fetch_common(line_user_id: uid)
      json = (JSON.parse(json) rescue {}) if json.is_a?(String)

      {
        qr_code:     find_value_by_key(json, 'qr_code'),
        time_follow: find_value_by_key(json, 'time_follow'),
        inflow:      mypage_svc.extract_inflow(json)
      }.with_indifferent_access
    rescue => e
      Rails.logger.debug("[basic_info] uid=#{uid} #{e.class}: #{e.message}")
      {}.with_indifferent_access
    end

    def default_start_on
      raw = ENV['LME_DEFAULT_START_DATE'].presence || '2023-01-01'
      Date.parse(raw).strftime('%F')
    rescue
      '2023-01-01'
    end

    # --- 1) Yamada用ID解決（ID or URL） ---------------------------------
    def resolve_yamada_spreadsheet_id!
      id = ENV['LME_YAMADA_COUNT_SPREADSHEET_ID'].presence
      return id if id.present?

      url = ENV['LME_YAMADA_COUNT_SPREADSHEET_URL'].presence
      if url.to_s =~ %r{\Ahttps?://docs\.google\.com/spreadsheets/d/([A-Za-z0-9_-]+)}
        return Regexp.last_match(1)
      end

      raise 'Copy target Spreadsheet not provided. Set LME_YAMADA_COUNT_SPREADSHEET_ID or LME_YAMADA_COUNT_SPREADSHEET_URL.'
    end

    # --- 2) 汎用: 別スプレッドシートに値を書き出す（シートが無ければ作る） ----------
    def ensure_sheet_exists_in_spreadsheet!(service, spreadsheet_id, sheet_name)
      ss = service.get_spreadsheet(spreadsheet_id)
      exists = ss.sheets.any? { |s| s.properties&.title == sheet_name }
      return if exists

      add_req = Google::Apis::SheetsV4::AddSheetRequest.new(
        properties: Google::Apis::SheetsV4::SheetProperties.new(title: sheet_name)
      )
      batch = Google::Apis::SheetsV4::BatchUpdateSpreadsheetRequest.new(
        requests: [Google::Apis::SheetsV4::Request.new(add_sheet: add_req)]
      )
      service.batch_update_spreadsheet(spreadsheet_id, batch)
    end

    # --- 3) 別ブックコピー（ソース → ターゲット / 式ごと） -------------------------
    def copy_to_yamada_sheet!(service,
                              source_spreadsheet_id:,
                              source_sheet_name:,
                              target_spreadsheet_id:,
                              target_sheet_name:)
      raise 'source_spreadsheet_id is blank' if source_spreadsheet_id.to_s.blank?
      raise 'source_sheet_name is blank'     if source_sheet_name.to_s.blank?
      raise 'target_spreadsheet_id is blank' if target_spreadsheet_id.to_s.blank?
      raise 'target_sheet_name is blank'     if target_sheet_name.to_s.blank?

      src_range = a1(source_sheet_name, 'B2:ZZZ')
      vr = service.get_spreadsheet_values(
        source_spreadsheet_id,
        src_range,
        value_render_option: 'FORMULA',
        date_time_render_option: 'SERIAL_NUMBER'
      )
      values = vr.values || []
      if values.empty?
        Rails.logger.info("[copy_to_yamada_sheet!] No data to copy from #{source_sheet_name}!")
        return
      end

      ensure_sheet_exists_in_spreadsheet!(service, target_spreadsheet_id, target_sheet_name)

      begin
        clear_req = Google::Apis::SheetsV4::ClearValuesRequest.new
        service.clear_values(target_spreadsheet_id, a1(target_sheet_name, 'B2:ZZZ'), clear_req)
      rescue StandardError => e
        Rails.logger.warn("[copy_to_yamada_sheet!] clear skipped: #{e.class} - #{e.message}")
      end

      target_range = a1(target_sheet_name, 'B2')
      begin
        service.update_spreadsheet_value(
          target_spreadsheet_id,
          target_range,
          Google::Apis::SheetsV4::ValueRange.new(values: values),
          value_input_option: 'USER_ENTERED'
        )
        Rails.logger.info("[copy_to_yamada_sheet!] Successfully copied data #{source_sheet_name} -> #{target_sheet_name} (with formulas)")
      rescue StandardError => e
        Rails.logger.error("[copy_to_yamada_sheet!] Error copying data: #{e.class} - #{e.message}")
      end
    end

    def extract_join_months_from_tags(names)
      months = []

      Array(names).each do |raw|
        s = normalize_nfkc(raw)
        next unless s.include?('入会')

        # 1) 2025年7月(9日)入会
        if s =~ /(\d{4})\s*年\s*(\d{1,2})\s*月(?:\s*\d{1,2}\s*日)?/
          y, m = $1.to_i, $2.to_i
          months << format_join_month(y, m) if y > 1900 && (1..12).include?(m)
          next
        end

        # 2) 2025/7( /07/09 ) 入会、2025-07、2025.7 など
        if s =~ /(\d{4})\s*[-\/\.]\s*(\d{1,2})(?:\s*[-\/\.]\s*\d{1,2})?/
          y, m = $1.to_i, $2.to_i
          months << format_join_month(y, m) if y > 1900 && (1..12).include?(m)
          next
        end

        # 3) 07/2025 入会
        if s =~ /(\d{1,2})\s*[-\/\.]\s*(\d{4})/
          m, y = $1.to_i, $2.to_i
          months << format_join_month(y, m) if y > 1900 && (1..12).include?(m)
          next
        end

        # 4) 入会と年月の順序が入れ替わるパターン
        if s =~ /入会.*?(\d{4})\s*[年\/\-\.]\s*(\d{1,2})/
          y, m = $1.to_i, $2.to_i
          months << format_join_month(y, m) if y > 1900 && (1..12).include?(m)
          next
        end
        if s =~ /(\d{4})\s*[年\/\-\.]\s*(\d{1,2}).*?入会/
          y, m = $1.to_i, $2.to_i
          months << format_join_month(y, m) if y > 1900 && (1..12).include?(m)
          next
        end
      end

      months.uniq.sort_by do |txt|
        if txt =~ /(\d{4})年\s*(\d{2})月/
          [$1.to_i, $2.to_i]
        else
          [0, 0]
        end
      end
    end

    def normalize_nfkc(s)
      if s.respond_to?(:unicode_normalize)
        s.unicode_normalize(:nfkc).strip
      else
        s.tr('０-９／－．', '0-9/-.').strip
      end
    end

    def format_join_month(y, m)
      format('%04d年 %02d月入会', y, m)
    end
  end
end
