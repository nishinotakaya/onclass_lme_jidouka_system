require 'cgi'
require 'uri'
require 'json'
require 'faraday'

class LmeHttpClient
  ORIGIN      = 'https://step.lme.jp'
  UA          = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
  ACCEPT_LANG = 'ja,en-US;q=0.9,en;q=0.8'
  CH_UA       = %Q("Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140")

  class << self
    # ====== GET (HTML) + Cookie（リダイレクト追従しつつ Cookie 合流 / XSRF 更新）
    # @return [body, final_url, res, merged_cookie, xsrf_cookie]
    def get_html_with_cookies(cookie_header, path, max_redirects: 5, timeout: 10)
      cur_cookie  = cookie_header.to_s
      cur_path    = path
      final_url   = URI.join(ORIGIN, cur_path).to_s
      last_resp   = nil

      max_redirects.times do
        conn = Faraday.new(url: ORIGIN) do |f|
          f.options.timeout = timeout
          f.adapter Faraday.default_adapter
        end
        res = conn.get(cur_path) do |req|
          req.headers['accept']                    = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
          req.headers['accept-language']           = ACCEPT_LANG
          req.headers['cookie']                    = cur_cookie
          req.headers['user-agent']                = UA
          req.headers['sec-ch-ua']                 = CH_UA
          req.headers['sec-ch-ua-mobile']          = '?0'
          req.headers['sec-ch-ua-platform']        = %Q("macOS")
          req.headers['upgrade-insecure-requests'] = '1'
          req.headers['cache-control']             = 'no-cache'
          req.headers['pragma']                    = 'no-cache'
          req.headers['referer']                   = ORIGIN
        end
        last_resp = res
        final_url = URI.join(ORIGIN, cur_path).to_s

        # Cookie を合流
        cur_cookie = merge_cookie_strings(cur_cookie, res.headers['set-cookie'])
        xsrf       = extract_cookie(cur_cookie, 'XSRF-TOKEN')

        # リダイレクト？
        case res.status.to_i
        when 301, 302, 303, 307, 308
          loc = res.headers['location'].to_s
          break if loc.blank?
          uri = URI.join(ORIGIN, loc)
          cur_path  = uri.request_uri
          final_url = uri.to_s
          # 次の hop でも最新 Cookie を送る
          next
        else
          return [res.body.to_s, final_url, res, cur_cookie, xsrf]
        end
      end

      xsrf = extract_cookie(cur_cookie, 'XSRF-TOKEN')
      [last_resp&.body.to_s, final_url, last_resp, cur_cookie, xsrf]
    rescue => _
      xsrf = extract_cookie(cur_cookie, 'XSRF-TOKEN')
      ['', final_url, last_resp, cur_cookie, xsrf]
    end

    # ====== meta[name=csrf-token] 抽出
    def extract_meta_csrf(html)
      return nil if html.blank?
      html[/<meta[^>]+name=["']csrf-token["'][^>]*content=["']([^"']+)["']/i, 1] ||
        html[/csrfToken["']?\s*[:=]\s*["']([^"']+)["']/i, 1]
    end

    # ===== Helpers (Cookie/XSRF) ============================================

    # 既存CookieとSet-Cookieをマージして "k=v; k2=v2" を返す
    def merge_cookie_strings(old_cookie, set_cookie)
      jar = {}
      old_cookie.to_s.split(';').each do |pair|
        k, v = pair.strip.split('=', 2)
        next if k.blank?
        jar[k] = v.to_s
      end
      Array(set_cookie).each do |sc|
        sc.to_s.split("\n").each do |line|
          nv = line.split(';', 2).first # "name=value"
          k, v = nv.split('=', 2)
          next if k.blank?
          jar[k.strip] = v.to_s
        end
      end
      jar.map { |k, v| "#{k}=#{v}" }.join('; ')
    end

    # Cookie 文字列から特定キーの値を抜く
    def extract_cookie(cookie_str, key)
      return nil if cookie_str.blank?
      cookie_str.split(';').map(&:strip).each do |pair|
        k, v = pair.split('=', 2)
        return v if k == key
      end
      nil
    end
  end
end