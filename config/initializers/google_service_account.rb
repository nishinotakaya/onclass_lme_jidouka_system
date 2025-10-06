# config/initializers/google_service_account.rb
require "fileutils"
require "base64"
if (ENV["RAILS_GROUPS"] || "").split(":").include?("assets") || ENV["ASSETS_PRECOMPILE"] == "true"
  Rails.logger.info "[GSA] Skip on assets:precompile"
  return
end
target_path = ENV["GOOGLE_APPLICATION_CREDENTIALS"].presence ||
              "/app/config/keys/google_service_account.json"

b64 = ENV["GOOGLE_SERVICE_ACCOUNT_JSON_BASE64"].to_s.strip
if b64.empty?
  Rails.logger.warn "[GSA] GOOGLE_SERVICE_ACCOUNT_JSON_BASE64 is empty; skip writing #{target_path}"
else
  begin
    FileUtils.mkdir_p(File.dirname(target_path))
    File.binwrite(target_path, Base64.decode64(b64))
    Rails.logger.info "[GSA] Wrote service account key to #{target_path}"
  rescue => e
    Rails.logger.error "[GSA] Failed to write key: #{e.class} #{e.message}"
  end
end
