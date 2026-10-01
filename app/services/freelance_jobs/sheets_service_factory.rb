# frozen_string_literal: true

require "google/apis/sheets_v4"
require "googleauth"
require "json"

module FreelanceJobs
  # Google Sheets API の認証済みサービスを作る。SheetsClient（案件一覧）と SiteListSheet
  # （申込サイト一覧の件数列）で同じ認証手順を共用するために切り出した。
  # 認証は Youtube::CompetitorWorker#build_sheets_service と同じ流儀
  # （サービスアカウントJSON鍵、ENV["GOOGLE_APPLICATION_CREDENTIALS"]）。
  module SheetsServiceFactory
    def self.build(application_name:)
      service = Google::Apis::SheetsV4::SheetsService.new
      service.client_options.application_name = application_name
      scope = [Google::Apis::SheetsV4::AUTH_SPREADSHEETS]
      keyfile = ENV["GOOGLE_APPLICATION_CREDENTIALS"]

      raise FreelanceJobs::FetchError, "ENV GOOGLE_APPLICATION_CREDENTIALS is not set." if keyfile.nil? || keyfile.strip.empty?
      raise FreelanceJobs::FetchError, "Service account key not found: #{keyfile}" unless File.exist?(keyfile)

      json = begin
        JSON.parse(File.read(keyfile))
      rescue JSON::ParserError
        nil
      end
      unless json && json["type"] == "service_account" && json["private_key"] && json["client_email"]
        raise FreelanceJobs::FetchError, "Invalid service account JSON: missing private_key/client_email/type=service_account"
      end

      authorizer = Google::Auth::ServiceAccountCredentials.make_creds(json_key_io: File.open(keyfile), scope: scope)
      authorizer.fetch_access_token!
      service.authorization = authorizer
      service
    end
  end
end
