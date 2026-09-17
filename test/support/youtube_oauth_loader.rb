# frozen_string_literal: true

# Youtube::OauthAlertNotifier を Rails なしで読むためのローダー。
require "support/rails_stub_loader"

require File.expand_path("../../app/services/youtube/oauth_alert_notifier", __dir__)
