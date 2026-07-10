# frozen_string_literal: true

# YouTube動画（概要欄のLME URL・出演者・LMEランディング紐付けを保持）
class YoutubeVideo < ApplicationRecord
  validates :video_id, presence: true, uniqueness: true

  # uLandコード経由でランディングと紐付く
  def lme_landing
    return nil if uland_code.blank?

    LmeLanding.find_by(code: uland_code)
  end
end
