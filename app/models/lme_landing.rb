# frozen_string_literal: true

# LMEのQRコードアクション（ランディング）
class LmeLanding < ApplicationRecord
  validates :landing_id, presence: true, uniqueness: true

  def youtube_videos
    return YoutubeVideo.none if code.blank?

    YoutubeVideo.where(uland_code: code)
  end
end
