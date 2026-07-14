# frozen_string_literal: true

# LMEログインセッションの永続レコード（bot単位で1本）
class LmeSession < ApplicationRecord
  validates :bot_id, presence: true, uniqueness: true

  def expired?
    expires_at.present? && expires_at < Time.current
  end
end
