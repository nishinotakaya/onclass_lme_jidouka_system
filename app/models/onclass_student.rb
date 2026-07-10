# frozen_string_literal: true

# オンクラス受講生（コース別）
class OnclassStudent < ApplicationRecord
  validates :user_id, presence: true
  validates :course_id, presence: true, uniqueness: { scope: :user_id }
end
