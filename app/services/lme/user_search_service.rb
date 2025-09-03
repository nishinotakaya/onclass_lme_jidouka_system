# app/services/lme/user_search_service.rb
# frozen_string_literal: true
module Lme
  class UserSearchService < BaseService
    # ここはプロジェクトの要件/APIに合わせて拡張してください。
    # 例: キーワード検索エンドポイントがあるなら search(conn, keyword:)
    # 例: line_user_id から簡易情報取得など。

    def search(_conn, keyword:)
      # 例示：未確定APIのため未実装。必要になったら実装してください。
      raise NotImplementedError, "User search API endpoint is not defined yet. keyword=#{keyword.inspect}"
    end
  end
end
