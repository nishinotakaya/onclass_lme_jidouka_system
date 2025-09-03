# app/services/lme/proaka_config.rb
# frozen_string_literal: true
module Lme
  module ProakaConfig
    PROAKA_CATEGORY_ID = 5_180_568

    PROAKA_TAGS = {
      v1: 1_394_734, # プロアカ_動画①
      v2: 1_394_736, # プロアカ_動画②
      v3: 1_394_737, # プロアカ_動画③
      v4: 1_394_738  # プロアカ_動画④
    }.freeze

    PROAKA_DIGEST_NAMES = {
      dv1: '動画①_ダイジェスト',
      dv2: '動画②_ダイジェスト',
      dv3: '動画③_ダイジェスト'
    }.freeze

    RICHMENU_SELECT_NAMES = [
      '月収40万円のエンジニアになれる方法を知りたい',
      'プログラミング無料体験したい',
      '現役エンジニアに質問したい'
    ].freeze
  end
end
