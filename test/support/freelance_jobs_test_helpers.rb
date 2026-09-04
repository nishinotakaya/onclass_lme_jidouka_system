# frozen_string_literal: true
# test/support/freelance_jobs_test_helpers.rb
#
# FreelanceJobs のテスト専用ヘルパー（fixture読み込み・小さなHTML/JSON断片の組み立て）。
# app/ 側のコードには一切依存を追加しない（Nokogiri/JSONなどgemのみ使用）。

require "nokogiri"
require "json"

module FreelanceJobsTestHelpers
  FIXTURE_DIR = File.expand_path("../fixtures/files/freelance_jobs", __dir__)

  def fixture_path(name)
    File.join(FIXTURE_DIR, name)
  end

  def read_fixture(name)
    File.read(fixture_path(name))
  end

  # CrowdWorksの `<div id="vue-container" data="...">` を安全に組み立てる
  # （Nokogiriにアトリビュートのエスケープを任せるのでJSON中の引用符・タグ文字を気にしなくてよい）。
  def build_crowdworks_body(search_result_hash)
    document = Nokogiri::HTML::Document.new
    container = Nokogiri::XML::Node.new("div", document)
    container["id"] = "vue-container"
    container["data"] = JSON.generate({ "searchResult" => search_result_hash })

    body_node = Nokogiri::XML::Node.new("body", document)
    body_node.add_child(container)
    document.root = Nokogiri::XML::Node.new("html", document)
    document.root.add_child(body_node)
    document.to_html
  end

  # 1件分のCrowdWorks job_offerエントリ（デフォルト値はfixedの正常系）。
  def build_crowdworks_job_offer(id: 1, title: "テスト案件", category_id: 16, payment: { "fixed_price_payment" => { "min_budget" => 1000, "max_budget" => 2000 } },
                                  num_contracts: 0, hope_number: 2, num_application_conditions: 1,
                                  expired_on: "2026-12-31", last_released_at: "2026-09-01T10:00:00+09:00",
                                  skills: [], username: "tester")
    {
      "job_offer" => {
        "id" => id, "title" => title, "description_digest" => "説明文です。",
        "category_id" => category_id, "skills" => skills, "options" => [], "status" => "released",
        "expired_on" => expired_on, "last_released_at" => last_released_at, "is_login_required" => false
      },
      "payment" => payment,
      "entry" => { "project_entry" => { "num_contracts" => num_contracts, "project_contract_hope_number" => hope_number,
                                         "num_application_conditions" => num_application_conditions } },
      "client" => { "user_id" => 1, "username" => username, "user_picture_url" => "", "is_employer_certification" => false }
    }
  end

  # ランサーズ検索カード1件分のHTML片（実際のDOM構造を模したもの）。
  def build_lancers_card_html(job_id:, title:, badge_text: "プロジェクト", price_text: "10,000円 ~ 20,000円 / 固定",
                               remaining_text: "あと5日", proposal_numbers: %w[0 2], tags: [], description: "説明文です。")
    tag_list_html = tags.map { |tag| "<li class=\"p-search-job-media__tag-list\">#{tag}</li>" }.join
    <<~HTML
      <div class="p-search-job-media">
        <div class="p-search-job-media__content">
          <div class="p-search-job-media__content-left">
            <div class="p-search-job-media__time">
              <span class="p-search-job-media__time-remaining">#{remaining_text}</span>
            </div>
          </div>
          <div class="p-search-job-media__content-right">
            <a class="p-search-job-media__title" href="/work/detail/#{job_id}">#{title}</a>
            <span class="c-badge--worktype"><span class="c-badge__text">#{badge_text}</span></span>
            <span class="p-search-job-media__price">#{price_text}</span>
            <div class="js-job-show-description">#{description}</div>
            <ul class="p-search-job-media__tag-lists">#{tag_list_html}</ul>
            <div class="p-search-job-media__proposals">#{proposal_numbers.join(" / ")}</div>
            <a href="/client/tester_client">tester_client</a>
          </div>
        </div>
      </div>
    HTML
  end

  def wrap_html(fragment)
    "<html><body>#{fragment}</body></html>"
  end

  # クラウディア一覧アイテム1件分のHTML片（実データ構造を模したもの）。
  def build_craudia_item_html(href:, title:, work_type: "固定報酬制", reward: "10,000 円",
                               applicant_count: "2", deadline_days: 10)
    <<~HTML
      <div class="work-list__item-inner">
        <div class="work-list__detail">
          <a href="#{href}" class="work-list__title">#{title}</a>
          <div class="work-list__data">
            <div class="work-list__work-type">#{work_type}</div>
            <div class="work-list__reward">#{reward}</div>
          </div>
          <div class="work-list__status">
            <div>
              <dt>参加申請数</dt>
              <dd><span>#{applicant_count}</span> 件</dd>
            </div>
            <div>
              <dt>募集期間</dt>
              <dd>あと <span>#{deadline_days}</span> 日</dd>
            </div>
          </div>
        </div>
        <div class="work-list__other"></div>
      </div>
    HTML
  end
end
