# frozen_string_literal: true
require "test_helper"

module Lme
  class LineInflowsWorkerSeminarTagTest < ActiveSupport::TestCase
    setup do
      Time.zone = "Asia/Tokyo"
      @worker = Lme::LineInflowsWorker.new
    end

    test "種別が先の従来形式を解析できる" do
      assert_equal [:hope,   "2025-10-09"], @worker.send(:parse_seminar_tag_name, "参加希望 2025/10/09")
      assert_equal [:attend, "2025-10-09"], @worker.send(:parse_seminar_tag_name, "参加 2025-10-09")
      assert_equal [:hope,   "2025-10-09"], @worker.send(:parse_seminar_tag_name, "参加希望2025年10月9日（木）")
      assert_equal [:hope,   "2025-10-09"], @worker.send(:parse_seminar_tag_name, "セミナー参加希望 2025/10/09 19時〜")
    end

    test "日付が先の体験会形式を解析できる" do
      assert_equal [:hope,   "2026-09-10"], @worker.send(:parse_seminar_tag_name, "2026年9月10日 体験会参加希望")
      assert_equal [:attend, "2026-09-10"], @worker.send(:parse_seminar_tag_name, "2026年9月10日 体験会参加")
      assert_equal [:hope,   "2026-09-10"], @worker.send(:parse_seminar_tag_name, "２０２６年９月１０日　体験会参加希望")
      assert_equal [:hope,   "2026-09-10"], @worker.send(:parse_seminar_tag_name, "【体験会】2026年9月10日 参加希望")
    end

    test "年省略は当年として扱う" do
      expected_ymd = "#{Time.zone.today.year}-09-10"
      assert_equal [:hope, expected_ymd], @worker.send(:parse_seminar_tag_name, "9月10日 体験会参加希望")
    end

    test "セミナー形式でないタグと無効日付は nil" do
      assert_nil @worker.send(:parse_seminar_tag_name, "プロアカ決済完了")
      assert_nil @worker.send(:parse_seminar_tag_name, "2026年9月10日")
      assert_nil @worker.send(:parse_seminar_tag_name, "参加希望")
      assert_nil @worker.send(:parse_seminar_tag_name, "参加希望 2025/13/40")
    end

    test "カテゴリを問わずタグ名の形式で参加希望・参加を集約する" do
      categories = [
        { "id" => 1, "tags" => [{ "name" => "2026年9月10日 体験会参加希望" }] },
        { "id" => Lme::LineInflowsWorker::PROAKA_SEMINAR_CATEGORY,
          "tags" => [{ "name" => "参加希望 2025/12/11" }, { "name" => "参加 2025/12/11" }] }
      ]
      seminar_map = @worker.send(:seminar_map_from_categories, categories)

      assert_equal({ hope: true,  attend: false }, seminar_map["2026-09-10"])
      assert_equal({ hope: true,  attend: true  }, seminar_map["2025-12-11"])
    end
  end
end
