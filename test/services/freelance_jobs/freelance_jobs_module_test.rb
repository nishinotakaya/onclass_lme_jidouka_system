# frozen_string_literal: true
# test/services/freelance_jobs/freelance_jobs_module_test.rb
#
# FreelanceJobsモジュール直下の共通ヘルパー（format_number, logger）を検証する。

require_relative "../../support/freelance_jobs_loader"
require "logger"

class FreelanceJobsModuleTest < Minitest::Test
  def test_format_number_adds_three_digit_separators
    assert_equal "1,000", FreelanceJobs.format_number(1000)
    assert_equal "150,000", FreelanceJobs.format_number(150_000)
    assert_equal "1,234,567", FreelanceJobs.format_number(1_234_567)
  end

  def test_format_number_leaves_numbers_under_1000_unseparated
    assert_equal "0", FreelanceJobs.format_number(0)
    assert_equal "999", FreelanceJobs.format_number(999)
  end

  def test_format_number_truncates_decimal_part
    assert_equal "1,500", FreelanceJobs.format_number(1500.0)
    assert_equal "1,500", FreelanceJobs.format_number(1500.99)
  end

  def test_format_number_returns_nil_for_nil_input
    assert_nil FreelanceJobs.format_number(nil)
  end

  def test_format_number_handles_numeric_strings
    assert_equal "12,345", FreelanceJobs.format_number("12345")
  end

  def test_logger_returns_a_usable_logger_instance
    refute_nil FreelanceJobs.logger
    assert_respond_to FreelanceJobs.logger, :info
    assert_respond_to FreelanceJobs.logger, :error
  end

  def test_fetch_error_is_a_standard_error
    assert_kind_of StandardError, FreelanceJobs::FetchError.new("boom")
  end
end
