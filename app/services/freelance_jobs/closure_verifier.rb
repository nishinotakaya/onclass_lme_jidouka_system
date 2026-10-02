# frozen_string_literal: true

module FreelanceJobs
  # AC-12: 一覧ページには募集終了の印が出ないサイト（closed_detail?を持つsource_class。
  # 例: エンジニアファクトリー）について、シートに関係する行（今回の新規候補＋既存行）だけ
  # 詳細ページを取得して確認し、募集終了と判明したURLを集める。
  # 一覧の全件を毎回詳細確認すると取得元へのリクエスト数が跳ね上がるため、
  # 一覧側のfetchには詳細確認を入れず、シート起因の行に絞るのがこのクラスの役目。
  class ClosureVerifier
    # 1サイトあたりの詳細確認上限（1回の実行での通信量に上限を設ける）。
    MAX_CHECKS_PER_SITE = 80

    def initialize(source_specs:, fetcher_factory:, logger: FreelanceJobs.logger)
      @source_specs = source_specs
      @fetcher_factory = fetcher_factory
      @logger = logger
    end

    # 通信あり。source_specsのうちclosed_detail?を持つサイトだけを対象に詳細確認し、
    # 募集終了と判明したURL（normalize_url済み）をまとめて返す。
    def call(candidate_rows:, existing_rows:)
      @source_specs.each_with_object([]) do |(source_class, _options), closed_urls|
        next unless source_class.respond_to?(:closed_detail?)

        closed_urls.concat(verify_site(source_class, candidate_rows, existing_rows))
      end
    end

    private

    # 1サイトぶんの確認。対象URLが0件ならfetcherすら作らない
    # （REQUEST_INTERVALのsleepを含むfetcherを無駄に生成しないため）。
    def verify_site(source_class, candidate_rows, existing_rows)
      site_name = source_class::SITE_NAME
      target_urls = target_urls_for(site_name, candidate_rows, existing_rows)
      return [] if target_urls.empty?

      checked_urls, skipped_count = cap_target_urls(target_urls)
      warn_about_skipped_urls(site_name, skipped_count) if skipped_count.positive?

      check_urls(source_class, site_name, @fetcher_factory.call(source_class), checked_urls)
    end

    # 確認対象URL（行に入っている表記のまま）を、
    # ①existing_rowsの同サイト行→②candidate_rowsのうちexisting_rowsに無い新規候補行、の順で並べる。
    # 既存行を先にするのは、今シートに載っていて利用者に見えている行ほど終了の見逃しの害が大きく、
    # かつ件数が少ない（サイトあたり最大40行程度）のでMAX_CHECKS_PER_SITEの内に必ず収まるため。
    # 新規候補が上限で未確認のまま追加されても、翌日の実行では既存行として先に確認されるので回収される。
    def target_urls_for(site_name, candidate_rows, existing_rows)
      candidate_urls = raw_urls_for_site(candidate_rows, site_name)
      existing_urls = raw_urls_for_site(existing_rows, site_name)
      existing_normalized_urls = existing_urls.each_with_object({}) do |url, normalized_urls|
        normalized_urls[FreelanceJobs::JobPosting.normalize_url(url)] = true
      end

      new_candidate_urls = candidate_urls.reject do |url|
        existing_normalized_urls[FreelanceJobs::JobPosting.normalize_url(url)]
      end

      dedupe_by_normalized_url(existing_urls + new_candidate_urls)
    end

    # 対象サイトの行のURL列を、正規化後が空文字の行を除いて元の並び順のまま取り出す。
    def raw_urls_for_site(rows, site_name)
      rows.each_with_object([]) do |row, urls|
        next unless row[FreelanceJobs::SheetMerger::SITE_COLUMN_INDEX] == site_name

        url = row[FreelanceJobs::SheetMerger::URL_COLUMN_INDEX].to_s
        urls << url unless FreelanceJobs::JobPosting.normalize_url(url).empty?
      end
    end

    # 正規化後のURLをキーに重複排除し、先に現れた表記のものを残す。
    def dedupe_by_normalized_url(urls)
      seen_normalized_urls = {}
      urls.each_with_object([]) do |url, deduped_urls|
        normalized_url = FreelanceJobs::JobPosting.normalize_url(url)
        next if seen_normalized_urls[normalized_url]

        seen_normalized_urls[normalized_url] = true
        deduped_urls << url
      end
    end

    def cap_target_urls(target_urls)
      return [target_urls, 0] if target_urls.size <= MAX_CHECKS_PER_SITE

      [target_urls.first(MAX_CHECKS_PER_SITE), target_urls.size - MAX_CHECKS_PER_SITE]
    end

    # 1サイトぶんのURLを順に確認する。AccessBlockedErrorが出たら以降のURLは確認せず
    # そのサイトの確認を打ち切る（ブロック前に判定できた分は戻り値に残す）。
    def check_urls(source_class, site_name, fetcher, target_urls)
      closed_urls = []
      checked_count = 0

      target_urls.each do |url|
        checked_count += 1
        result = check_single_url(source_class, site_name, fetcher, url)
        break if result == :access_blocked

        closed_urls << FreelanceJobs::JobPosting.normalize_url(url) if result == :closed
      end

      @logger.info(
        "[FreelanceJobs::ClosureVerifier] #{site_name} 詳細確認 #{checked_count} 件中 #{closed_urls.size} 件が募集終了"
      )
      closed_urls
    end

    # 1URLぶんの判定。:closed / :open / :access_blocked / :error のいずれかを返す。
    def check_single_url(source_class, site_name, fetcher, url)
      body = fetcher.get(url)
      source_class.closed_detail?(body) ? :closed : :open
    rescue FreelanceJobs::AccessBlockedError => error
      @logger.warn(
        "[FreelanceJobs::ClosureVerifier] #{site_name} 詳細確認中にアクセス制限が発生したため、" \
        "このサイトの確認を打ち切ります: #{error.message}"
      )
      :access_blocked
    rescue StandardError => error
      # サイト側の一時障害（タイムアウト等）で既存行を誤って募集終了扱いにして消してしまわないよう、
      # このURLだけ未判定のまま次のURLへ進む。
      @logger.warn(
        "[FreelanceJobs::ClosureVerifier] #{site_name} #{url} の詳細確認に失敗したため未判定のまま次へ進みます: #{error.message}"
      )
      :error
    end

    def warn_about_skipped_urls(site_name, skipped_count)
      @logger.warn(
        "[FreelanceJobs::ClosureVerifier] #{site_name} 詳細確認の上限(#{MAX_CHECKS_PER_SITE})を超えたため " \
        "#{skipped_count} 件は未確認"
      )
    end
  end
end
