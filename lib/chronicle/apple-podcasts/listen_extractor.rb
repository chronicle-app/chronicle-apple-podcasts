require 'chronicle/etl'
require 'sqlite3'

module Chronicle
  module ApplePodcasts
    class ListenExtractor < Chronicle::ETL::Extractor
      register_connector do |r|
        r.provider = 'apple-podcasts'
        r.identifier = 'listens'
        r.description = 'listening history'
      end

      # macOS Catalina and higher
      setting :input, default: File.join(Dir.home, 'Library', 'Group Containers', '243LU875E5.groups.com.apple.podcasts', 'Documents', 'MTLibrary.sqlite'), required: true
      
      setting :icloud_account_id
      setting :icloud_account_account_dsid
      setting :icloud_account_account_display_name

      def prepare
        @db = SQLite3::Database.new(@config.input, results_as_hash: true, readonly: true)
        @icloud_account = load_icloud_account
        @history = load_history
      end

      def extract
        @history.each do |entry|
          entry.transform_keys!(&:to_sym)
          yield Chronicle::ETL::Extraction.new(data: entry, meta: { icloud_account: @icloud_account } )
        end
      end

      def results_count
        @history.count
      end

      private

      def load_icloud_account
        {
          id: @config.icloud_account_id || icloud_account_info_default[:AccountID],
          dsid: @config.icloud_account_dsid || icloud_account_info_default[:AccountDSID],
          display_name: @config.icloud_account_display_name || icloud_account_info_default[:DisplayName]
        }
      end

      def icloud_account_info_default
        @icloud_account_info_default || begin
          output = `defaults read MobileMeAccounts Accounts | plutil -convert json -r -o - -- -`
          JSON.parse(output, symbolize_names: true).first
        end
      end

      def load_history
        conditions = []
        conditions << "last_played_utc < datetime('now')"
        conditions << "last_played_utc > '#{@config.since.utc}'" if @config.since
        conditions << "last_played_utc < '#{@config.until.utc}'" if @config.until

        sql = <<~SQL
          SELECT
          datetime (episode.ZLASTDATEPLAYED + 978307200,
            "unixepoch") AS last_played_utc,
          datetime (episode.ZPUBDATE + 978307200,
            "unixepoch") AS published_at_utc,
          episode.ZTITLE,
          episode.ZITEMDESCRIPTION,
          episode.ZSTORETRACKID,
          episode.ZAUTHOR,
          pod.ZAUTHOR AS podcast_ZAUTHOR,
          episode.ZWEBPAGEURL,
          episode.ZENCLOSUREURL,
          pod.ZTITLE AS podcast_ZTITLE,
          pod.ZITEMDESCRIPTION AS podcast_ZITEMDESCRIPTION,
          pod.ZSTORECOLLECTIONID AS podcast_ZSTORECOLLECTIONID,
          pod.ZSTORECLEANURL AS podcast_ZSTORECLEANURL,
          pod.ZIMAGEURL AS podcast_ZIMAGEURL,
          pod.ZCATEGORY AS podcast_ZCATEGORY,
          pod.ZWEBPAGEURL AS podcast_ZWEBPAGEURL,
          pod.ZFEEDURL AS podcast_ZFEEDURL
        FROM
          ZMTEPISODE AS episode
          JOIN ZMTPODCAST AS pod ON pod.Z_PK = episode.ZPODCAST
        SQL
        
        sql += " WHERE #{conditions.join(" AND ")}" if conditions.any?
        sql += " ORDER BY last_played_utc DESC"
        sql += " LIMIT #{@config.limit}" if @config.limit

        results = @db.execute(sql)
      end
    end
  end
end
