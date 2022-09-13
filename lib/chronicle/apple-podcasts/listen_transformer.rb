require 'chronicle/etl'
require 'reverse_markdown'

module Chronicle
  module ApplePodcasts
    class ListenTransformer < Chronicle::ETL::Transformer
      register_connector do |r|
        r.provider = 'apple-podcasts'
        r.description = 'a history item'
        r.identifier = 'listen'
      end

      def transform
        @data = @extraction.data
        build_listened
      end

      def id
        @data[:ZSTORETRACKID]
      end

      def timestamp
        Time.find_zone("UTC").parse(@data[:last_played_utc])
      end

      private

      def build_listened
        record = ::Chronicle::ETL::Models::Activity.new({
          verb: 'listened',
          provider: 'apple-podcasts',
          provider_id: id,
          end_at: timestamp
        })
        record.dedupe_on << [:provider, :verb, :provider_id]
        record.actor = build_actor
        record.involved = build_episode
        record
      end

      def build_episode
        record = ::Chronicle::ETL::Models::Entity.new({
          title: @data[:ZTITLE],
          represents: 'episode',
          body: body_as_markdown(@data[:ZITEMDESCRIPTION]),
          provider: 'apple-podcasts',
          provider_id: id,
        })
        record.containers = [build_podcast]
        record.involvements = [build_published]
        record
      end

      def build_podcast
        record = ::Chronicle::ETL::Models::Entity.new({
          title: @data[:podcast_ZTITLE],
          body: body_as_markdown(@data[:podcast_ZITEMDESCRIPTION]),
          provider: 'apple-podcasts',
          provider_id: @data[:podcast_ZSTORECOLLECTIONID],
          provider_url: @data[:podcast_ZSTORECLEANURL],
        })
        record.dedupe_on << [:provider_url]
        record.dedupe_on << [:provider, :provider_id, :represents]
        record.attachments = ::Chronicle::ETL::Models::Attachment.new({
          url_original: @data[:podcast_ZIMAGEURL]
        })
        record
      end

      def build_published
        record = ::Chronicle::ETL::Models::Activity.new({
          verb: 'published',
          provider: 'apple-podcasts',
          provider_id: id,
          end_at: Time.find_zone("UTC").parse(@data[:published_at_utc])
        })
        record.dedupe_on << [:provider, :verb, :provider_id]
        record.actor = build_creator
        record
      end

      def build_creator
        record = ::Chronicle::ETL::Models::Entity.new({
          title: @data[:ZAUTHOR],
          provider: 'apple-podcasts',
          represents: 'artist'
        })
        record.dedupe_on << [:provider, :title, :represents]
        record
      end

      def build_actor
        record = ::Chronicle::ETL::Models::Entity.new({
          represents: 'identity',
          provider: 'icloud',
          provider_id: @extraction.meta[:icloud_account][:dsid],
          title: @extraction.meta[:icloud_account][:display_name],
          slug: @extraction.meta[:icloud_account][:id]
        })
        record.dedupe_on << [:provider, :represents, :slug]
        record
      end

      def body_as_markdown(body)
        ReverseMarkdown.convert(body)&.strip
      end
    end
  end
end
