require 'akane/storages/abstract_storage'
require 'akane-bigquery'

require 'thread'

module Akane
  module Storages
    class Bigquery < AbstractStorage
      class Stop < Exception; end # :nodoc:

      def initialize(*)
        super

        @client, @api = AkaneBigquery.make_bigquery_client(@config)

        @project_id = @config['project_id']
        @dataset_id = @config['dataset_id']

        @lock = Mutex.new
        @thread = nil

        @flush_interval = @config['flush_interval'] ? @config['flush_interval'].to_i : 60
        @flush_threshold = @config['flush_threshold'] ? @config['flush_threshold'].to_i : 1000

        @pending_inserts = []
        @failing_inserts = []
        @pending_inserts_lock = Mutex.new

        swap_buffers # initialize
        start
      end

      def name
        @name ||= "bigquery:#{@project_id}/#{@dataset_id}"
      end

      def bq_insert(table, row)
        @lock.synchronize do
          @buffers[table] << row
        end
        self
      end

      def start
        @lock.synchronize do
          unless @thread
            @thread = Thread.new(&method(:worker_loop))
            @stop = false
          end
        end
      end

      def exitable?
        @stop && (@thread ? @thread.alive? : true)
      end

      def stop!
        @lock.synchronize do
          super
          @thread.raise(Stop) if @thread
        end
      end

      def record_tweet(account, tweet)
        hash = tweet.attrs
        row = {
          'json'.freeze => hash.to_json,
          'id_str'.freeze => hash[:id_str],
          'id'.freeze => hash[:id],
          'text'.freeze => hash[:text],
          'lang'.freeze => hash[:lang],
          'source'.freeze => hash[:source],
          'in_reply_to_status_id'.freeze => hash[:in_reply_to_status_id],
          'in_reply_to_status_id_str'.freeze => hash[:in_reply_to_status_id_str],
          'in_reply_to_user_id'.freeze => hash[:in_reply_to_user_id],
          'in_reply_to_user_id_str'.freeze => hash[:in_reply_to_user_id_str],
          'in_reply_to_screen_name'.freeze => hash[:in_reply_to_screen_name],
          'user'.freeze => {
            'id_str'.freeze => hash[:user][:id_str],
            'id'.freeze => hash[:user][:id],
            'name'.freeze => hash[:user][:name],
            'screen_name'.freeze => hash[:user][:screen_name],
            'protected'.freeze => hash[:user][:protected],
          },
          'created_at'.freeze => Time.parse(hash[:created_at]).to_i
        }

        if hash['coordinates'.freeze]
          row['coordinates_longitude'.freeze], row['coordinates_latitude'.freeze] = \
            hash[:coordinates][:coordinates]
        end

        if hash[:place]
          place = hash[:place]
          row['place'.freeze] = {
            'id'.freeze => place[:id],
            'country'.freeze => place[:country],
            'country_code'.freeze => place[:country_code],
            'name'.freeze => place[:name],
            'full_name'.freeze => place[:full_name],
            'place_type'.freeze => place[:place_type],
            'url'.freeze => place[:url],
          }
        end

        bq_insert :tweets, row
      end

      def mark_as_deleted(account, user_id, tweet_id)
        bq_insert(:deletions,
          'user_id'.freeze => user_id,
          'user_id_str'.freeze => user_id.to_s,
          'tweet_id'.freeze => tweet_id,
          'tweet_id_str'.freeze => tweet_id.to_s,
          'deleted_at'.freeze => Time.now.to_i,
        )
      end

      def record_event(account, event)
        source = event['source'.freeze]
        target = event['target'.freeze]
        target_object = event['target_object'.freeze]

        source_id = source[:id]
        target_id = target[:id]

        unless source_id && target_id
          @logger.warn "Discarding event because source and target id is missing: #{event.inspect}"
          return
        end

        hash = Hash[
          event.map { |k,v| [k, v && v.respond_to?(:attrs) ? v.attrs : nil] }
        ]

        row = {
          'json'.freeze => hash.to_json,
          'event'.freeze => event['event'.freeze],
          'source_id'.freeze => source_id,
          'source_id_str'.freeze => source_id.to_s,
          'target_id'.freeze => target_id,
          'target_id_str'.freeze => target_id.to_s,
          'created_at'.freeze => Time.now.to_i
        }

        if target_object && target_object[:id]
          id = target_object[:id]
          row['target_object_id'.freeze] = id
          row['target_object_id_str'.freeze] = id.to_s
        end

        p row
        bq_insert :events, row
      end

      def record_message(account, message)
      end

      def status
        @buffers ? @buffers.map{ |table, buf| "#{table}=#{buf.size}" }.join(', ') + " | #{@failing_inserts.size} failures, #{@pending_inserts.size} inserts" : "-"
      end

      private

      def swap_buffers
        @lock.synchronize do
          old_buffers = @buffers
          @buffers = {tweets: [], messages: [], deletions: [], events: []}

          old_buffers
        end
      end

      def worker_loop
        @last_flush = Time.now
        retry_interval = 1

        begin
          flush_pending_inserts

          loop do
            if @flush_interval <= (Time.now - @last_flush) || @flush_threshold <= @buffers.values.map(&:size).inject(:+)
              flush_buffer
            end

            flush_pending_inserts

            sleep 1
          end
        rescue Stop
          @logger.info "Flushing buffer for graceful quit"
          flush_buffer
          until @pending_inserts.empty? && @failing_inserts.empty?
            flush_pending_inserts(true)
            sleep 10 unless @failing_inserts.empty?
          end
        rescue Exception => e
          @logger.error "#{name} - Encountered error on buffer worker"
          @logger.error e.inspect
          @logger.error e.backtrace.join("\n")

          @logger.error "Retrying after #{retry_interval.to_i}"
          sleep retry_interval.to_i
          retry_interval *= 1.8
          retry
        end
      end

      def flush_buffer
        prev_buffers = swap_buffers()

        prev_buffers.each do |table, rows|
          next if rows.empty?

          insert_id_base = "#{Time.now.to_f}:#{rows.__id__}:#{table}"
          request = {
            api_method: @api.tabledata.insert_all,
            parameters: {
              'datasetId' => @dataset_id,
              'projectId' => @project_id,
              'tableId' => table.to_s,
            },
            body_object: {
              'rows' => rows.map.with_index { |row, index|
                {
                  'insertId'.freeze => "#{insert_id_base}:#{index}",
                  'json'.freeze => row,
                }
              }
            }
          }
          @pending_inserts_lock.synchronize do
            @logger.debug "Adding pending inserts for #{table}, #{rows.size} rows"
            @pending_inserts << {request: request, insert_id: insert_id_base}
          end
        end

        @last_flush = Time.now
      end

      def flush_pending_inserts(do_failures = false)
        while failing_request = @failing_inserts.shift
          if do_failures || Time.now <= failing_request[:next_try]
            @logger.info "[#{name}] Retrying #{failing_request[:insert_id]}"
            @pending_inserts_lock.synchronize { @pending_inserts.push(failing_request) }
          end
        end

        while request = @pending_inserts_lock.synchronize { @pending_inserts.shift }
          table = request[:request][:parameters]['tableId']
          result = @client.execute(request[:request])

          if result.error?
            if request[:retry]
              request[:retry] *= 1.8
            else
              request[:retry] = 5
            end

            request[:next_try] = Time.now + request[:retry]

            @logger.error "[#{name}] Failed #{table} to insert: #{result.error_message} (#{request[:insert_id]}); retrying in #{request[:retry]} seconds"
            @failing_inserts << request
          else
            @logger.debug "[#{name}] Inserted records in #{table}"
          end
        end
      end

    end
  end
end

