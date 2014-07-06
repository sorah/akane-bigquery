require 'akane-bigquery'
require 'yaml'
require 'thor'
require 'oj'

module AkaneBigquery
  class CLI < Thor
    class_option :config,
      required: true, aliases: %w(-c),
      desc: "path to akane config file (yml)"
    class_option :config_name,
      desc: "select bigquery configuration by name key. use this if you have multiple bigquery storages in config file"

    desc "init", 'creates table on bigquery'
    def init
      # check dataset existence
      dataset =  client.execute(
        api_method: api.datasets.get,
        parameters: {
          'projectId' => config['project_id'],
          'datasetId' => config['dataset_id'],
        }
      )

      if dataset.error?
        if dataset.error_message =~ /^Not Found:/i
          puts "Creating dataset #{config['dataset_id']} ..."
          dataset = client.execute(
            api_method: api.datasets.insert,
            parameters: {
              'projectId' => config['project_id'],
            },
            body_object: {
              'datasetReference' => {
                'datasetId' => config['dataset_id'],
              },
              'description' => 'akane',
            }
          )

          raise dataset.error_message if dataset.error?
        else
          raise dataset.error_message
        end
      end

      schemas = AkaneBigquery::Schema::SCHEMA

      schemas.each do |table_id, schema|
        table = client.execute(
          api_method: api.tables.get,
          parameters: {
            'projectId' => config['project_id'],
            'datasetId' => config['dataset_id'],
            'tableId' => table_id,
          },
        )

        if table.error?
          if table.error_message =~ /^Not Found:/i
            puts "Creating table #{table_id} ..."
            table = client.execute(
              api_method: api.tables.insert,
              parameters: {
                'projectId' => config['project_id'],
                'datasetId' => config['dataset_id'],
              },
              body_object: {
                'tableReference' => {
                  'projectId' => config['project_id'],
                  'datasetId' => config['dataset_id'],
                  'tableId' => table_id,
                },
                'friendlyName' => table_id,
                'schema' => schema,
              }
            )
            raise table.error_message if table.error?
          else
            raise table.error_message
          end
        end

      end
    end

    desc "prepare SOURCE DEST", "prepare JSONs or loading into BigQuery from existing file storage data"
    method_option :months, desc: "Names of months to process. Separeted by comma."
    method_option :before, desc: "Dump only data before specified datetime. Value will be parsed by `Time.parse` of Ruby."
    def prepare(source, prefix)
      limit = 524288000 # 500MBytes

      count = -1
      bytes = 0

      new_io = lambda do
        bytes = 0
        count += 1
        path = File.join(prefix, "tweets.#{count.to_s.rjust(4,'0')}.txt")
        puts "=> Using #{path}"
        File.open(path, 'w')
      end
      io = new_io.call

      months = options[:months] && options[:months].split(/,/)
      before = options[:before] && Time.parse(options[:before])

      userdirs = Dir.entries(File.join(source, "users"))
      userdirs.each_with_index do |user_dirname, index|
        next if user_dirname == "." || user_dirname == ".."
        puts " * #{user_dirname} (#{index.succ}/#{userdirs.size}, #{((index.succ/userdirs.size.to_f)*100).to_i}%)"

        userdir = File.join(source, "users", user_dirname)

        tweet_filepaths = if options[:months]
                            months.map { |_| File.join(userdir, "tweets.#{_}.txt") }
                          else
                            Dir[File.join(userdir, 'tweets.*.txt')]
                          end
        tweet_filepaths.each do |file|
          begin
            File.open(file, 'r') do |tweets_io|
              tweets_io.each_line do |line|
                json = line.chomp

                tweet  = Oj.load(json)

                created_at = Time.parse(tweet['created_at'.freeze])
                next if before && before <= created_at

                new_json = {
                  'json'.freeze => json,
                  'id_str'.freeze => tweet['id_str'.freeze],
                  'id'.freeze => tweet['id'.freeze],
                  'text'.freeze => tweet['text'.freeze],
                  'lang'.freeze => tweet['lang'.freeze],
                  'source'.freeze => tweet['source'.freeze],
                  'in_reply_to_status_id'.freeze => tweet['in_reply_to_status_id'.freeze],
                  'in_reply_to_status_id_str'.freeze => tweet['in_reply_to_status_id_str'.freeze],
                  'in_reply_to_user_id'.freeze => tweet['in_reply_to_user_id'.freeze],
                  'in_reply_to_user_id_str'.freeze => tweet['in_reply_to_user_id_str'.freeze],
                  'in_reply_to_screen_name'.freeze => tweet['in_reply_to_screen_name'.freeze],
                  'user'.freeze => {
                    'id_str'.freeze => tweet['user'.freeze]['id_str'.freeze],
                    'id'.freeze => tweet['user'.freeze]['id'.freeze],
                    'name'.freeze => tweet['user'.freeze]['name'.freeze],
                    'screen_name'.freeze => tweet['user'.freeze]['screen_name'.freeze],
                    'protected'.freeze => tweet['user'.freeze]['protected'.freeze],
                  },
                  'created_at'.freeze => created_at.to_i
                }

                if tweet['coordinates'.freeze]
                  new_json['coordinates_longitude'.freeze] = tweet['coordinates'.freeze]['coordinates'.freeze][0]
                  new_json['coordinates_latitude'.freeze] = tweet['coordinates'.freeze]['coordinates'.freeze][1]
                end

                if tweet['place'.freeze]
                  place = tweet['place'.freeze]
                  new_json['place'.freeze] = {
                    'id'.freeze => place['id'.freeze],
                    'country'.freeze => place['country'.freeze],
                    'country_code'.freeze => place['country_code'.freeze],
                    'name'.freeze => place['name'.freeze],
                    'full_name'.freeze => place['full_name'.freeze],
                    'place_type'.freeze => place['place_type'.freeze],
                    'url'.freeze => place['url'.freeze],
                  }
                end

                new_json_str = Oj.dump(new_json)
                io.puts new_json_str
                bytes += new_json_str.size + 1
                io = new_io.call if limit <= bytes
              end
            end
          rescue Errno::ENOENT
          end

        end

      end
    end

    private

    def config
      @config ||= begin
        storages = YAML.load_file(options[:config])['storages']

        conf = if options[:config_name]
          storages.find { |_| _['bigquery'] && _['bigquery']['name'] == options[:config_name] }
        else
          storages.find { |_| _['bigquery'] }
        end

        (conf && conf['bigquery']) or \
          abort 'error: bigquery storage configuration not found'
      end
    end

    def client
      client_and_api; @client
    end

    def api
      client_and_api; @api
    end

    def client_and_api
      return @client_and_api if @client_and_api

      @client_and_api = AkaneBigquery.make_bigquery_client(config) 
      @client, @api = @client_and_api
    end
  end
end
