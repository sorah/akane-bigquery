require 'akane-bigquery'
require 'yaml'
require 'thor'

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

    desc "prepare SOURCE PREFIX", "prepare JSONs on Cloud Storage for loading into BigQuery from existing file storage data"
    def prepare(source, prefix)

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
