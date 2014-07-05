require 'akane/storages/bigquery'
require 'akane-bigquery/version'
require 'akane-bigquery/schema'

require 'google/api_client'

module AkaneBigquery
  def self.make_client(config)
    raise ArgumentError, "missing config['key']" unless config['key']
    raise ArgumentError, "missing config['key']['path']" unless config['key']['path']
    raise ArgumentError, "missing config['key']['passphrase']" unless config['key']['passphrase']
    raise ArgumentError, "missing config['client_id']" unless config['client_id']
    raise ArgumentError, "missing config['service_email']" unless config['service_email']

    client = Google::APIClient.new(
      application_name: config["app_name"] || 'akane',
      application_version: AkaneBigquery::VERSION,
    )

    key = Google::APIClient::KeyUtils.load_from_pkcs12(
      config['key']['path'],
      config['key']['passphrase']
    )

    client.authorization = Signet::OAuth2::Client.new(
      token_credential_uri: 'https://accounts.google.com/o/oauth2/token',
      audience: 'https://accounts.google.com/o/oauth2/token',
      scope: 'https://www.googleapis.com/auth/bigquery',
      issuer: config['service_email'],
      signing_key: key,
    )

    client.authorization.fetch_access_token!

    return client
  end

  def self.make_bigquery_client(config)
    client = make_client(config)
    [client, client.discovered_api("bigquery", "v2")]
  end
end
