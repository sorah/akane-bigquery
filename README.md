# akane-bigquery - Storage engine for akane.gem that streams tweets to Google BigQuery

Storage plugin gem for [akane](https://github.com/sorah/akane), allows you to use BigQuery as akane's storage engine.

## Installation

Add this line to your application's Gemfile:

    gem 'akane-bigquery'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install akane-bigquery

## Loading past data

If you're using akane's `file` storage for past data,`akane-bigquery prepare` allows you to load them into BigQuery.

```
$ mkdir /tmp/akane-bigquery
$ akane-bigquery prepare /path/to/your/file-storage /tmp/akane-bigquery
$ gsutil -m cp /tmp/akane-bigquery/* gs://YOUR_BUCKET/
$ bq load --source_format=NEWLINE_DELIMITED_JSON YOUR_DATASET_ID.tweets "$(gsutil ls gs://YOUR_BUCKET/ | ruby -e 'ARGF.readlines.map(&:chomp).reject(&:empty?).join(",").display')"
```

## Usage

TODO: Write usage instructions here

## Contributing

1. Fork it ( https://github.com/sorah/akane-bigquery/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
