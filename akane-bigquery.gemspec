# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'akane/bigquery/version'

Gem::Specification.new do |spec|
  spec.name          = "akane-bigquery"
  spec.version       = Akane::Bigquery::VERSION
  spec.authors       = ["Shota Fukumori (sora_h)"]
  spec.email         = ["her@sorah.jp"]
  spec.summary       = %q{akane.gem Google Bigquery storage adapter}
  spec.description   = %q{Google Bigquery storage adapter for akane.gem}
  spec.homepage      = "https://github.com/sorah/akane-bigquery"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "akane", ">= 0.1.0"
  spec.add_dependency 'google-api-client', '>= 0.7.1'

  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rspec", "~> 3.0.2"
  spec.add_development_dependency "webmock", "~> 1.17.3"
  spec.add_development_dependency "rake"
end
