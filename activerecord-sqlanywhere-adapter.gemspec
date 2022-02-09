# frozen_string_literal: true

$:.push File.expand_path("../lib", __FILE__)

require "active_record/connection_adapters/sqlanywhere/version"

Gem::Specification.new do |spec|
  spec.name          = "activerecord-sqlanywhere-adapter"
  spec.version       = Activerecord::ConnectionAdapters::SQLAnywhere::VERSION
  spec.authors       = ["Unact"]
  spec.email         = ["it@unact.ru"]

  spec.summary       = "ActiveRecord driver for SQL Anywhere"
  spec.homepage      = "https://github.com/Unact/activerecord-sqlanywhere-adapter"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency "sqlanywhere2", ">= 0.0.7"
  spec.add_runtime_dependency "activerecord", ">= 6.0.0", "< 6.1"
  spec.required_ruby_version = ">= 2.0.0"
end
