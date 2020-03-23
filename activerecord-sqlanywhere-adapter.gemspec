# frozen_string_literal: true

$:.push File.expand_path("../lib", __FILE__)

require 'active_record/connection_adapters/sqlanywhere/version'

Gem::Specification.new do |spec|
  spec.name          = "activerecord-sqlanywhere-adapter"
  spec.version       = Activerecord::ConnectionAdapters::SQLAnywhere::VERSION
  spec.authors       = ["Eric Farar", "Unact"]
  spec.email         = ["eric.farrar@ianywhere.com", "it@unact.ru"]

  spec.summary       = "ActiveRecord driver for SQL Anywhere"
  spec.homepage      = "https://github.com/Unact/activerecord-sqlanywhere-adapter"
  spec.license       = "Apache License Version 2.0"

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency "sqlanywhere2"
  spec.add_runtime_dependency "activerecord", ">= 5.2.0"
  spec.required_ruby_version = ">= 2.0.0"
end
