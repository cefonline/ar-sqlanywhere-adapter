# frozen_string_literal: true

source "https://rubygems.org"

git_source(:github) { |repo| "https://github.com/#{repo}.git" }

gemspec

group :development do
  gem "rubocop", require: false
  gem "rubocop-performance", require: false

  gem "sqlanywhere2", github: "Unact/sqlanywhere2", branch: "master"

  gem "byebug"
end
