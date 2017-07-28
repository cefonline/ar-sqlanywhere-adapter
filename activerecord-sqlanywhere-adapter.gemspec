Gem::Specification.new do |s|
  s.name = %q{activerecord-sqlanywhere-adapter}
  s.version = "1.1.0"

  s.authors = [%q{Eric Farar}]
  s.description = %q{ActiveRecord driver for SQL Anywhere}
  s.email = %q{eric.farrar@ianywhere.com}
  s.files = [
    "CHANGELOG",
    "LICENSE",
    "README.md",
    "Rakefile",
    "test/connection.rb",
    "lib/active_record/connection_adapters/sqlanywhere_adapter.rb",
    "lib/arel/visitors/sqlanywhere.rb",
    "lib/active_record/connection_adapters/sqlanywhere.rake",
    "lib/activerecord-sqlanywhere-adapter.rb",
    "lib/active_record/connection_adapters/sqlanywhere_adapter/utils.rb"
  ]
  s.homepage = %q{http://sqlanywhere.rubyforge.org}
  s.licenses = [%q{Apache License Version 2.0}]
  s.require_paths = [%q{lib}]
  s.rubygems_version = %q{>= 2.1.0}
  s.summary = %q{ActiveRecord driver for SQL Anywhere}

  s.add_dependency(%q<sqlanywhere>, [">= 0.1.5"])
  s.add_dependency(%q<activerecord>, [">= 4.0", "< 4.2"])
end
