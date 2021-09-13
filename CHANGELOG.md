## 6.0.2

- Add support for SQLAnywhere 17 structure unload

## 6.0.1

- Add support for SQLAnywhere 17 structure load

## 6.0.0

- Change gem major version to match rails major version
- Migrate to rails 6

## 3.0.9

- Fix global temprorary tables being excluded from `data_source` tables

## 3.0.8

- Fix ruby VM crash when a prepared statement has a large number of parameters

## 3.0.7

- Fix database setup rake tasks
- Add create_database, start_database, create_user, drop_user methods
- Remove `asa` rake tasks

## 3.0.6

- Fix transaction isolation not returning previous isolation level
- Fix COMMIT log name

## 3.0.5

- Fix SQLAnywhere error code to ActiveRecord errors mapping

## 3.0.4

- Fix compatibility issue with SQLAnywhere 12
- Add sqlanywhere_version method

## 3.0.3

- Remove `execute_immediate` method
- Change COMMIT and ROLLBACK commands to instead use commit/rollback methods from SQLAnywhere2

## 3.0.2

- Fix Binary not inserting correctly

## 3.0.1

- Fix BigDecimal not inserting correctly

## 3.0.0

- Migrate to SQLAnywhere2 gem
- Cleanup project structure
- Add frozen_string_literal magic comment

## 0.2.0

- Added support for Rails 3.0.3
- Added support for Arel 2
- Removed test instructions for ActiveRecord 2.2.2
- Updated license to 2010

## 0.1.3

- Added :encoding option to connection string
- Fixed bug associated with dangling connections in development mode (http://groups.google.com/group/sql-anywhere-web-development/browse_thread/thread/79fa81bdfcf84c13/e29074e5b8b7ad6a?lnk=gst&q=activerecord#e29074e5b8b7ad6a)

## 0.1.2

- Fixed bug in ActiveRecord::ConnectionAdapters::SQLAnywhereAdapter#table_structure SQL (Paul Smith)
- Added options for :commlinks and :connection_name to database.yml configuration (Paul Smith)
- Fixed ActiveRecord::ConnectionAdapters::SQLAnywhereColumn.string_to_binary and binary_to_string  (Paul Smith)
- Added :time as a native datatype  (Paul Smith)
- Override SQLAnywhereAdapter#active? to prevent stale connections  (Paul Smith)
- 'Fixed' coding style to match Rails standards  (Paul Smith)
- Added temporary option for timestamp_format
- Fixed bug to let migrations drop columns with indexes
- Formatted code
- Fixed bug to raise proper exceptions when a query with a bad column in executed

## 0.1.1

- Changed file permissions on archives
- Changed archives to be specific to platform (.zip on windows, .tar.gz
otherwise)
- Removed the default rake task

## 0.1.0

- Initial Release
