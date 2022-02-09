# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module SQLAnywhere
      class SchemaDumper < ConnectionAdapters::SchemaDumper
        private

        def default_primary_key?(column)
          schema_type(column) == :integer && column.default_function == "AUTOINCREMENT"
        end
      end
    end
  end
end
