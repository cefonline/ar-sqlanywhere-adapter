# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module SQLAnywhere
      class SchemaCreation < AbstractAdapter::SchemaCreation # :nodoc:
        private
          def action_sql(action, dependency)
            if dependency == :default
              "ON #{action} SET DEFAULT"
            else
              super
            end
          rescue ArgumentError => e
            raise ArgumentError, e.message.strip + ', :default'
          end
      end
    end
  end
end
