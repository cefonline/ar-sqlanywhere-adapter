module ActiveRecord
  module ConnectionAdapters
    class SQLAnywhereAdapter < AbstractAdapter
      module Quoting # :nodoc:
        # Cast a +value+ to a type that the database understands. For example,
        # SQLite does not understand dates, so this method will convert a Date
        # to a String.
        def type_cast(value, column)
          case value
          when true, false
            value ? 1 : 0
          else
            super(value, column)
          end
        end

        # Applies quotations around column names in generated queries
        def quote_column_name(name) #:nodoc:
          # Remove backslashes and double quotes from column names
          name = name.to_s.gsub(/\\|"/, '')
          %Q("#{name}")
        end

        def quote_table_name(table_name) #:nodoc:
          parts = table_name.split(".")

          parts.collect{ |part| quote_column_name(part) }.join(".")
        end

        # Handles special quoting of binary columns. Binary columns will be treated as strings inside of ActiveRecord.
        # ActiveRecord requires that any strings it inserts into databases must escape the backslash (\).
        # Since in the binary case, the (\x) is significant to SQL Anywhere, it cannot be escaped.
        def quote(value, column = nil)
          case value
          when String, ActiveSupport::Multibyte::Chars
            if column && column.type == :binary && column.class.respond_to?(:string_to_binary)
              "'#{column.class.string_to_binary(value.to_s)}'"
            else
               super(value, column)
            end
          when true, false
            value ? 1 : 0
          else
            super(value, column)
          end
        end

        def quoted_true
          '1'
        end

        def quoted_false
          '0'
        end

        protected
          # Handles the encoding of a binary object into SQL Anywhere
          # SQL Anywhere requires that binary values be encoded as \xHH, where HH is a hexadecimal number
          # This function encodes the binary string in this format
          def self.string_to_binary(value)
            "\\x" + value.unpack("H*")[0].scan(/../).join("\\x")
          end
      end
    end
  end
end
