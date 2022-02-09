# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module SQLAnywhere
      module Quoting
        def self.quote_ident(ident)
          # Remove backslashes and double quotes from ident
          ident = ident.to_s.gsub(/\\|"/, "")
          %Q("#{ident}")
        end

        def quote_table_name_for_assignment(table, attr)
          quote_column_name(attr)
        end

        def quote_table_name(table_name)
          SQLAnywhere::Utils.extract_owner_qualified_name(table_name.to_s).quoted
        end

        # Applies quotations around column names in generated queries
        def quote_column_name(name)
          Quoting.quote_ident name
        end

        def _quote(value, column = nil)
          case value
          # We might receive values with wrong encoding. Convert them to correct encoding.
          # dup the value since it might be Frozen
          when String, ActiveSupport::Multibyte::Chars then super(value).dup.force_encoding(@connection.encoding)
          when Type::Binary::Data then "'#{value}'"
          # This by default returns a value with ASCII_8BIT encoding which is a binary type in SQLAnywhere2
          # So we convert it to correct connection type
          when BigDecimal then super(value).force_encoding(@connection.encoding)
          else super(value)
          end
        end

        def _type_cast(value)
          case value
          # We might receive values with wrong encoding. Convert them to correct encoding
          # dup the value since it might be Frozen
          when String, ActiveSupport::Multibyte::Chars then super(value).dup.force_encoding(@connection.encoding)
          when Type::Binary::Data then value.to_s
          # This by default returns a value with ASCII_8BIT encoding which is a binary type in SQLAnywhere2
          # So we convert it to correct connection type
          when BigDecimal then super(value).force_encoding(@connection.encoding)
          else super(value)
          end
        end

        def quoted_false
          unquoted_false.to_s
        end

        def unquoted_false
          0
        end

        def quoted_true
          unquoted_true.to_s
        end

        def unquoted_true
          1
        end
      end
    end
  end
end
