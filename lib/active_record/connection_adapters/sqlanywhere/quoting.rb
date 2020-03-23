# frozen_string_literal: true
module ActiveRecord
  module ConnectionAdapters
    module SQLAnywhere
      module Quoting # :nodoc:

        def self.quote_ident(ident)
          # Remove backslashes and double quotes from ident
          ident = ident.to_s.gsub(/\\|"/, '')
          %Q("#{ident}")
        end

        def quote_table_name_for_assignment(table, attr)
          quote_column_name(attr)
        end

        def quote_table_name table_name
          SQLAnywhere::Utils.extract_owner_qualified_name(table_name.to_s).quoted
        end

        # Applies quotations around column names in generated queries
        def quote_column_name(name) #:nodoc:
          Quoting.quote_ident name
        end

        def _quote(value, column = nil)
          case value
          when Type::Binary::Data then "'#{string_to_binary(value.to_s)}'"
          else super(value)
          end
        end

        def _type_cast(value)
          case value
          when Type::Binary::Data then string_to_binary(value.to_s)
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

        private

          # Handles the encoding of a binary object into SQL Anywhere
          # SQL Anywhere requires that binary values be encoded as \xHH, where HH is a hexadecimal number
          # This function encodes the binary string in this format
          def string_to_binary(value)
            value
            #"\\x" + value.unpack("H*")[0].scan(/../).join("\\x")
          end

          def binary_to_string(value)
            # This is causing issues when importing some documents including PDF docs
            # that have \\x46 in the document, the code below is replacing this with
            # the hex value of 46 which modifies the document content and makes it unreadable
            # and no longer useful. I'm not exactly sure why this is needed as I don't want my
            # binary data modified in any way
            #value.gsub(/\\x[0-9]{2}/) { |byte| byte[2..3].hex }
            value
          end

      end
    end
  end
end
