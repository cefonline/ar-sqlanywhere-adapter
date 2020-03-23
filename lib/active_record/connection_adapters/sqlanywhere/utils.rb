# frozen_string_literal: true
# based on "active_record/connection_adapters/postgresql/utils"

module ActiveRecord
  module ConnectionAdapters
    module SQLAnywhere
      # Value Object to hold a owner qualified name.
      # This is usually the name of a SQLAnywhere relation but it can also represent
      # owner qualified type names. +onwer+ and +identifier+ are unquoted to prevent
      # double quoting.
      class Name # :nodoc:
        SEPARATOR = "."
        attr_reader :owner, :identifier

        def initialize(owner, identifier)
          @owner, @identifier = unquote(owner), unquote(identifier)
        end

        def to_s
          parts.join SEPARATOR
        end

        def quoted
          if owner
            SQLAnywhere::Quoting.quote_ident(owner) + SEPARATOR + SQLAnywhere::Quoting.quote_ident(identifier)
          else
            SQLAnywhere::Quoting.quote_ident(identifier)
          end
        end

        def ==(o)
          o.class == self.class && o.parts == parts
        end
        alias_method :eql?, :==

        def hash
          parts.hash
        end

        protected

          def parts
            @parts ||= [@owner, @identifier].compact
          end

        private
          def unquote(part)
            if part && part.start_with?('"')
              part[1..-2]
            else
              part
            end
          end
      end

      module Utils # :nodoc:
        extend self

        # Returns an instance of <tt>ActiveRecord::ConnectionAdapters::SQLAnywhere::Name</tt>
        # extracted from +string+.
        # +owner+ is +nil+ if not specified in +string+.
        # +owner+ and +identifier+ exclude surrounding quotes (regardless of whether provided in +string+)
        # +string+ supports the range of owner/table references understood by SQLAnywhere, for example:
        #
        # * <tt>table_name</tt>
        # * <tt>"table.name"</tt>
        # * <tt>owner.table_name</tt>
        # * <tt>owner."table.name"</tt>
        # * <tt>"owner".table_name</tt>
        # * <tt>"ow.ner"."table name"</tt>
        def extract_owner_qualified_name(string)
          owner, table = string.scan(/[^".\s]+|"[^"]*"/)
          if table.nil?
            table = owner
            owner = nil
          end
          SQLAnywhere::Name.new(owner, table)
        end
      end
    end
  end
end
