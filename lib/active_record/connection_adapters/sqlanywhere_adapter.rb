#====================================================
#
#    Copyright 2008-2010 iAnywhere Solutions, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# See the License for the specific language governing permissions and
# limitations under the License.
#
# While not a requirement of the license, if you do modify this file, we
# would appreciate hearing about it.   Please email sqlany_interfaces@sybase.com
#
#
#====================================================

require 'active_record/connection_adapters/abstract_adapter'
require "active_record/connection_adapters/sqlanywhere/column"
require 'active_record/connection_adapters/sqlanywhere/quoting'
require "active_record/connection_adapters/sqlanywhere/schema_creation"
require "active_record/connection_adapters/sqlanywhere/schema_statements"
require "active_record/connection_adapters/sqlanywhere/schema_dumper"
require "active_record/connection_adapters/sqlanywhere/utils"
require 'arel/visitors/sqlanywhere.rb'

# Singleton class to hold a valid instance of the SQLAnywhereInterface across all connections
class SA
  include Singleton
  def api
    if @pid != Process.pid
      reset_api
    end
    @api
  end

  def initialize
    @api = nil
    @pid = nil
    require 'sqlanywhere' unless defined? SQLAnywhere
    reset_api
  end

  private
  def reset_api
    @pid = Process.pid
    if @api != nil
      @api.sqlany_fini()
      SQLAnywhere::API.sqlany_finalize_interface( @api )
    end
    @api = SQLAnywhere::SQLAnywhereInterface.new()
    raise LoadError, "Could not load SQLAnywhere DBCAPI library" if SQLAnywhere::API.sqlany_initialize_interface(@api) == 0
    raise LoadError, "Could not initialize SQLAnywhere DBCAPI library" if @api.sqlany_init() == 0
  end
end

module ActiveRecord
  class Base
    DEFAULT_CONFIG = { :username => 'dba', :password => 'sql' }
    CREATE_DB_CONFIG = %i(collation ncollation page_size jconnect checksum system_proc_as_definer blank_padding user_id)

    # Main connection function to SQL Anywhere
    # Connection Adapter takes four parameters:
    # * :database (required, no default). Corresponds to "DatabaseName=" in connection string
    # * :server (optional, defaults to :databse). Corresponds to "ServerName=" in connection string
    # * :username (optional, default to 'dba')
    # * :password (optional, deafult to 'sql')
    # * :encoding (optional, defaults to charset of OS)
    # * :commlinks (optional). Corresponds to "CommLinks=" in connection string
    # * :connection_name (optional). Corresponds to "ConnectionName=" in connection string

    def self.sqlanywhere_connection(config)

      if config[:connection_string]
        connection_string = config[:connection_string]
      else
        conn_config = DEFAULT_CONFIG.merge(config.dup)

        raise ArgumentError, "No database name was given. Please add a :database option." unless conn_config.has_key?(:database)

        connection_string  = "ServerName=#{(conn_config.delete(:server) || conn_config.delete(:database))};"
        connection_string += "DatabaseName=#{conn_config.delete(:database)};"
        connection_string += "UserID=#{conn_config.delete(:username)};"
        connection_string += "Password=#{conn_config.delete(:password)};"
        connection_string += "CommLinks=#{conn_config.delete(:commlinks)};" if config[:commlinks]
        connection_string += "ConnectionName=#{conn_config.delete(:connection_name)};" if config[:connection_name]
        connection_string += "CharSet=#{conn_config.delete(:encoding)};" if config[:encoding]

        # Since we are using default ConnectionPool class
        # and SqlAnywhere uses CPOOL variable for connection
        # we have to delete pool if it is available
        conn_config.delete(:pool)
        conn_config.delete(:adapter)
        conn_config.delete(:blocking_timeout)

        conn_config.except! *CREATE_DB_CONFIG

        # Then add all other connection settings
        conn_config.each_pair do |k, v|
          connection_string += "#{k}=#{v};"
        end
      end

      db = SA.instance.api.sqlany_new_connection()

      ConnectionAdapters::SQLAnywhereAdapter.new(db, logger, connection_string, config)
    end
  end

  module ConnectionAdapters
    class SQLAnywhereException < StandardError
      attr_reader :errno
      attr_reader :sql

      def initialize(message, errno, sql)
        super(message)
        @errno = errno
        @sql = sql
      end
    end

    class SQLAnywhereAdapter < AbstractAdapter
      include SQLAnywhere::Quoting
      include SQLAnywhere::SchemaStatements
      attr_reader :connection_string

      def utility_db?
        if @is_utility_db.nil?
          @is_utility_db = connection_string.split(";").any? do |key_value|
            k, v = key_value.split("=")
            ["DatabaseName", "DBN"].include?(k) && [UTILITY_DB, "'#{UTILITY_DB}'", "\"#{UTILITY_DB}\""].include?(v)
          end
        end
        @is_utility_db
      end

      SQLE_DATABASE_NOT_FOUND = -83
      ADAPTER_NAME = "SQLAnywhere".freeze
      UTILITY_DB = 'utility_db'.freeze
      SKIP_ERROR_CODES = [
        0,
        100 # При чтении ответа, когда следующей записи нет, не поднимаем ошибку
      ]

      def arel_visitor
        Arel::Visitors::SQLAnywhere.new self
      end

      def quote_table_name_for_assignment(table, attr)
        quote_column_name(attr)
      end

      def quote_table_name table_name
        SQLAnywhere::Utils.extract_owner_qualified_name(table_name.to_s).quoted.freeze
      end

      def initialize(connection, logger, connection_string, config)
        super(connection, logger, config)
        @auto_commit = true
        @affected_rows = 0
        @connection_string = connection_string
        connect!
      end

      def current_database
        select_value "SELECT DB_PROPERTY('Name')", "SCHEMA"
      end

      def collation
        select_value "SELECT DB_EXTENDED_PROPERTY('Collation', 'Specification')", "SCHEMA"
      end

      def charset
        select_value "SELECT DB_PROPERTY('CharSet')", "SCHEMA"
      end

      def ncollation
        select_value "SELECT DB_EXTENDED_PROPERTY('NcharCollation', 'Specification')", "SCHEMA"
      end

      def ncharset
        select_value "SELECT DB_PROPERTY('CharSet')", "SCHEMA"
      end

      def explain(arel, binds = [])
        raise NotImplementedError
      end

      def truncate_table(table_name)
        execute "TRUNCATE TABLE #{quote_table_name table_name}"
      end

      def start_database dbf, dbname
        execute "START DATABASE '#{dbf}' AS #{dbname} AUTOSTOP OFF"
      end

      def stop_database database
        execute("STOP DATABASE #{database} UNCONDITIONALLY")
      rescue ActiveRecord::StatementInvalid => error # Если БД нет, то все ОК
        raise unless error.is_a? ActiveRecord::NoDatabaseError
      end

      def drop_database dbf
        execute("DROP DATABASE '#{dbf}'")
      rescue ActiveRecord::StatementInvalid => error # Если БД нет, то все ОК
        raise unless error.is_a? ActiveRecord::NoDatabaseError
      end

      # id нужен для того, что бы созадть пользователя под тем же идентификатором, под котором он заведен в продакшен
      # это позволит в дальнейшем накатить дамп
      def create_admin_user username, password, id=nil
        at_part = id.present? ? "AT #{id}" : ""
        execute "GRANT CONNECT TO \"#{username}\" #{at_part} IDENTIFIED BY '#{password}';"
        execute "GRANT ROLE \"SYS_AUTH_DBA_ROLE\" TO \"#{username}\"  WITH ADMIN OPTION WITH NO SYSTEM PRIVILEGE INHERITANCE;"
      end

      # Не работает, надо названия таблиц доработать
      def supports_migrations? #:nodoc:
        false
      end

      def requires_reloading?
        true
      end

      def active?
        # The liveness variable is used a low-cost "no-op" to test liveness
        SA.instance.api.sqlany_execute_immediate(@connection, "SET liveness = 1") == 1
      rescue
        false
      end

      def disconnect!
        SA.instance.api.sqlany_disconnect( @connection )
        super
      end

      def reconnect!
        disconnect!
        connect!
      end

      def supports_count_distinct? #:nodoc:
        true
      end

      def supports_autoincrement? #:nodoc:
        true
      end

      def supports_foreign_keys?
        true
      end

      def execute sql, name = nil
        log(sql, name) do
          _execute sql, name
        end
      end

      # The database execution function
      def _execute(sql, name)
        begin
          stmt = SA.instance.api.sqlany_prepare(@connection, sql)
          sqlanywhere_error_test(sql) if stmt==nil
          r = SA.instance.api.sqlany_execute(stmt)
          sqlanywhere_error_test(sql) if r==0
          @affected_rows = SA.instance.api.sqlany_affected_rows(stmt)
          sqlanywhere_error_test(sql) if @affected_rows==-1
          SA.instance.api.sqlany_free_stmt(stmt)
          SA.instance.api.sqlany_commit(@connection) if @auto_commit
        rescue StandardError => e
          @affected_rows = 0
          SA.instance.api.sqlany_free_stmt(stmt)
          SA.instance.api.sqlany_rollback @connection
          raise e
        end
        @affected_rows
      end

      def sqlanywhere_error_test(sql = '')
        error_code, error_message = SA.instance.api.sqlany_error(@connection)
        sqlanywhere_error(error_code, encode_sql_value(error_message), sql) unless SKIP_ERROR_CODES.include? error_code
      end

      def sqlanywhere_error(code, message, sql)
        raise SQLAnywhereException.new(message, code, sql)
      end

      def translate_exception(exception, message)
        encoded_msg = encode_sql_value(message.dup)
        return super unless exception.respond_to?(:errno)
        case exception.errno
          when -143
            if exception.sql !~ /^SELECT/i then
              raise ActiveRecord::ActiveRecordError.new encoded_msg
            else
              super
            end
          when -194
            raise ActiveRecord::InvalidForeignKey.new encoded_msg
          when -196
            raise ActiveRecord::RecordNotUnique.new encoded_msg
          when SQLE_DATABASE_NOT_FOUND
            raise ActiveRecord::NoDatabaseError.new encoded_msg
          when -183
            raise ArgumentError, encoded_msg
          else
            super
        end
      end

      def last_inserted_id(result)
        select('SELECT @@IDENTITY', 'SCHEMA').first["@@IDENTITY"]
      end

      def begin_db_transaction #:nodoc:
        @auto_commit = false
      end

      def commit_db_transaction #:nodoc:
        SA.instance.api.sqlany_commit(@connection)
        @auto_commit = true
      end

      def rollback_db_transaction #:nodoc:
        SA.instance.api.sqlany_rollback(@connection)
        @auto_commit = true
      end

      def disable_referential_integrity(&block) #:nodoc:
        with_connection_property "wait_for_commit", "ON", &block
      end

      def with_connection_property(property_name, property_value, &block)
        old = select_value("SELECT connection_property( '#{property_name}' )", 'SCHEMA')

        begin
          update("SET TEMPORARY OPTION #{property_name} = '#{property_value}'", 'SCHEMA')
          result = yield
        ensure
          update("SET TEMPORARY OPTION #{property_name} = '#{old}'", 'SCHEMA')
          result
        end
      end

      # Adjust the order of offset & limit as SQLA requires
      # TOP & START AT to be at the start of the statement not the end
      def combine_bind_parameters(
        from_clause: [],
        join_clause: [],
        where_clause: [],
        having_clause: [],
        limit: nil,
        offset: nil
      ) # :nodoc:
        result = []
        result << limit if limit
        if offset
          # Can't see a better way of doing this, we need to add 1 to the offset value
          # as SQLA uses START AT, see active_record model query_methods.rb bound_attributes method
          offset_bind = Attribute.with_cast_value(
            "OFFSET".freeze,
            offset.value.to_i+1, # SQLA START AT = OFFSET + 1
            Type::Value.new,
          )
          result << offset_bind
        end
        result = result + from_clause + join_clause + where_clause + having_clause
        result
      end





      protected

      # === Abstract Adapter (Misc Support) =========================== #

        def extract_limit(sql_type)
          case sql_type
            when /^tinyint/i
              1
            when /^smallint/i
              2
            when /^integer/i
              4
            when /^bigint/i
              8
            else super
          end
        end

        def initialize_type_map(m) # :nodoc:
          m.register_type %r(boolean)i,         Type::Boolean.new
          m.alias_type    %r(tinyint)i,         'boolean'
          m.alias_type    %r(bit)i,             'boolean'

          m.register_type %r(char)i,            Type::String.new
          m.alias_type    %r(varchar)i,         'char'
          m.alias_type    %r(varbit)i,          'char'
          m.alias_type    %r(xml)i,             'char'

          m.register_type %r(binary)i,            Type::Binary.new
          m.alias_type    %r(long binary)i,       'binary'
          m.alias_type    %r(uniqueidentifier)i,  'binary'

          m.register_type %r(text)i,            Type::Text.new
          m.alias_type    %r(long varchar)i,    'text'

          m.register_type %r(date)i,              Type::Date.new
          m.register_type %r(time)i,              Type::Time.new
          m.register_type %r(timestamp)i,         Type::DateTime.new
          m.register_type %r(datetime)i,          Type::DateTime.new

          m.register_type %r(int)i,               Type::Integer.new
          m.register_type %r(smallint)i,          Type::Integer.new(limit: 2)
          m.register_type %r(^bigint)i,           Type::Integer.new(limit: 8)

          #register_class_with_limit m, %r(tinyint)i,          Type::Boolean
          #register_class_with_limit m, %r(bit)i,              Type::Boolean
          #register_class_with_limit m, %r(long varchar)i,     Type::Text
          #register_class_with_limit m, %r(varchar)i,          Type::String
          #register_class_with_limit m, %r(timestamp)i,        Type::DateTime
          #register_class_with_limit m, %r(smallint|bigint)i,  Type::Integer
          #register_class_with_limit m, %r(xml)i,              Type::String
          #register_class_with_limit m, %r(uniqueidentifier)i, Type::Integer
          #register_class_with_limit m, %r(long binary)i,      Type::Binary

          super
        end

        def select(sql, name = nil, binds = []) #:nodoc:
           exec_query(sql, name, binds)
        end

        # Queries the structure of a table including the columns names, defaults, type, and nullability
        # ActiveRecord uses the type to parse scale and precision information out of the types. As a result,
        # chars, varchars, binary, nchars, nvarchars must all be returned in the form <i>type</i>(<i>width</i>)
        # numeric and decimal must be returned in the form <i>type</i>(<i>width</i>, <i>scale</i>)
        # Nullability is returned as 0 (no nulls allowed) or 1 (nulls allowed)
        # Also, ActiveRecord expects an autoincrement column to have default value of NULL
        # Owner support
        def table_structure(table_name)
          owner, name = extract_owner_qualified_name table_name

          sql = <<-SQL
            SELECT
              SYS.SYSCOLUMN.column_name AS name,
              if left("default",1)='''' then
                substring("default", 2, length("default")-2)
              else
                SYS.SYSCOLUMN."default"
              endif AS "default",
              IF SYS.SYSCOLUMN.domain_id IN (7,8,9,11,33,34,35,3,27) THEN
                IF SYS.SYSCOLUMN.domain_id IN (3,27) THEN
                  SYS.SYSDOMAIN.domain_name || '(' || SYS.SYSCOLUMN.width || ',' || SYS.SYSCOLUMN.scale || ')'
                ELSE
                  SYS.SYSDOMAIN.domain_name || '(' || SYS.SYSCOLUMN.width || ')'
                ENDIF
              ELSE
                SYS.SYSDOMAIN.domain_name
              ENDIF AS domain,
              IF SYS.SYSCOLUMN.nulls = 'Y' THEN 1 ELSE 0 ENDIF AS nulls
            FROM
              SYS.SYSCOLUMN
            INNER JOIN SYS.SYSTABLE ON SYS.SYSCOLUMN.table_id = SYS.SYSTABLE.table_id
            INNER JOIN  SYS.SYSDOMAIN ON SYS.SYSCOLUMN.domain_id = SYS.SYSDOMAIN.domain_id
            WHERE
              table_name = '#{name}'
            AND SYS.SYSTABLE.creator = (
              SELECT user_id FROM SYS.SYSUSER WHERE SYS.SYSUSER.user_name = '#{owner}'
            )
          SQL
          structure = exec_query(sql, "SCHEMA").to_hash

          structure.map do |column|
            if String === column["default"]
              # Escape the hexadecimal characters.
              # For example, a column default with a new line might look like 'foo\x0Abar'.
              # After the gsub it will look like 'foo\nbar'.
              column["default"].gsub!(/\\x(\h{2})/) {$1.hex.chr}
            end
            column
          end
          raise(ActiveRecord::StatementInvalid, "Could not find table '#{table_name}'") if structure == false
          structure
        end
        alias column_definitions table_structure

      private

        def extract_owner_qualified_name(string)
          name = SQLAnywhere::Utils.extract_owner_qualified_name(string.to_s)
          [name.owner, name.identifier]
        end

        def connect!
          result = SA.instance.api.sqlany_connect(@connection, @connection_string)
          if result == 1 then
            set_connection_options
          else
            error = SA.instance.api.sqlany_error(@connection)
            if error.first == SQLE_DATABASE_NOT_FOUND
              raise ActiveRecord::NoDatabaseError
            else
              raise ActiveRecord::ActiveRecordError.new("#{error}: Cannot Establish Connection")
            end
          end
        end

        def set_connection_options
          SA.instance.api.sqlany_execute_immediate(@connection, "SET TEMPORARY OPTION non_keywords = 'LOGIN'")
          SA.instance.api.sqlany_execute_immediate(@connection, "SET TEMPORARY OPTION timestamp_format = 'YYYY-MM-DD HH:NN:SS'")
          #SA.instance.api.sqlany_execute_immediate(@connection, "SET OPTION reserved_keywords = 'LIMIT'")
          # The liveness variable is used a low-cost "no-op" to test liveness
          SA.instance.api.sqlany_execute_immediate(@connection, "CREATE VARIABLE liveness INT")
          if @config[:blocking_timeout]
            SA.instance.api.sqlany_execute_immediate(
              @connection,
              "SET TEMPORARY OPTION blocking_timeout = #{@config[:blocking_timeout]}"
            )
          end
        rescue nil
        end

      def exec_query sql, name = nil, binds = [], prepare = false
        log(sql, name, binds, type_casted_binds(binds)) do
          _exec_query sql, name, binds, prepare
        end
      end

      def _exec_query(sql, name, binds, prepare)
        stmt = SA.instance.api.sqlany_prepare(@connection, sql)
        sqlanywhere_error_test(sql) if stmt==nil

        begin
          binds.each_with_index do |bind, i|
            bind_value = type_cast(bind.value_for_database)
            result, bind_param = SA.instance.api.sqlany_describe_bind_param(stmt, i)
            sqlanywhere_error_test(sql) if result==0
            bind_param.set_direction(1) # https://github.com/sqlanywhere/sqlanywhere/blob/master/ext/sacapi.h#L175
            if bind_value.nil?
              bind_param.set_value(nil)
            else
              bind_param.set_value(bind_value)
            end
            # Just could not get binary data to work as a bind parameter even when using the correct format
            # by escaping the data as per comments in quoting.rb:
            # SQL Anywhere requires that binary values be encoded as \xHH, where HH is a hexadecimal number
            # It ALWAYS treated the value as a string no matter what I tried. I added -zr all -zo reqlog.txt
            # option to my server to log exactly what SQLA was processing. I think the reason is that the set_value
            # method receives the value as a string and then sets the type as a string so SQLA then treats whatever
            # we pass in as a string.
            #
            # To fix this problem I've had to modify a fork of sqlanywhere gem working on branch ruby22. I modified
            # the code to allow me to get/set the bind param. This allowed me to override the type set by set_value
            # before I bound the param to the statement, see code below. Setting the type means we pass in the binary
            # data as is without encoding the value at all (see quoting.rb)
            #
            # The problem with this approach is that it binds this gem to a particular branch of sqlanywhere.
            #
            if bind.value_for_database.class == Type::Binary::Data
              bind_param.set_type(1)
            end
            result = SA.instance.api.sqlany_bind_param(stmt, i, bind_param)
            sqlanywhere_error_test(sql) if result==0
          end

          if SA.instance.api.sqlany_execute(stmt) == 0
            sqlanywhere_error_test(sql)
          end

          fields = []
          native_types = []

          num_cols = SA.instance.api.sqlany_num_cols(stmt)
          sqlanywhere_error_test(sql) if num_cols == -1

          for i in 0...num_cols
            result, col_num, name, ruby_type, native_type, precision, scale, max_size, nullable = SA.instance.api.sqlany_get_column_info(stmt, i)
            sqlanywhere_error_test(sql) if result==0
            fields << name
            native_types << native_type
          end
          rows = []

          loop do
            next_res = SA.instance.api.sqlany_fetch_next(stmt)

            if next_res == 0
              sqlanywhere_error_test(sql)
              break
            end

            row = []
            for i in 0...num_cols
              r, value = SA.instance.api.sqlany_get_column(stmt, i)
              row << native_type_to_ruby_type(native_types[i], value)
            end
            rows << row
          end
          @affected_rows = SA.instance.api.sqlany_affected_rows(stmt)
          sqlanywhere_error_test(sql) if @affected_rows==-1
          SA.instance.api.sqlany_free_stmt(stmt)
          SA.instance.api.sqlany_commit(@connection) if @auto_commit
        rescue StandardError => e
          @affected_rows = 0
          SA.instance.api.sqlany_free_stmt(stmt)
          SA.instance.api.sqlany_rollback @connection
          raise e
        end

        ActiveRecord::Result.new(fields, rows)
      end

      def exec_delete(sql, name = 'SQL', binds = [])
        exec_query(sql, name, binds)
        @affected_rows
      end
      alias :exec_update :exec_delete

      # convert sqlany type to ruby type
      # the types are taken from here
      # http://dcx.sybase.com/1101/en/dbprogramming_en11/pg-c-api-native-type-enum.html
      def native_type_to_ruby_type(native_type, value)
        return nil if value.nil?
        case native_type
        when 484 # DT_DECIMAL (also and more importantly numeric)
          BigDecimal(value)
        when 448,452,456,460,640  # DT_VARCHAR, DT_FIXCHAR, DT_LONGVARCHAR, DT_STRING, DT_LONGNVARCHAR
          encode_sql_value(value)
        else
          value
        end
      end

      def encode_sql_value value
        value.
          force_encoding(ActiveRecord::Base.connection_config[:CharSet]).
          encode('UTF-8', invalid: :replace, undef: :replace)
      end
    end
  end
end
