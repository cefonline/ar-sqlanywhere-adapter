# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module SQLAnywhere
      module DatabaseStatements
        BIND_LIMIT = 32767
        DEFAULT_AUTH = {
          "username" => "DBA",
          "password" => "sql123"
        }

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

        def create_database(name, options = {})
          options = options.symbolize_keys

          sql = "CREATE DATABASE '#{name}'"
          sql += " BLANK PADDING #{options[:blank_padding] ? "ON" : "OFF"}" if options[:blank_padding]
          sql += " CHECKSUM #{options[:checksum] ? "ON" : "OFF"}" if options[:checksum]
          sql += " COLLATION '#{options[:collation]}'" if options[:collation]
          sql += " JCONNECT #{options[:jconnect] ? "ON" : "OFF"}" if options[:jconnect]
          sql += " PAGE SIZE #{options[:page_size]}" if options[:page_size]
          sql += " NCHAR COLLATION '#{options[:ncollation]}'" if options[:ncollation]
          sql += " DBA USER '#{options.fetch(:dba_user, DEFAULT_AUTH["username"])}'"
          sql += " DBA PASSWORD '#{options.fetch(:dba_password, DEFAULT_AUTH["password"])}'"
          if options[:system_proc_as_definer]
            sql += " SYSTEM PROCEDURE AS DEFINER #{options[:system_proc_as_definer] ? "ON" : "OFF"}"
          end

          execute sql
        end

        def start_database(name)
          execute "START DATABASE '#{name}' AUTOSTOP OFF"
        rescue ActiveRecord::StatementInvalid => error
          raise unless error.is_a? ActiveRecord::NoDatabaseError
        end

        def stop_database(name)
          execute("STOP DATABASE #{name} UNCONDITIONALLY")
        rescue ActiveRecord::StatementInvalid => error
          raise unless error.is_a? ActiveRecord::NoDatabaseError
        end

        def drop_database(name)
          execute("DROP DATABASE '#{name}'")
        rescue ActiveRecord::StatementInvalid => error
          raise unless error.is_a? ActiveRecord::NoDatabaseError
        end

        def create_user(name, password)
          execute("CREATE USER #{name} IDENTIFIED BY #{password}")
        end

        def drop_user(name)
          execute("DROP USER #{name}")
        end

        def last_inserted_id(result)
          select("SELECT @@IDENTITY", "SCHEMA").first["@@IDENTITY"]
        end

        def current_isolation_level
          level = case select_value("SELECT CONNECTION_PROPERTY('isolation_level')")
                  when "0" then :read_uncommitted
                  when "1" then :read_committed
                  when "2" then :repeatable_read
                  when "3" then :serializable
          end

          transaction_isolation_levels.fetch(level)
        end

        def set_transaction_isolation_level(isolation_level)
          execute("SET TRANSACTION ISOLATION LEVEL #{isolation_level}")
        end

        def begin_db_transaction
          @auto_commit = false
          execute("BEGIN TRANSACTION")
        end

        def begin_isolated_db_transaction(isolation)
          @auto_commit = false
          set_transaction_isolation_level(transaction_isolation_levels.fetch(isolation))
          begin_db_transaction
        end

        def commit_db_transaction
          ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
            log("COMMIT", nil) { @connection.commit }
          end
        ensure
          @auto_commit = true
        end

        def exec_rollback_db_transaction
          ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
            log("ROLLBACK", nil) { @connection.rollback }
          end
        ensure
          @auto_commit = true
        end

        def disable_referential_integrity(&block)
          @auto_commit = false
          with_connection_property "wait_for_commit", "ON", &block
        ensure
          @auto_commit = true
        end

        def with_connection_property(property_name, property_value, &block)
          old = select_value("SELECT connection_property( '#{property_name}' )", "SCHEMA")

          begin
            execute("SET TEMPORARY OPTION #{property_name} = '#{property_value}'", "SCHEMA")
            result = yield
          ensure
            execute("SET TEMPORARY OPTION #{property_name} = '#{old}'", "SCHEMA")
            result
          end
        end

        def exec_query(sql, name = nil, binds = [], prepare = false)
          execute_stmt(sql, name, binds, cache_stmt: prepare) do |stmt, result|
            ActiveRecord::Result.new(result.columns.map(&:name), result.rows) if result
          end
        end

        def exec_delete(sql, name = nil, binds = [])
          execute_stmt(sql, name, binds) { |stmt, _| stmt.affected_rows }
        end
        alias :exec_update :exec_delete

        def execute(sql, name = nil)
          log(sql, name) do
            execute_stmt_with_binds(sql)
          end
        end

        def execute_stmt(sql, name, binds, cache_stmt: false, &block)
          type_casted_binds = type_casted_binds(binds)

          log(sql, name, binds, type_casted_binds) do
            execute_stmt_with_binds(sql, type_casted_binds, &block)
          end
        end

        def execute_stmt_with_binds(sql, type_casted_binds = [], &block)
          ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
            raise ActiveRecord::ActiveRecordError.new("Bind limit exceeded") if type_casted_binds.length > BIND_LIMIT

            stmt = @connection.prepare(sql)

            begin
              result = stmt.execute(*type_casted_binds)
            rescue SQLAnywhere2::Error => e
              stmt.close
              @connection.rollback if @auto_commit
              raise e
            end

            ret = yield stmt, result if block_given?
            stmt.close
            @connection.commit if @auto_commit
            ret
          end
        end
      end
    end
  end
end
