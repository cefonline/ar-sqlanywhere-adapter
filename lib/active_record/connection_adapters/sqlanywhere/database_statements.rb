# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module SQLAnywhere
      module DatabaseStatements
        UTILITY_DB = 'utility_db'

        def utility_db?
          if @is_utility_db.nil?
            @is_utility_db = @connection_string.split(";").any? do |key_value|
              k, v = key_value.split("=")
              ["DatabaseName", "DBN"].include?(k) && [UTILITY_DB, "'#{UTILITY_DB}'", "\"#{UTILITY_DB}\""].include?(v)
            end
          end
          @is_utility_db
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

        def last_inserted_id(result)
          select('SELECT @@IDENTITY', 'SCHEMA').first["@@IDENTITY"]
        end

        def begin_db_transaction
          @auto_commit = false
          execute("BEGIN TRANSACTION")
        end

        def begin_isolated_db_transaction(isolation)
          @auto_commit = false
          execute("SET TRANSACTION ISOLATION LEVEL #{transaction_isolation_levels.fetch(isolation)}")
          begin_db_transaction
        end

        def commit_db_transaction
          execute("COMMIT")
        ensure
          @auto_commit = true
        end

        def exec_rollback_db_transaction
          execute("ROLLBACK")
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
          old = select_value("SELECT connection_property( '#{property_name}' )", 'SCHEMA')

          begin
            execute("SET TEMPORARY OPTION #{property_name} = '#{property_value}'", 'SCHEMA')
            result = yield
          ensure
            execute("SET TEMPORARY OPTION #{property_name} = '#{old}'", 'SCHEMA')
            result
          end
        end

        def exec_query sql, name = nil, binds = [], prepare = false
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

        def execute_immediate(sql)
          ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
            @connection.execute_immediate(sql)
          end
        end

        def execute_stmt_with_binds(sql, type_casted_binds = [], &block)
          ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
            stmt = @connection.prepare(sql)

            begin
              result = stmt.execute(*type_casted_binds)
            rescue SQLAnywhere2::Error => e
              stmt.close
              @connection.execute_immediate("ROLLBACK") if @auto_commit
              raise e
            end

            ret = yield stmt, result if block_given?
            stmt.close
            @connection.execute_immediate("COMMIT") if @auto_commit
            ret
          end
        end
      end
    end
  end
end
