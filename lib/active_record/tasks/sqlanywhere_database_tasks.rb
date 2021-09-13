# frozen_string_literal: true

require "active_record/tasks/database_tasks"

# https://github.com/rails/rails/blob/5-2-stable/activerecord/lib/active_record/tasks/postgresql_database_tasks.rb
module ActiveRecord
  module Tasks # :nodoc:
    class SQLAnywhereDatabaseTasks # :nodoc:
      delegate :connection, :establish_connection, :clear_active_connections!, to: ActiveRecord::Base

      def initialize(configuration)
        @configuration = configuration
      end

      def create
        establish_connection configuration_without_database
        connection.create_database(configuration['database'], configuration)
        connection.start_database(configuration['database'])
        establish_connection configuration_as_dba
        connection.create_user(configuration['username'], configuration['password'])
        establish_connection configuration
      rescue ActiveRecord::StatementInvalid => error
        if error.message.include?("already exists")
          raise DatabaseAlreadyExists
        else
          raise
        end
      end

      def drop
        establish_connection configuration_without_database
        connection.stop_database configuration["database"]
        connection.drop_database configuration["database"]
      end

      def purge
        drop
        create
      end

      def collation
        connection.collation
      end

      def charset
        connection.charset
      end

      def structure_dump filename, extra_flags
        args = ["-c", connection.connection_string, "-y", "-q", "-n", "-r", filename]
        args.push("-up") if need_option_for_password_unload?
        args.concat(Array(extra_flags)) if extra_flags

        Kernel.system("dbunload", *args)
      end

      def structure_load filename, extra_flags
        args = ["-c", cmd_connection_string, "-q", filename, "-onerror", "exit", "-nogui"]
        args.concat(Array(extra_flags)) if extra_flags

        establish_connection configuration_as_dba
        connection.execute("SET OPTION PUBLIC.min_password_length = 0")
        connection.drop_user(configuration["username"])

        Kernel.system("dbisql", *args)

        establish_connection configuration
      end

      private
        def need_option_for_password_unload?
          connection.sqlanywhere_version.to_s.split('.')[0].to_i == 17
        end

        def configuration
          @configuration
        end

        def configuration_without_database
          configuration_as_dba.merge("database" => 'utility_db')
        end

        def configuration_as_dba
          configuration.merge(ActiveRecord::ConnectionAdapters::SQLAnywhereAdapter::DEFAULT_AUTH)
        end

        def cmd_connection_string
          connection_string = "ENG=#{(configuration["server"])};"
          connection_string += "DBN=#{configuration["database"]};"
          connection_string += "UID=#{ActiveRecord::ConnectionAdapters::SQLAnywhereAdapter::DEFAULT_AUTH["username"]};"
          connection_string += "PWD=#{ActiveRecord::ConnectionAdapters::SQLAnywhereAdapter::DEFAULT_AUTH["password"]};"
          connection_string += "LINKS=#{configuration["commlinks"]};" if configuration["commlinks"]
          connection_string
        end
    end

    DatabaseTasks.register_task(/sqlanywhere/, ActiveRecord::Tasks::SQLAnywhereDatabaseTasks)
  end
end
