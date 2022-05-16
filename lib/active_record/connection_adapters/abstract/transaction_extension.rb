# frozen_string_literal: true

require "active_record/connection_adapters/abstract/transaction"

module ActiveRecord
  module ConnectionAdapters
    module SQLAnywhereTransaction
      private

      def sqlanywhere?
        connection.respond_to?(:sqlanywhere?) && connection.sqlanywhere?
      end

      def current_sqlanywhere_isolation_level
        return unless sqlanywhere?

        connection.current_isolation_level
      end
    end

    Transaction.send :prepend, SQLAnywhereTransaction

    module SQLAnywhereRealTransaction
      attr_reader :starting_sqlanywhere_isolation_level
      def initialize(connection, isolation: nil, joinable: true, run_commit_callbacks: false)
        @connection = connection
        @starting_sqlanywhere_isolation_level = current_sqlanywhere_isolation_level if isolation
        super
      end

      def commit
        super
        reset_starting_sqlanywhere_isolation_level
      end

      def rollback
        super
        reset_starting_sqlanywhere_isolation_level
      end

      private

      def reset_starting_sqlanywhere_isolation_level
        if sqlanywhere? && starting_sqlanywhere_isolation_level
          connection.set_transaction_isolation_level(starting_sqlanywhere_isolation_level)
        end
      end
    end

    RealTransaction.send :prepend, SQLAnywhereRealTransaction
  end
end
