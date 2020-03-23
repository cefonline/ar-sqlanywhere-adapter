# frozen_string_literal: true

module Arel
  module Visitors
    class SQLAnywhere < Arel::Visitors::ToSql
      private

      def visit_Arel_Nodes_SelectStatement(o, collector)
        if o.with
          collector = visit o.with, collector
          collector << " "
        end

        if o.offset && !o.limit
          o.limit = Arel::Nodes::Limit.new(2147483647)
        end

        if o.limit && o.orders.empty?
          o.orders = [Arel::Nodes::Ascending.new(Arel.sql("1"))]
        end

        collector << "SELECT"

        distinct_core = o.cores.find { |core| core.set_quantifier.class == Arel::Nodes::Distinct }
        collector = maybe_visit distinct_core.set_quantifier, collector unless distinct_core.nil?
        collector = maybe_visit o.limit, collector

        if o.offset
          offset = Arel::Nodes::Offset.new(o.offset.expr.value.value + 1)
          o.offset = nil

          collector = maybe_visit offset, collector
        end

        collector = o.cores.inject(collector) { |c,x|
          visit_Arel_Nodes_SelectCore(x, c)
        }

        unless o.orders.empty?
          collector << ORDER_BY
          len = o.orders.length - 1
          o.orders.each_with_index { |x, i|
            collector = visit(x, collector)
            collector << COMMA unless len == i
          }
        end

        collector
      end

      def visit_Arel_Nodes_SelectCore(o, collector)
        collect_nodes_for o.projections, collector, " "

        if o.source && !o.source.empty?
          collector << " FROM "
          collector = visit o.source, collector
        end

        collect_nodes_for o.wheres, collector, " WHERE ", " AND "
        collect_nodes_for o.groups, collector, " GROUP BY "
        collect_nodes_for o.havings, collector, " HAVING ", " AND "
        collect_nodes_for o.windows, collector, " WINDOW "

        collector
      end

      def visit_Arel_Nodes_Offset(o, collector)
        collector << "START AT "
        visit(o.expr, collector)
      end

      def visit_Arel_Nodes_Limit(o, collector)
        collector << "TOP "
        visit(o.expr, collector)
      end

      def visit_Arel_Nodes_True(o, collector)
        "1=1"
      end

      def visit_Arel_Nodes_False(o, collector)
        "1=0"
      end
    end
  end
end
