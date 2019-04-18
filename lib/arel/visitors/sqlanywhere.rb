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

        collector = maybe_visit o.limit, collector
        collector = maybe_visit o.offset, collector

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
        collector = maybe_visit o.set_quantifier, collector

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
