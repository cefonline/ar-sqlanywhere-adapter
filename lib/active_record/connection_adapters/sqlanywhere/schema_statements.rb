# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module SQLAnywhere
      module SchemaStatements
        def native_database_types
          {
            primary_key: "INTEGER PRIMARY KEY DEFAULT AUTOINCREMENT NOT NULL",
            string: { name: "varchar", limit: 255 },
            text: { name: "long varchar" },
            integer: { name: "integer", limit: 4 },
            float: { name: "float" },
            decimal: { name: "decimal" },
            datetime: { name: "datetime" },
            timestamp: { name: "datetime" },
            time: { name: "time" },
            date: { name: "date" },
            binary: { name: "binary" },
            boolean: { name: "bit" }
          }
        end

        # SQL Anywhere does not support sizing of integers based on the sytax INTEGER(size). Integer sizes
        # must be captured when generating the SQL and replaced with the appropriate size.
        def type_to_sql(type, limit: nil, precision: nil, scale: nil, **)
          type = type.to_sym

          return super unless native_database_types[type]

          if type == :integer
            case limit
            when 1 then "tinyint"
            when 2 then "smallint"
            when 3..4 then "integer"
            when 5..8 then "bigint"
            else "integer"
            end
          elsif type == :string && !limit.nil?
            "varchar (#{limit})"
          elsif type == :boolean
            "bit"
          elsif type == :binary
            if limit
              "binary (#{limit})"
            else
              "long binary"
            end
          else
            super
          end
        end

        def data_source_sql(name = nil, type: nil)
          scope = quoted_scope(name, type: type)
          scope[:type] ||= "'BASE','GBL TEMP','VIEW'"

          <<~SQL.squish
            SELECT SYS.SYSUSER.user_name + '.' + SYS.SYSTAB.table_name table_name
            FROM SYS.SYSTAB
            JOIN SYS.SYSUSER ON SYS.SYSUSER.user_id = SYS.SYSTAB.creator
            WHERE
              SYS.SYSTAB.table_type_str IN (#{scope[:type]}) AND
              SYS.SYSUSER.user_name = #{scope[:owner]} AND
              SYS.SYSTAB.server_type = 1
              #{"AND SYS.SYSTAB.table_name = #{scope[:name]}" if scope[:name]}
          SQL
        end

        def new_column_from_field(table_name, field)
          type_metadata = fetch_type_metadata(field["domain"])

          # numerics and string literals are default
          if  /^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$/.match?(field["default"]) ||
              /^'.*'$/.match?(field["default"]) ||
              field["default"].nil?
          then
            default, default_function = field["default"], nil
          else
            default, default_function = nil, field["default"].upcase
          end

          SQLAnywhere::Column.new(
            field["name"],
            default,
            type_metadata,
            (field["nulls"] == 1),
            default_function,
            comment: field["remarks"]
          )
        end

        def indexes(table_name, name = nil)
          scope = quoted_scope(table_name)

          sql = <<~SQL.squish
            SELECT DISTINCT index_name, \"unique\"
            FROM SYS.SYSTABLE
            INNER JOIN SYS.SYSIDXCOL ON SYS.SYSTABLE.table_id = SYS.SYSIDXCOL.table_id
            INNER JOIN SYS.SYSIDX ON SYS.SYSTABLE.table_id = SYS.SYSIDX.table_id AND SYS.SYSIDXCOL.index_id = SYS.SYSIDX.index_id
            INNER JOIN SYS.SYSUSER ON SYS.SYSUSER.user_id = SYS.SYSTABLE.creator
            WHERE
              SYS.SYSTABLE.table_name = #{scope[:name]} AND
              SYS.SYSIDX.index_category > 2 AND
              SYS.SYSUSER.user_name = #{scope[:owner]}
          SQL

          exec_query(sql, name).map do |row|
            sql = <<~SQL.squish
              SELECT column_name
              FROM SYS.SYSIDX
              INNER JOIN SYS.SYSIDXCOL ON
                SYS.SYSIDXCOL.table_id = SYS.SYSIDX.table_id AND
                SYS.SYSIDXCOL.index_id = SYS.SYSIDX.index_id
              INNER JOIN SYS.SYSCOLUMN ON
                SYS.SYSCOLUMN.table_id = SYS.SYSIDXCOL.table_id AND
                SYS.SYSCOLUMN.column_id = SYS.SYSIDXCOL.column_id
              WHERE index_name = '#{row["index_name"]}'
            SQL

            IndexDefinition.new(
              table_name,
              row["index_name"],
              row["unique"] == 1,
              exec_query(sql, name).map { |col| col["column_name"] }
            )
          end
        end

        def primary_key(table_name)
          scope = quoted_scope(table_name)

          sql = <<~SQL.squish
            select cname
            from SYS.SYSCOLUMNS
            where tname = #{scope[:name]}
              and creator = #{scope[:owner]}
              and in_primary_key = 'Y'
          SQL

          rs = exec_query(sql, "SCHEMA")
          if !rs.nil? && !rs.first.nil?
            rs.first["cname"]
          else
            nil
          end
        end

        def remove_index(table_name, options = {})
          exec_query "DROP INDEX #{quote_table_name(table_name)}.#{quote_column_name(index_name(table_name, options))}"
        end

        def rename_table(name, new_name)
          exec_query "ALTER TABLE #{quote_table_name(name)} RENAME #{quote_table_name(new_name)}"
          rename_table_indexes(name, new_name)
        end

        def change_column_default(table_name, column_name, default)
          sql = <<~SQL.squish
            ALTER TABLE #{quote_table_name(table_name)}
            ALTER #{quote_column_name(column_name)} DEFAULT #{quote(default)}
          SQL

          exec_query(sql)
        end

        def change_column_null(table_name, column_name, null, default = nil)
          unless null || default.nil?
            sql = <<~SQL.squish
              UPDATE #{quote_table_name(table_name)}
              SET #{quote_column_name(column_name)}=#{quote(default)}
              WHERE #{quote_column_name(column_name)} IS NULL
            SQL

            exec_query(sql)
          end

          sql = <<~SQL.squish
            ALTER TABLE #{quote_table_name(table_name)}
            SET #{quote_column_name(column_name)}=#{quote(default)}
            ALTER #{quote_column_name(column_name)} #{null ? "" : "NOT"} NULL
          SQL

          exec_query(sql)
        end

        def change_column(table_name, column_name, type, options = {})
          sql_type = type_to_sql(type, limit: options[:limit], precision: options[:precision], scale: options[:scale])
          sql = <<~SQL.squish
            ALTER TABLE #{quote_table_name(table_name)}
            ALTER #{quote_column_name(column_name)} #{sql_type}#{options[:null] ? " NULL" : ""}
          SQL

          exec_query(sql)
        end

        def rename_column(table_name, column_name, new_column_name)
          if column_name.downcase == new_column_name.downcase
            whine = "if_the_only_change_is_case_sqlanywhere_doesnt_rename_the_column"
            rename_column table_name, column_name, "#{new_column_name}#{whine}"
            rename_column table_name, "#{new_column_name}#{whine}", new_column_name
          else
            sql = <<~SQL.squish
              ALTER TABLE #{quote_table_name(table_name)}
              RENAME #{quote_column_name(column_name)} TO #{quote_column_name(new_column_name)}
            SQL

            exec_query(sql)
          end
          rename_column_indexes(table_name, column_name, new_column_name)
        end

        def remove_column(table_name, column_name, type = nil, **options)
          sql = <<~SQL.squish
            SELECT "index_name" FROM SYS.SYSTAB join SYS.SYSTABCOL join SYS.SYSIDXCOL join SYS.SYSIDX
            WHERE "column_name" = '#{column_name}' AND "table_name" = '#{table_name}'
          SQL
          select(sql, nil).each do |row|
            execute "DROP INDEX \"#{table_name}\".\"#{row["index_name"]}\""
          end
          exec_query "ALTER TABLE #{quote_table_name(table_name)} DROP #{column_name}"
        end

        # SQLA requires the ORDER BY columns in the select list for distinct queries, and
        # requires that the ORDER BY include the distinct column.
        def columns_for_distinct(columns, orders)
          order_columns = orders.reject(&:blank?).map do |s|
              # Convert Arel node to string
              s = s.to_sql unless s.is_a?(String)
              # Remove any ASC/DESC modifiers
              s.gsub(/\s+(?:ASC|DESC)\b/i, "")
               .gsub(/\s+NULLS\s+(?:FIRST|LAST)\b/i, "")
            end.reject(&:blank?).map.with_index { |column, i| "#{column} AS alias_#{i}" }

          [super, *order_columns].join(", ")
        end

        def primary_keys(table_name)
          scope = quoted_scope(table_name)
          # SQL to get primary keys for a table
          sql = <<~SQL.squish
            SELECT list(c.column_name ORDER BY ixc.sequence) AS pk_columns
            FROM SYSIDX ix, SYSTABLE t, SYSIDXCOL ixc, SYSCOLUMN c, SYSUSER s
            WHERE ix.table_id = t.table_id
              AND ixc.table_id = t.table_id
              AND ixc.index_id = ix.index_id
              AND ixc.table_id = c.table_id
              AND ixc.column_id = c.column_id
              AND ix.index_category in (1,2)
              AND t.table_name = #{scope[:name]}
              AND s.user_name = #{scope[:owner]}
            GROUP BY ix.index_name, ix.index_id, ix.index_category
            ORDER BY ix.index_id
          SQL
          pks = exec_query(sql, "SCHEMA").to_hash.first

          pks["pk_columns"] ? pks["pk_columns"].split(",") : nil
        end

        def foreign_keys(table_name)
          scope = quoted_scope(table_name)

          # Don't support compound fk
          sql = <<~SQL.squish
            SELECT
                '"' + user_name(systab_p.creator) + '"."' + systab_p.table_name + '"' to_table,
                systabcol_p.column_name primary_key,
                systabcol_f.column_name column,
                sysidx.index_name name,
                isnull(systrigger_c.referential_action, 'R') on_update,
                isnull(systrigger_d.referential_action, 'R') on_delete
            FROM sys.sysfkey
            JOIN sys.sysidxcol sysidxcol_f ON
              sysidxcol_f.table_id = sysfkey.foreign_table_id
              AND sysidxcol_f.index_id = sysfkey.foreign_index_id
            JOIN sys.sysidxcol sysidxcol_p ON
              sysidxcol_p.table_id = sysfkey.primary_table_id
              AND sysidxcol_p.index_id = sysfkey.primary_index_id
            JOIN sys.systable systab_f ON systab_f.table_id = sysidxcol_f.table_id
            JOIN sys.sysuser sysuser_f ON sysuser_f.user_id = systab_f.creator
            JOIN sys.systable systab_p ON systab_p.table_id = sysidxcol_p.table_id
            JOIN sys.systabcol systabcol_f ON
              systabcol_f.table_id = sysidxcol_f.table_id
              AND systabcol_f.column_id = sysidxcol_f.column_id
            JOIN sys.systabcol systabcol_p ON
              systabcol_p.table_id = sysidxcol_p.table_id
              AND systabcol_p.column_id = sysidxcol_p.column_id
            JOIN sys.sysidx ON sysidx.table_id = sysfkey.foreign_table_id
              AND sysidx.index_id = sysfkey.foreign_index_id
            LEFT JOIN sys.systrigger systrigger_c ON
              systrigger_c.table_id = sysfkey.primary_table_id
              AND systrigger_c.foreign_table_id = sysfkey.foreign_table_id
              AND systrigger_c.foreign_key_id = sysfkey.foreign_index_id
              AND systrigger_c.event = 'C'
            LEFT JOIN sys.systrigger systrigger_d ON
              systrigger_d.table_id = sysfkey.primary_table_id
              AND systrigger_d.foreign_table_id = sysfkey.foreign_table_id
              AND systrigger_d.foreign_key_id = sysfkey.foreign_index_id
              AND systrigger_d.event = 'D'
            WHERE
              sysidxcol_f.primary_column_id = sysidxcol_p.column_id
              AND systab_f.table_name = #{scope[:name]}
              AND sysuser_f.user_name = #{scope[:owner]}
              AND (
                SELECT count(*)
                FROM sysidxcol
                WHERE sysidxcol.table_id = sysfkey.foreign_table_id AND sysidxcol.index_id = sysfkey.foreign_index_id
              ) = 1
          SQL

          fk_info = exec_query(sql, "SCHEMA")
          fk_info.map do |row|
            options = {
              column: row["column"],
              name: row["name"],
              primary_key: row["primary_key"]
            }

            options[:on_update] = extract_foreign_key_action(row["on_update"])
            options[:on_delete] = extract_foreign_key_action(row["on_delete"])

            ForeignKeyDefinition.new(table_name, row["to_table"], options)
          end
        end

        def create_schema_dumper(options) # :nodoc:
          SQLAnywhere::SchemaDumper.create(self, options)
        end

        private

        # http://dcx.sap.com/index.html#sa160/en/dbreference/systrigger-system-view.html
        def extract_foreign_key_action(specifier)
          case specifier
          when "C"; :cascade
          when "D"; :default
          when "N"; :nullify
          when "R"; :restrict
          end
        end

        def schema_creation
          SQLAnywhere::SchemaCreation.new(self)
        end

        def quoted_scope(name = nil, type: nil)
          owner, name = extract_owner_qualified_name(name)
          type = \
            case type
            when "BASE TABLE"
              "'BASE', 'GBL TEMP'"
            when "VIEW"
              "'VIEW'"
            end
          scope = {}
          scope[:owner] = owner ? quote(owner) : "ANY(SELECT user_name FROM SYS.SYSUSER WHERE user_name != 'SYS')"
          scope[:name] = quote(name) if name
          scope[:type] = type
          scope
        end

        def extract_owner_qualified_name(string)
          name = Utils.extract_owner_qualified_name(string.to_s)

          [name.owner, name.identifier]
        end
      end
    end
  end
end
