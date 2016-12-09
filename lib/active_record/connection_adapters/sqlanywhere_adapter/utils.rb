module ActiveRecord::ConnectionAdapters::SQLAnywhereAdapter
  STRUCTURE_FILE_NAME_WITH_PATH = "db/structure.sql"
  UNLOAD_DATA_DIR = "test/raw_data"

  def dbunload options = [], directory = nil
    opt = options_with_defaults(options)
    opt << directory if directory

    Kernel.system("dbunload #{opt.join(' ')}")
  end

  def dbisql options, file_name
    opt = options_with_defaults(options)
    opt << file_name

    Kernel.system("dbisql #{opt.join(' ')}")
  end

  def unload_table table_name
    dbunload([
      "-d",
      "-y",
      "-xi",
      "-ss",
      "-t #{table_name}",
      "-r #{unloaded_sql_file_name(table_name)}"
    ], UNLOAD_DATA_DIR)
  end

  def structure_dump file_name=STRUCTURE_FILE_NAME_WITH_PATH
    dbunload([
      "-y",
      "-r #{file_name}",
      "-n"
    ])
  end

  def load_table table_name
    dbisql([
      "-onerror exit"
    ], unloaded_sql_file_name(table_name))
  end

  def structure_load file_name=STRUCTURE_FILE_NAME_WITH_PATH
    dbisql([
      "-onerror exit"
    ], file_name)
  end

  protected
    def unloaded_sql_file_name table_name
      "#{UNLOAD_DATA_DIR}/#{table_name}.sql"
    end

  private
    def options_with_defaults options
      options.clone.concat(default_options)
    end

    def default_options
      ["-c \"#{@connection_string}\""]
    end
end
