srcs += custom_target(
  'gen-c-source-files-form-sql-queries',
  command : ['./gen.sh', '@OUTPUT@', '@INPUT@'],
  input : files(
    'create_tables.sql',
    'get_file.sql',
    'get_tag.sql',
    'get_tags.sql',
    'get_files_in_tags.sql',
    'has_file_tags.sql',
    'insert_tag.sql',
  ),
  output : ['sql_queries.c', 'sql_queries.h'],
)
