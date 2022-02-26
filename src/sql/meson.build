srcs += custom_target(
  'gen-c-source-files-form-sql-queries',
  command : ['./gen.sh', '@OUTPUT@', '@INPUT@'],
  input : files(
    'create_tables.sql',
  ),
  output : ['sql_queries.c', 'sql_queries.h'],
)
