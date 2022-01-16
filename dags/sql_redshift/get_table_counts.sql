SELECT
    svv_tables.table_schema,
    svv_tables.table_name,
    svv_table_info.tbl_rows
FROM svv_tables
INNER JOIN svv_table_info
    ON svv_tables.table_schema = svv_table_info.schema
        AND svv_tables.table_name = tinf.table
WHERE svv_tables.table_type = 'BASE TABLE'
      AND svv_tables.table_schema = 'tickit_demo'
ORDER BY svv_tables.table_name;
