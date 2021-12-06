SELECT tab.table_schema,
       tab.table_name,
       tinf.tbl_rows AS rows
FROM svv_tables tab
         JOIN svv_table_info tinf
              ON tab.table_schema = tinf.schema
                  AND tab.table_name = tinf.table
WHERE tab.table_type = 'BASE TABLE'
  AND tab.table_schema = 'tickit_demo'
ORDER BY tab.table_name;
