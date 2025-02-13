from logpool import control
from astroinject.database.dbpool import PostgresConnectionManager

def map_table(config):
    pg_conn = PostgresConnectionManager(**config["database"])
    
    infos = pg_conn.execute_query(f"""
            select *
            from information_schema.columns
            where table_schema NOT IN ('information_schema', 'pg_catalog')
            order by table_schema, table_name
        """, fetch = True)
    
    schema = None
    table_name = None
    
    if "." in config['tablename']:
        schema = config['tablename'].split(".")[0]
        table_name = config['tablename'].split(".")[1]
    else:
        table_name = config['tablename']
    
    dic = {}
    count = 0
    for row in infos:
        table_name = row[2]
            
        if row[2] == table_name:
            if schema is not None and schema != row[1]:
                continue
            
            if schema is None:
                schema = row[1]
                
            column_name = row[3]
            col_type = row[27]
            
            col_type = col_type.replace('character varying', 'VARCHAR')
            col_type = col_type.replace('character', 'CHAR')
            
            col_type = col_type.upper()
            
            if 'DOUBLE' in col_type or 'FLOAT8' in col_type:
                count = count + 1
                dic[column_name] = 'DOUBLE'
                
            elif 'REAL' in col_type or 'FLOAT4' in col_type or 'FLOAT' in col_type:
                count = count + 1
                dic[column_name] = 'REAL'
                
            elif 'SMALLINT' in col_type or 'SHORT' in col_type:
                count = count + 1
                dic[column_name] = 'SMALLINT'
            
            elif 'INTEGER' in col_type:
                count = count + 1
                dic[column_name] = 'INTEGER'
                
            elif 'LONG' in col_type or 'BIGINT' in col_type:
                count = count + 1
                dic[column_name] = 'BIGINT'
            
            elif 'INT' in col_type:
                count = count + 1
                dic[column_name] = 'BIGINT'
            
            elif 'BOOL' in col_type:
                count = count + 1
                dic[column_name] = 'BOOL'
                
            elif 'CHAR' in col_type or 'TEXT' in col_type:
                count = count + 1
                dic[column_name] = 'VARCHAR'
            
            else:
                control.critical(f'did not found type representation for: {col_type} on column: {column_name}')

    ##Insert Columns
    querycols = []
    for column in dic:
        if column in dic:
            if column == 'ID' or column == 'RA' or column == 'DEC':
                principal = 1
                indexed = 0
                query = f"""INSERT INTO "TAP_SCHEMA"."columns" VALUES ('{table_name}', '{column}', '{dic[column]}', -1, 1, '', NULL, NULL, NULL, {indexed}, {principal}, 0, NULL);"""
                querycols.append(query)
            else:
                principal = 0
                indexed = 0
                query = f"""INSERT INTO "TAP_SCHEMA"."columns" VALUES ('{table_name}', '{column}', '{dic[column]}', -1, 1, '', NULL, NULL, NULL, {indexed}, {principal}, 0, NULL);"""
                querycols.append(query)

    for com in querycols:
        pg_conn.execute_query(com)

    ##Insert schema 
    try:
        query = f"""INSERT INTO "TAP_SCHEMA"."schemas" VALUES ('{schema}', NULL, NULL);"""
        pg_conn.execute_query(query)
    except:
        control.critical(f'Could not insert schema {schema}')

    ##Insert Table
    query = f"""INSERT INTO "TAP_SCHEMA"."tables" VALUES ('{schema}', '{table_name}', 'table', NULL, NULL, NULL);"""
    pg_conn.execute_query(query)
    
    pg_conn.close()
    control.info(f'succesfully mapped table {table_name} with {count} columns')