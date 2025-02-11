
def map_table(self):
    self._check_connection()

    res = self._cursor.execute(f"""select *
            from information_schema.columns
            where table_schema NOT IN ('information_schema', 'pg_catalog')
            order by table_schema, table_name""")
    
    dic = {}
    count = 0
    for row in self._cursor:
        if row['table_name'] == self._tablename:
            typ = row['data_type'].upper()
            
            if 'DOUBLE' in str(typ).upper():
                count = count + 1
                dic[row['column_name']] = 'DOUBLE'
                
            elif 'REAL' in str(typ).upper() or 'FLOAT' in str(typ).upper():
                count = count + 1
                dic[row['column_name']] = 'REAL'
                
            elif 'SMALLINT' in str(typ).upper() or 'SHORT' in str(typ).upper():
                count = count + 1
                dic[row['column_name']] = 'SMALLINT'
            
            elif 'INTEGER' in str(typ).upper():
                count = count + 1
                dic[row['column_name']] = 'INTEGER'
                
            elif 'LONG' in str(typ).upper() or 'BIGINT' in str(typ).upper():
                count = count + 1
                dic[row['column_name']] = 'BIGINT'
            
            elif 'INT' in str(typ).upper():
                count = count + 1
                dic[row['column_name']] = 'BIGINT'
                
            elif 'CHAR' in str(typ).upper() or 'TEXT' in str(typ).upper():
                count = count + 1
                dic[row['column_name']] = 'VARCHAR'
            
            else:
                logging.error('Did not found type representation for: ', typ, ' on column: ', row['column_name'])

    ##Insert Columns
    querycols = []
    for column in dic:
        if column in dic:
            if column == 'ID' or column == 'RA' or column == 'DEC':
                principal = 1
                indexed = 0
                query = f"""INSERT INTO "TAP_SCHEMA"."columns" VALUES ('{self._tablename}', '{column}', 'description', NULL, NULL, NULL, '{dic[column]}', -1, {principal}, {indexed}, 1, NULL);"""
                querycols.append(query)
            else:
                principal = 0
                indexed = 0
                query = f"""INSERT INTO "TAP_SCHEMA"."columns" VALUES ('{self._tablename}', '{column}', 'description', NULL, NULL, NULL, '{dic[column]}', -1, {principal}, {indexed}, 1, NULL);"""
                querycols.append(query)

    for com in querycols:
        self.execute(com)

    ##Insert schema 
    try:
        query = f"""INSERT INTO "TAP_SCHEMA"."schemas" VALUES ('{self._schema}', NULL, NULL, NULL);"""
        self.execute(query)
    except:
        logging.error(f'Couldnt insert schema {self._schema}')

    ##Insert Table
    query = f"""INSERT INTO "TAP_SCHEMA"."tables" VALUES ('{self._schema}', '{self._tablename}', 'table', NULL, NULL, NULL);"""
    self.execute(query)