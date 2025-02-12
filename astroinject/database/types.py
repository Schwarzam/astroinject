from astroinject.database.dbpool import PostgresConnectionManager

import numpy as np
from logpool import control 

from astropy.table import MaskedColumn 

def build_type_map(config):
    """
    Build a type map dictionary by querying PostgreSQL's information_schema.
    
    :param db_params: Dictionary with connection parameters (dbname, user, password, host, port)
    :param table_name: Name of the target table.
    :param schema: Schema name (default is "public").
    :return: A dictionary mapping column names (lowercase) to type strings.
    """
    # Connect to PostgreSQL
    pg_conn = PostgresConnectionManager(**config["database"])
    
    infos = pg_conn.execute_query(f"""
            select *
            from information_schema.columns
            where table_schema NOT IN ('information_schema', 'pg_catalog')
            order by table_schema, table_name
        """, fetch = True)
    

    type_map = {}
    for row in infos:
        table_name = row[2]
        if table_name == config['tablename']:

            column_name = row[3]
            col_type = row[27]
            
            col_type = col_type.replace('character varying', 'VARCHAR')
            col_type = col_type.replace('character', 'CHAR')
            
            col_type = col_type.upper()
            
            db_type = None
            if 'DOUBLE' in col_type or 'FLOAT8' in col_type:
                db_type = 'DOUBLE'
                
            elif 'REAL' in col_type or 'FLOAT4' in col_type or 'FLOAT' in col_type:
                db_type = 'REAL'
                
            elif 'SMALLINT' in col_type or 'SHORT' in col_type:
                db_type = 'SMALLINT'
            
            elif 'INTEGER' in col_type:
                db_type = 'INTEGER'
                
            elif 'LONG' in col_type or 'BIGINT' in col_type:
                db_type = 'BIGINT'
            
            elif 'INT' in col_type:
                db_type = 'BIGINT'
            
            elif 'BOOL' in col_type:
                db_type = 'BOOL'
                
            elif 'CHAR' in col_type or 'TEXT' in col_type:
                db_type = 'VARCHAR'
            
            else:
                control.critical(f'did not found type representation for: {col_type} on column: {column_name}')

            if not col_type.startswith("_"):
                type_map[column_name] = db_type
            
    return type_map

def force_cast_types(table, type_map):
    """
    Force cast the columns of an Astropy Table to the types specified in type_map.
    Missing (NaN) values in numeric columns will be preserved as masked values.
    
    :param table: An astropy.table.Table.
    :param type_map: Dictionary mapping column names (lowercase) to type strings,
                     e.g. { "id": "BIGINT", "ra": "DOUBLE", ... }.
    :return: The table with columns cast to the desired types.
    """
    for col in table.colnames:
        # Assume column names are already lowercased
        if col in type_map:
            desired_type = type_map[col]
            try:
                # For integer types, if the column is originally floating and contains NaN,
                # convert to MaskedColumn so that NaNs remain masked.
                if desired_type in ("INTEGER", "INT", "BIGINT", "LONG", "SMALLINT"):
                    # If the column is floating (or already a MaskedColumn), create a MaskedColumn.
                    if np.issubdtype(table[col].dtype, np.floating):
                        # Convert to MaskedColumn with mask set where the data is NaN.
                        data = table[col].data
                        mask = np.isnan(data)
                        table[col] = MaskedColumn(data, mask=mask)
                    elif not isinstance(table[col], MaskedColumn):
                        # If not already masked, convert without any mask.
                        table[col] = MaskedColumn(table[col].data, mask=np.zeros(len(table[col]), dtype=bool))
                    
                    # Now cast the unmasked data to the desired integer type.
                    if desired_type in ("INTEGER", "INT"):
                        table[col] = table[col].astype(np.int32)
                    elif desired_type in ("BIGINT", "LONG"):
                        table[col] = table[col].astype(np.int64)
                    elif desired_type == "SMALLINT":
                        table[col] = table[col].astype(np.int16)
                elif desired_type == "DOUBLE":
                    table[col] = table[col].astype(np.float64)
                elif desired_type == "REAL":
                    table[col] = table[col].astype(np.float32)
                elif desired_type == "BOOL":
                    table[col] = table[col].astype(bool)
                elif desired_type == "VARCHAR":
                    table[col] = table[col].astype(str)
                else:
                    control.warn(f"Type {desired_type} for column {col} not explicitly handled.")
            except Exception as e:
                control.critical(f"Failed to cast column {col} to {desired_type}: {e}")
    return table
