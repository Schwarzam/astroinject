from astropy.table import Table
from astroinject.utils import first_valid_index

from astroinject.database.utils import infer_pg_type

# Function to generate CREATE TABLE query dynamically
def generate_create_table_query(table_name, table, id_col=None):
    columns_definitions = []
    
    for col in table.colnames:
        sample_value = table[col][first_valid_index(table[col])]  # Take first row as a sample
        pg_type = infer_pg_type(sample_value)
        
        if id_col and id_col.lower() == col.lower():  # Ensure 'id' is the primary key
            columns_definitions.append(f"{col} {pg_type} PRIMARY KEY")  # Use BIGINT to avoid range issues
        else:
            columns_definitions.append(f"{col} {pg_type} NULL")

    columns_str = ",\n    ".join(columns_definitions)
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {columns_str}\n);"
    return create_table_query

def vacuum_query(table_name):
    return f"VACUUM ANALYZE {table_name};"
