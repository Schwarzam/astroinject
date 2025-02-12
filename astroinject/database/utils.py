import numpy as np
from astroinject.utils import first_valid_index

# Function to infer PostgreSQL data types dynamically
def infer_pg_type(value):
    if isinstance(value, (np.int16, np.int32, np.int64, int)):
        if abs(value) > 2_147_483_647:  # PostgreSQL INTEGER limit
            return "BIGINT"
        else:
            return "INTEGER"
    elif isinstance(value, (np.float32, float)) and not isinstance(value, np.float64):
        return "FLOAT4"
    elif isinstance(value, np.float64):
        return "FLOAT8"
    elif isinstance(value, (np.bool_, bool)):
        return "BOOLEAN"
    elif isinstance(value, str):
        return "TEXT"
    if isinstance(value, np.ndarray):  # Multi-dimensional columns
        if isinstance(value[0], (np.int16, np.int32, np.int64, int)):
            return "BIGINT[]"
        elif isinstance(value[0], (np.float32, np.float64, float)):
            if isinstance(value[0], np.float32):
                return "FLOAT4[]"
            else:
                return "FLOAT8[]"
        elif isinstance(value[0], (np.bool_, bool)):
            return "BOOLEAN[]"
        elif isinstance(value[0], str):
            return "TEXT[]"  # Array of strings
    else:
        raise ValueError(f"Unsupported type: {type(value)}")
    
def convert_table_to_postgres_records(table):
    """Optimized conversion of an `astropy.table.Table` for PostgreSQL `COPY` bulk insert.
       Converts masked columns to replace masked values with `None`.
    """
    
    converted_data = []
    
    for col in table.colnames:
        col_data = table[col]

        # Handle string columns correctly
        if np.issubdtype(col_data.dtype, np.str_):  
            col_data = col_data.astype(str).tolist()

        # Handle integer columns
        elif np.issubdtype(col_data.dtype, np.integer):  
            col_data = col_data.astype(int).tolist()

        # Handle float columns
        elif np.issubdtype(col_data.dtype, np.floating):  
            col_data = col_data.astype(float).tolist()

        # Handle boolean columns
        elif np.issubdtype(col_data.dtype, np.bool_):  
            col_data = col_data.astype(bool).tolist()

        # Handle multi-dimensional columns
        elif isinstance(col_data[first_valid_index(col_data)], (list, np.ndarray)):  
            if np.issubdtype(col_data.dtype, np.integer):
                col_data = [list(map(int, row)) if row is not None else None for row in col_data]
            elif np.issubdtype(col_data.dtype, np.floating):
                col_data = [list(map(float, row)) if row is not None else None for row in col_data]
            else:
                col_data = [list(map(str, row)) if row is not None else None for row in col_data]

        else:  
            col_data = col_data.tolist()

        converted_data.append(col_data)

    # 🔥 Convert to row-wise format for PostgreSQL COPY
    return list(map(tuple, zip(*converted_data)))