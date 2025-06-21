import numpy as np
from logpool import control
from astropy.table import MaskedColumn

from astroinject.utils import first_valid_index
from astroinject.database.types import force_cast_types

def vectorized_string_to_array(column_data):
    """Fully vectorized conversion of formatted string arrays to NumPy arrays (handling sequences properly)."""
    
    # 🔹 Remove `{}` brackets using NumPy vectorized operations
    column_data = np.char.replace(column_data.astype(str), "{", "")
    column_data = np.char.replace(column_data, "}", "")

    # 🔹 Split strings into lists using NumPy
    split_arrays = np.char.split(column_data, ",")

    # 🔹 Extract first value for type detection
    first_value = split_arrays[0][0]  # ✅ Check only the first element

    # 🔥 Auto-detect type and convert (Handling Sequences Properly)
    if np.char.isnumeric(first_value):  
        return np.array([np.array(row, dtype=np.int64) for row in split_arrays], dtype=np.int64)  # ✅ Integer Array

    elif np.char.isnumeric(np.char.replace(first_value, ".", "")):  
        return np.array([np.array(row, dtype=np.float64) for row in split_arrays], dtype=np.float64)  # ✅ Float Array

    return np.array(split_arrays, dtype=object)  # ✅ String Array (Default)

def convert_str_arrays_to_arrays(table):
    for col in table.colnames:
        fv = first_valid_index(table[col])
        
        if np.issubdtype(table[col][fv].dtype, str) and "{" in table[col][fv]:
            table[col] = vectorized_string_to_array(table[col])
    
    return table
            

def preprocess_table(
        table, 
        config,
        types_map = None
    ):
    
    if config["delete_columns"]:
        table.remove_columns(config["delete_columns"])
    
    for col in table.colnames:
        table.rename_column(col, col.lower())
    
    if types_map:
        table = force_cast_types(table, types_map)
    
    for col in table.colnames:
        if "|S" in str(table[col].dtype):
            table[col] = table[col].astype(str)
    
    if config["rename_columns"]:
        for col in config["rename_columns"]:
            table.rename_column(col, config["rename_columns"][col].lower())
    
    if config["patterns_to_replace"]:
        for info in config["patterns_to_replace"]:
            info['name'] = info['name'].lower()
            table[info['name']] = np.char.replace(table[info['name']], info["pattern"], info["replacement"])
    
    if "mask_value" in config and config["mask_value"]:
        for col in table.colnames:
            if np.issubdtype(table[col].dtype, (np.number)):  # Only modify numeric columns
                if "MaskedColumn" in str(type(table[col])):
                    control.warn(f"column {col} is already masked, cannot mask again")
                    continue
                else:
                    table[col] = MaskedColumn(table[col], mask=(table[col] == config["mask_value"]))  
    
    table = convert_str_arrays_to_arrays(table)
    # TODO: Add type optimization
    #table = optimize_table_types(table)
    
    return table