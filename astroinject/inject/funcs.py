import os
import logging
from astropy.table import Table
import re

# Filtering the dictionary to match the function parameters
def filter_args(func, args_dict):
    valid_keys = func.__code__.co_varnames[:func.__code__.co_argcount]
    return {k: args_dict[k] for k in valid_keys if k in args_dict}


def process_file_to_dataframe(
            file_name : list, 
            _format = "fits", ## fits, csv, csv_delimwhites
            delcolumns = [],
            addnullcols = [],
            fillna = -99,
            check_for_null=True
        ):
        
        logging.debug(f"Processing file {file_name}")
        try: 
            if _format == "fits":
                df = fits_to_dataframe(file_name)
            elif _format == "csv":
                df = pd.read_csv(file_name, index_col=False)
            elif _format == "csv_delimwhites":
                df = pd.read_csv(file_name, index_col=False, delim_whitespace=True)
            else:
                logging.error(f"Format {_format} not supported")
                return False
        except Exception as e:
            logging.error(f"Error reading file {file_name} {e}")
            return False    

        for col in delcolumns:
            try:
                df = df.drop(col, axis=1)
            except Exception as e:
                logging.debug(f"Error dropping column {col} {e}")

        for col in addnullcols:
            try:
                df[col] = None
            except Exception as e:
                logging.debug(f"Error adding null column {col} {e}")

        if check_for_null:
            if df.isnull().values.any():
                df = df.fillna(fillna)

        return df 
     

def find_files_with_pattern(folder, pattern):
    files = os.popen(f"""find {folder} -name "{pattern}" """).read()
    if not files:
        return []

    files = files.split('\n')
    files = [f for f in files if f]
    return files

def fits_to_dataframe(tablename):
    t = Table.read(tablename.replace('\n', ''))

    try:
        t.rename_column('FIELD_ID', 'ID')
    except:
        pass
    
    to_convert = []
    str_columns = []
    for col in t.columns: ##Convert incompatible columns
        if len(t[col].shape) > 1: ##col is 2D
            if t[col].shape[1] > 1:
                to_convert.append(col)


        if '|S' in str(t[col].dtype):
            str_columns.append(col)


    for col in to_convert:
        column_values = []

        for key, line in enumerate(t[col]): ##Convert array to string 
            temp = str(t[col][key])
            str_value = str(re.sub(r"\s+", ',', temp).replace(",]", "]"))
            column_values.append(str_value)

        t.remove_column(col)    
        t.add_column(column_values, name=col)

    t = t.to_pandas()
    str_columns = t[str_columns]

    str_columns = str_columns.stack().str.decode('utf-8').unstack()

    for col in str_columns:
        t[col] = str_columns[col]

    return t
