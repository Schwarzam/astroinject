import os
import logging
from astropy.table import Table
import re
import pandas as pd

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
            col_pattern_replace = [],
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
        
        if col_pattern_replace:
            for col in col_pattern_replace:
                try:
                    df[col["name"]] = df[col["name"]].str.replace(col["pattern"], col["replacement"])
                except Exception as e:
                    logging.debug(f"Error replacing pattern {col} {e}")

        return df 

def write_checkpoint(filename, config):
    filename = os.path.basename(filename)
    if "checkpoint" in config:
        f = open(config["checkpoint"], "w")
        f.write(filename + "\n")
        f.close()

def is_file_in_checkpoint(filename, config):
    filename = os.path.basename(filename)
    if "checkpoint" in config:
        if os.path.exists(config["checkpoint"]):
            f = open(config["checkpoint"], "r")
            lines = f.readlines()
            f.close()
            if filename in lines:
                return True
    return False

def write_error(msg, config):
    if "error" in config:
        f = open(config["error"], "a")
        f.write(msg + "\n")
        f.close()

def inject_files_procedure(files, conn, operation, config):
    for key, file in enumerate(files):
        if is_file_in_checkpoint(file, config):
            logging.info(f"File {os.path.basename(file)} already injected")
            continue

        filtered_args = filter_args(process_file_to_dataframe, operation)
        df = process_file_to_dataframe(file, **filtered_args)
        
        if df is False:
            logging.error(f"Error opening file {os.path.basename(file)}")   
            write_error(f"Error opening file {os.path.basename(file)}", config)
            continue

        res = conn.inject(df)
        
        if res is False:
            logging.error(f"Error injecting file {os.path.basename(file)}")
            write_error(f"Error injecting file {os.path.basename(file)}", config)
            continue
        
        if key == 0:
            logging.info(f"Creating keys on {conn._tablename} {conn._schema}")

            filtered_args = filter_args(conn.apply_pkey, operation)
            conn.apply_pkey(**filtered_args)
             
            filtered_args = filter_args(conn.apply_coords_index, operation)
            conn.apply_coords_index(**filtered_args)

            filtered_args = filter_args(conn.apply_field_index, operation)
            conn.apply_field_index(**filtered_args)
        
        write_checkpoint(file, config)
        logging.info(f"File {os.path.basename(file)} injected successfully")


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
