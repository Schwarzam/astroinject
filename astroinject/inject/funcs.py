import os
import logging
from astropy.table import Table
import re
import pandas as pd

# Filtering the dictionary to match the function parameters
def filter_args(func, args_dict):
    """
    Filters a dictionary to match the function parameters.

    Parameters
    ----------
    func : function
        The function whose parameters will be used to filter the dictionary.
    args_dict : dict
        Dictionary containing arguments to be filtered.

    Returns
    -------
    dict
        A new dictionary containing only the key-value pairs where the keys match the function's parameters.

    Examples
    --------
    >>> def example_function(a, b, c):
    ...     pass
    ...
    >>> args = {'a': 1, 'b': 2, 'd': 4}
    >>> filter_args(example_function, args)
    {'a': 1, 'b': 2}

    Notes
    -----
    - This function uses Python's introspection to determine valid function parameters.
    - The function `func` should be defined before calling this method.
    
    """
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
        """
        Processes a file and returns its contents as a Pandas DataFrame.

        Parameters
        ----------
        file_name : list
            Name of the file to be processed.
        _format : str, optional
            Format of the file, either 'fits', 'csv', or 'csv_delimwhites'. Default is 'fits'.
        delcolumns : list, optional
            List of column names to be deleted from the DataFrame. Default is an empty list.
        addnullcols : list, optional
            List of column names to be added to the DataFrame with null values. Default is an empty list.
        fillna : int, optional
            Value to fill NA/NaN values in the DataFrame. Default is -99.
        col_pattern_replace : list of dicts, optional
            List of dictionaries containing column names ('name'), patterns ('pattern'), and replacements ('replacement') for string replacement within the DataFrame. Default is an empty list.
        check_for_null : bool, optional
            Whether to check for null values in the DataFrame. If true, fills null values with `fillna`. Default is True.

        Returns
        -------
        pd.DataFrame
            The processed DataFrame, or False if an error occurs.

        Examples
        --------
        >>> process_file_to_dataframe("data.fits", _format="fits", delcolumns=["col1", "col2"])
        >>> process_file_to_dataframe("data.csv", _format="csv", fillna=0, check_for_null=True)

        Notes
        -----
        - Make sure the file exists and is in the correct format.
        - Logging is used for debugging and error reporting.

        """

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
        f = open(config["checkpoint"], "a")
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
    """
    Injects a list of files into a database connection after processing them into DataFrames.

    Parameters
    ----------
    files : list
        List of file paths to be processed and injected.
    conn : object
        Database connection object with an `inject` method for injecting DataFrames.
    operation : dict
        Dictionary containing arguments to be used in various function calls within the procedure.
    config : object
        Configuration object containing checkpoint and error-writing information.

    Returns
    -------
    None

    Side Effects
    ------------
    - Writes to log files using the logging module.
    - Writes to error and checkpoint files defined in the `config` object.
    - Injects processed DataFrames into the database via the `conn` object.

    Examples
    --------
    >>> conn = DatabaseConnection(...)
    >>> files = ["file1.csv", "file2.csv"]
    >>> operation = {"_format": "csv", "delcolumns": ["col1"]}
    >>> config = Config(...)
    >>> inject_files_procedure(files, conn, operation, config)

    Notes
    -----
    - Logging is used for debugging and error reporting.
    - Function assumes that `conn` object has methods `inject`, `apply_pkey`, `apply_coords_index`, and `apply_field_index`.
    - The `config` object should have methods for writing errors and checkpoints.

    """
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
    """
    Finds files within a folder that match a given pattern.

    Parameters
    ----------
    folder : str
        Path of the folder to search within.
    pattern : str
        Pattern to match files against. This should be a shell-style wildcard pattern.

    Returns
    -------
    list
        List of file paths that match the given pattern within the folder. Returns an empty list if no files match.

    Examples
    --------
    >>> find_files_with_pattern("/home/user/data", "*.csv")
    ['/home/user/data/file1.csv', '/home/user/data/file2.csv']

    >>> find_files_with_pattern("/home/user/data", "*.txt")
    []

    Notes
    -----
    - Uses `os.popen` and the `find` command-line utility to perform the file search, so this function is specific to Unix-like systems.
    - The pattern should be a shell-style wildcard pattern (e.g., "*.csv" for CSV files).

    """
    files = os.popen(f"""find {folder} -name "{pattern}" """).read()
    if not files:
        return []

    files = files.split('\n')
    files = [f for f in files if f]
    return files

def fits_to_dataframe(tablename):
    """
    Converts a FITS table to a Pandas DataFrame.

    Parameters
    ----------
    tablename : str
        Path of the FITS file containing the table to be converted.

    Returns
    -------
    pd.DataFrame
        A Pandas DataFrame containing the data from the FITS table.

    Raises
    ------
    Exception
        If there is an issue with reading the FITS file or converting its data.

    Examples
    --------
    >>> df = fits_to_dataframe("data.fits")
    >>> df.head()

    Notes
    -----
    - The function uses the Astropy `Table` class to read the FITS file.
    - Columns in the FITS table that are 2D arrays are converted to strings in the DataFrame.
    - The function attempts to rename the column 'FIELD_ID' to 'ID' if it exists.
    - String columns in the FITS file are decoded from bytes to UTF-8.

    """
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
