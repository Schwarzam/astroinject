import os

def first_valid_index(col_data):
    # check if column is masked
    for key, i in enumerate(col_data):
        # check if value is str
        if str(i) != 'masked' and '--' not in str(i) and str(i) != 'nan':
            return key
    return key

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
    files = os.popen(f"""find {folder} -path "{pattern}" """).read()
    if not files:
        return []

    files = files.split('\n')
    files = [f for f in files if f]
    return files