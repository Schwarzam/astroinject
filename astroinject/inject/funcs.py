import os

# Filtering the dictionary to match the function parameters
def filter_args(func, args_dict):
    valid_keys = func.__code__.co_varnames[:func.__code__.co_argcount]
    return {k: args_dict[k] for k in valid_keys if k in args_dict}


def find_files_with_pattern(folder, pattern):
    files = os.popen(f"""find {folder} -name "{pattern}" """).read()
    files = files.split('\n')
    files = [f for f in files if f]
    return files
