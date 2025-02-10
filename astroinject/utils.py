

def first_valid_index(col_data):
    # check if column is masked
    for key, i in enumerate(col_data):
        # check if value is str
        if str(i) != 'masked' and str(i) != '--' and str(i) != 'nan':
            return key
    return key