from astropy.table import Table

def open_table(table_name, format = "auto"):
    if format == "fits":
        table = Table.read(table_name)
    elif ".parquet" in table_name or format == "parquet":
        table = Table.read(table_name, format="parquet")
    elif ".csv" in table_name or format == "csv":
        table = Table.read(table_name, format="csv")
    else:
        raise ValueError(f"Unsupported format: {format}")

    return table

