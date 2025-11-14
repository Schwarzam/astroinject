from astropy.table import Table
import gzip

import pandas as pd
from logpool import control

import warnings
warnings.filterwarnings("ignore")

def open_table(table_name, config):
	
	if "format" in config:
		format = config["format"]
	else:
		format = "auto"
	
	if format == "auto":
		format = table_name.split(".")[-1]
	
	if format == "gaia":
		control.info(f"reading table {table_name} with format {format}")
	
		table = Table.read(
			table_name,                # e.g. "gaia_chunk.ecsv.gz"
			format="ascii.ecsv",
			guess=False,               # skip format guessing
			fill_values=[('null', 99), ('nan', 99)],
		)
		return table
	
			
	if format == "fits":
		table = Table.read(table_name)
	elif ".parquet" in table_name or format == "parquet":
		try:
			table = Table.read(table_name, format="parquet")
		except Exception as e:
			#control.critical(f"Error reading parquet file, falling back to pandas: {e}")
			table = pd.read_parquet(table_name)
			table = Table.from_pandas(table)
	
 
	elif ".csv" in table_name or format == "csv":
		table = Table.read(table_name, format="csv")
	else:
		raise ValueError(f"Unsupported format: {format}")
	return table

