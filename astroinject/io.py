from astropy.table import Table
import gzip

from logpool import control

def open_table(table_name, config):
	
	if "format" in config:
		format = config["format"]
  
	
	if format == "auto":
		format = table_name.split(".")[-1]
	
	if format == "gaia":
		control.info(f"reading table {table_name} with format {format}")
	
		with gzip.open(table_name, "rt") as f:
			table = Table.read(
					f.read(), 
					format="ascii.ecsv", 
					fill_values=[('null', 99), ('nan', 99)]
			)
			
	if format == "fits":
		table = Table.read(table_name)
	elif ".parquet" in table_name or format == "parquet":
		table = Table.read(table_name, format="parquet")
	elif ".csv" in table_name or format == "csv":
		table = Table.read(table_name, format="csv")
	else:
		raise ValueError(f"Unsupported format: {format}")

	return table

