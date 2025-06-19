import argparse

from astroinject.pipeline.apply_index import apply_pgsphere_index, apply_q3c_index, apply_btree_index
from astroinject.pipeline.injection import injection_procedure, create_table, parallel_insertion

from astroinject.utils import find_files_with_pattern
from astroinject.config import load_config

from astroinject.pipeline.map_tap_schema import map_table

import warnings
from logpool import control

warnings.filterwarnings("ignore")

def injection():
    parser = argparse.ArgumentParser(description="Inject data from a CSV file into a database")
    
    parser.add_argument("-b", "--baseconfig", help="Base database config file")
    parser.add_argument("-c", "--tableconfig", help="Table specifig config file")
    args = parser.parse_args()

    config = load_config(args.baseconfig)
    config.update(load_config(args.tableconfig))

    control.info("starting injection procedure")
    control.info(f"config: \n{config}")
    
    files = find_files_with_pattern(
        config["folder"],
        config["pattern"]
    )
    
    control.info(f"found {len(files)} files to inject")
    
    parallel_insertion(files, config)
    apply_pgsphere_index(config)

def create_index_command():
    parser = argparse.ArgumentParser(description="Create indexes on a table in the database")
    
    parser.add_argument("-b", "--baseconfig", help="Base database config file")
    parser.add_argument("-i", "--index_type", choices=["pgsphere", "q3c", "btree"], help="Type of index to create")
    parser.add_argument("-st", "--schema.table", help="Table to create indexes on (format: schema.table)")
    parser.add_argument("-ra", "--ra_col", help="Column name for Right Ascension")
    parser.add_argument("-dec", "--dec_col", help="Column name for Declination")
    parser.add_argument("-c", "--target_col", nargs='*', help="Additional columns for B-Tree index creation")
    
    args = parser.parse_args()

    config = load_config(args.baseconfig)
    
    if args.schema_table:
        schema, table = args.schema_table.split(".")
        config["tablename"] = f"{schema}.{table}"
    else:
        config["tablename"] = args.schema_table
    config["ra_col"] = args.ra_col.lower() if args.ra_col else None
    config["dec_col"] = args.dec_col.lower() if args.dec_col else None
    config["additional_btree_index"] = [args.target_col] if not isinstance(args.target_col, list) else args.target_col
    
    control.info("starting index creation procedure")
    control.info(f"config: \n{config}")
    
    if args.index_type == "pgsphere":
        apply_pgsphere_index(config)
    elif args.index_type == "q3c":
        apply_q3c_index(config)
    elif args.index_type == "btree":
        apply_btree_index(config)
    
    
def map_table_command():
    parser = argparse.ArgumentParser(description="map a table to the TAP_SCHEMA")
    
    parser.add_argument("-b", "--baseconfig", help="Base database config file")
    parser.add_argument("-c", "--tableconfig", help="Table specifig config file")
    args = parser.parse_args()

    config = load_config(args.baseconfig)
    config.update(load_config(args.tableconfig))
    
    control.info("starting mapping procedure")
    map_table(config)
    control.info("finished mapping procedure")
