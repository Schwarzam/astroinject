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
    
    if "additional_btree_index" in config and config["additional_btree_index"]:
        control.info("creating additional B-Tree index")
        apply_btree_index(config)
    
    if not "index_type" in config or config["index_type"] is None:
        control.info("no index type specified, skipping index creation")
    elif config["index_type"] == "pgsphere":
        apply_pgsphere_index(config)
    elif config["index_type"] == "q3c":
        apply_q3c_index(config)

def create_schema_command():
    from astroinject.database.dbpool import PostgresConnectionManager
    from astroinject.database.gen_base_queries import vacuum_query
    parser = argparse.ArgumentParser(description="Create a table in the database")
    
    parser.add_argument("-b", "--baseconfig", help="Base database config file")
    parser.add_argument("-s", "--schema", help="Schema to create (format: schema)")

    args = parser.parse_args()
    config = load_config(args.baseconfig)
    
    # create query 
    query = "CREATE SCHEMA IF NOT EXISTS {schema};".format(schema=args.schema)
    
    pg_conn = PostgresConnectionManager(use_pool=False, **config["database"])
    control.info(f"executing:\n{query}")
    pg_conn.execute_query(query)
    pg_conn.close()
    control.info("done creating schema.")
    
        

def create_index_command():
    parser = argparse.ArgumentParser(description="Create indexes on a table in the database")
    
    parser.add_argument("-b", "--baseconfig", help="Base database config file")
    parser.add_argument("-i", "--index_type", choices=["pgsphere", "q3c", "btree"], help="Type of index to create")
    parser.add_argument("-st", "--schema_table", help="Table to create indexes on (format: schema.table)")
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
    
    if args.index_type == "pgsphere":
        apply_pgsphere_index(config)
    elif args.index_type == "q3c":
        apply_q3c_index(config)
    elif args.index_type == "btree":
        apply_btree_index(config)
    

def execute_query_command():
    from astroinject.database.dbpool import PostgresConnectionManager
    from astroinject.database.gen_base_queries import vacuum_query

    parser = argparse.ArgumentParser(description="Execute a custom SQL query on the database")
    
    parser.add_argument("-b", "--baseconfig", help="Base database config file")
    parser.add_argument("-q", "--query", help="SQL query to execute")
    parser.add_argument("-f", "--query_file", help="File containing SQL query to execute")
    parser.add_argument("--vacuum", action="store_true", help="Run VACUUM after query execution")
    parser.add_argument("-st", "--schema_table", help="Table name for vacuum (format: schema.table)")
    
    args = parser.parse_args()

    if not args.query and not args.query_file:
        raise ValueError("Either --query or --query_file must be provided")

    config = load_config(args.baseconfig)
    
    # Get the query from command line or file
    if args.query_file:
        with open(args.query_file, 'r') as f:
            query = f.read().strip()
    else:
        query = args.query
    
    pg_conn = PostgresConnectionManager(use_pool=False, **config["database"])
    control.info(f"executing custom query:\n{query}")
    
    pg_conn.execute_query(query)
    
    # Optional vacuum after query
    if args.vacuum and args.schema_table:
        vacuum_q = vacuum_query(args.schema_table)
        control.info(f"executing:\n{vacuum_q}")
        pg_conn.execute_query_wt_tblock(vacuum_q)
    
    pg_conn.close()
    control.info("done executing custom query.")    

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
