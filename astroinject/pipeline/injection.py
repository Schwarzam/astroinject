from logpool import control

from astroinject.io import open_table
from astroinject.processing import preprocess_table
from astroinject.database.utils import convert_table_to_postgres_records
from astroinject.database.gen_base_queries import generate_create_table_query
from astroinject.database.dbpool import PostgresConnectionManager

from functools import partial
import multiprocessing

def injection_procedure(filepath, config):
    control.info(f"Injecting table {filepath} into the database")
    table = open_table(filepath)
    if len(table) == 0:
        control.warn(f"Table {filepath} is empty. Skipping...")
        return
    
    table = preprocess_table(table, config)
    records = convert_table_to_postgres_records(table)
    
    pg_conn = PostgresConnectionManager(use_pool=False, **config["database"])
    pg_conn.insert_data_copy(config["tablename"], table.columns, records)
    pg_conn.close()
    
def create_table(filepath, config):
    table = open_table(filepath)
    table = preprocess_table(table, config)
    
    create_query = generate_create_table_query(config["tablename"], table, config["id_col"])
    control.info(f"creating table {config['tablename']} in the database")
    control.info(f"query: \n{create_query}")
    
    pg_conn = PostgresConnectionManager(use_pool=False, **config["database"])
    pg_conn.execute_query(create_query)
    pg_conn.close()
    
def parallel_insertion(files, config):
    """
    Uses multiprocessing to insert data in parallel.
    - num_workers: Adjust based on CPU cores (avoid overloading DB)
    """
    # Create the table before inserting (using the first file)
    create_table(files[0], config)

    # Use multiprocessing to insert in parallel
    with multiprocessing.Pool(processes=config["general"]["injection_processes"]) as pool:
        pool.map(partial(injection_procedure, config=config), files)
    
    control.info("✅ All files inserted in parallel!")