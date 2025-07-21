from logpool import control
from astroinject.io import open_table
from astroinject.processing import preprocess_table
from astroinject.database.utils import convert_table_to_postgres_records
from astroinject.database.gen_base_queries import generate_create_table_query
from astroinject.database.dbpool import PostgresConnectionManager
from astroinject.database.types import build_type_map

from multiprocessing import get_context
import gc

def injection_procedure(filepath, types_map, config):
    try:
        control.info(f"Injecting table {filepath} into the database")
        table = open_table(filepath, config)

        if len(table) == 0:
            control.warn(f"Table {filepath} is empty. Skipping...")
            return
        
        # check if the first row already exists in the database because of id column
        if "id_col" in config and config["id_col"] is not None:
            id_col = config["id_col"]
            if "rename_columns" in config and config["rename_columns"] is not None:
                for col in config["rename_columns"]:
                    if config["rename_columns"][col] == config["id_col"]:
                        id_col = col
            
            try:
                first_table_id = table[0][id_col.upper()]
            except KeyError:
                first_table_id = table[0][id_col.lower()]
            
            pg_conn = PostgresConnectionManager(**config["database"])
            
            try:
                constrain = f"{config['id_col']} = {int(first_table_id)}"
            except (ValueError, TypeError):
                constrain = f"{config['id_col']} = '{first_table_id}'"
            
            existing_ids = pg_conn.execute_query(f"""
                SELECT {config['id_col']}
                FROM {config['tablename']}
                WHERE {constrain}
            """, fetch=True)
            if existing_ids:
                control.warn(f"Row with ID {first_table_id} already exists in the database. Skipping {filepath}.")
                pg_conn.close()
                
                try: del table
                except: pass
                try: del existing_ids
                except: pass
                try: del pg_conn
                except: pass
                
                gc.collect()
                return
        
        table = preprocess_table(table, config, types_map)
        records = convert_table_to_postgres_records(table)

        pg_conn = PostgresConnectionManager(use_pool=False, **config["database"])
        pg_conn.insert_data_copy(config["tablename"], table.columns, records)
        pg_conn.close()

    except Exception as e:
        control.critical(f"Error while injecting {filepath}: {e}")

    finally:
        # Libera memória explicitamente
        try: del table
        except: pass
        try: del records
        except: pass
        try: del pg_conn
        except: pass
            
        gc.collect()

def create_table(filepath, config):
    table = open_table(filepath, config)
    table = preprocess_table(table, config)

    create_query = generate_create_table_query(config["tablename"], table, config["id_col"])
    control.info(f"Creating table {config['tablename']} in the database")
    control.info(f"Query: \n{create_query}")

    pg_conn = PostgresConnectionManager(use_pool=False, **config["database"])
    pg_conn.execute_query(create_query)
    pg_conn.close()

def parallel_insertion(files, config):
    """
    Uses multiprocessing to insert data in parallel.
    - Uses `spawn` context to avoid memory leaks from fork
    - Uses starmap instead of partial to pass arguments cleanly
    """
    # Cria a tabela com o primeiro arquivo
    create_table(files[0], config)

    # Gera o types_map se necessário
    types_map = build_type_map(config) if config.get("force_cast_correction") else None

    # Cria lista de argumentos para starmap
    args = [(filepath, types_map, config) for filepath in files]

    # Contexto spawn evita fork-related memory leaks
    ctx = get_context("spawn")
    with ctx.Pool(processes=config["general"]["injection_processes"]) as pool:
        pool.starmap(injection_procedure, args)

    control.info("✅ All files inserted in parallel!")