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

        table = preprocess_table(table, config, types_map)
        records = convert_table_to_postgres_records(table)

        pg_conn = PostgresConnectionManager(use_pool=False, **config["database"])
        pg_conn.insert_data_copy(config["tablename"], table.columns, records)
        pg_conn.close()

    except Exception as e:
        control.error(f"Error while injecting {filepath}: {e}")

    finally:
        # Libera memória explicitamente
        del table, records, pg_conn
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