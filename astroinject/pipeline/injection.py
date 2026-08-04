import logpool as control
from astroinject.io import open_table
from astroinject.processing import preprocess_table
from astroinject.database.utils import convert_table_to_postgres_records
from astroinject.database.gen_base_queries import generate_create_table_query
from astroinject.database.dbpool import PostgresConnectionManager
from astroinject.database.types import build_type_map

from multiprocessing import get_context
import gc
import time

def injection_procedure(filepath, types_map, config):
    pg_conn = None
    records = None
    table = None
    try:
        
        if isinstance(filepath, str):
            control.info(f"Reading table {filepath}")
            table = open_table(filepath, config)
        else:
            table = filepath
            filepath = "Memory file."
        
        control.info(f"Injecting table {filepath} into the database")

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
            
            pg_conn = PostgresConnectionManager(use_pool=False, **config["database"])
            try:
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
                    
                    try: del table
                    except: pass
                    try: del existing_ids
                    except: pass
                    
                    gc.collect()
                    return
            finally:
                pg_conn.close()
                pg_conn = None
        
        control.info(f"Preprocessing table {filepath}")
        table = preprocess_table(table, config, types_map)
        control.info(f"Converting table {filepath} to PostgreSQL records")
        records = convert_table_to_postgres_records(table)

        pg_conn = PostgresConnectionManager(use_pool=False, **config["database"])
        control.info(f"Copying table {filepath} into {config['tablename']}")
        pg_conn.insert_data_copy(config["tablename"], table.columns, records)
        pg_conn.close()
        pg_conn = None
        control.info(f"Finished injecting table {filepath}")

    except Exception as e:
        control.critical(f"Error while injecting {filepath}: {e}")
        raise

    finally:
        # Libera memória explicitamente
        try: del table
        except: pass
        try: del records
        except: pass
        try: del pg_conn
        except: pass
            
        gc.collect()

    return filepath


def _run_injection_task(args):
    filepath, types_map, config = args
    injection_procedure(filepath, types_map, config)
    return filepath

def create_table(filepath, config):
    """
    Filepath or astropy.table.Table
    """
    if isinstance(filepath, str):
        table = open_table(filepath, config)
    else:
        table = filepath
        filepath = "Memory file."

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

    # Contexto spawn evita fork-related memory leaks. maxtasksperchild keeps
    # FITS/Numpy memory from accumulating in long IDR runs.
    ctx = get_context("spawn")
    total_files = len(args)
    completed_files = 0
    timeout_seconds = config["general"].get("injection_worker_timeout_seconds")
    poll_seconds = config["general"].get("injection_worker_poll_seconds", 5)

    with ctx.Pool(
        processes=config["general"]["injection_processes"],
        maxtasksperchild=config["general"].get("max_tasks_per_child", 1),
    ) as pool:
        pending = {
            pool.apply_async(_run_injection_task, (arg,)): arg[0]
            for arg in args
        }
        last_progress = time.monotonic()

        while pending:
            for result, filepath in list(pending.items()):
                if not result.ready():
                    continue

                result.get()
                completed_files += 1
                last_progress = time.monotonic()
                del pending[result]
                control.info(f"Finished {completed_files}/{total_files}: {filepath}")

            if not pending:
                break

            if timeout_seconds and time.monotonic() - last_progress > timeout_seconds:
                pending_files = ", ".join(map(str, pending.values()))
                pool.terminate()
                raise TimeoutError(
                    "No injection worker finished within "
                    f"{timeout_seconds} seconds. Pending files: {pending_files}"
                )

            time.sleep(poll_seconds)

    control.info("✅ All files inserted in parallel!")
