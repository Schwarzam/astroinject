import logpool as control
from astroinject.io import open_table
from astroinject.processing import preprocess_table
from astroinject.database.utils import convert_table_to_postgres_records
from astroinject.database.gen_base_queries import generate_create_table_query
from astroinject.database.dbpool import PostgresConnectionManager
from astroinject.database.types import build_type_map

from multiprocessing import get_context
import gc
import os
from queue import Empty
import time
import traceback

from tqdm.auto import tqdm

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


def _run_injection_process(filepath, types_map, config, result_queue):
    if hasattr(control, "print_log"):
        control.print_log = config["general"].get("injection_worker_print_log", False)

    try:
        injection_procedure(filepath, types_map, config)
        result_queue.put(("success", os.getpid(), filepath, None))
    except Exception:
        result_queue.put(("error", os.getpid(), filepath, traceback.format_exc()))


def _write_injection_error(error_log_path, filepath, message):
    if not error_log_path:
        return

    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(error_log_path, "a", encoding="utf-8") as error_log:
        error_log.write(f"[{timestamp}] {filepath}\n")
        error_log.write(message.rstrip())
        error_log.write("\n\n")

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
    - Starts one process per file so timed-out workers can be logged and skipped
    """
    # Cria a tabela com o primeiro arquivo
    create_table(files[0], config)

    # Gera o types_map se necessário
    types_map = build_type_map(config) if config.get("force_cast_correction") else None

    # Cria lista de argumentos para os workers
    args = [(filepath, types_map, config) for filepath in files]

    # Contexto spawn evita fork-related memory leaks. Cada worker processa
    # somente um arquivo para evitar acúmulo de memória em FITS/Numpy.
    ctx = get_context("spawn")
    total_files = len(args)
    timeout_seconds = config["general"].get("injection_worker_timeout_seconds")
    poll_seconds = config["general"].get("injection_worker_poll_seconds", 5)
    max_processes = config["general"]["injection_processes"]
    error_log_path = config["general"].get("injection_error_log", "injection_error.log")
    result_queue = ctx.Queue()
    pending_args = list(args)
    running = {}
    completed_pids = set()
    error_files = 0

    def start_next_worker():
        if not pending_args:
            return

        filepath, worker_types_map, worker_config = pending_args.pop(0)
        process = ctx.Process(
            target=_run_injection_process,
            args=(filepath, worker_types_map, worker_config, result_queue),
        )
        process.start()
        running[process.pid] = {
            "process": process,
            "filepath": filepath,
            "started_at": time.monotonic(),
        }

    with tqdm(total=total_files, desc="Injecting files", unit="file", dynamic_ncols=True) as progress:
        while pending_args and len(running) < max_processes:
            start_next_worker()

        while running:
            while True:
                try:
                    status, pid, filepath, message = result_queue.get_nowait()
                except Empty:
                    break

                if pid in completed_pids:
                    continue

                completed_pids.add(pid)
                if status == "error":
                    error_files += 1
                    _write_injection_error(error_log_path, filepath, message)

                progress.update(1)

                worker = running.pop(pid, None)
                if worker:
                    worker["process"].join()

                while pending_args and len(running) < max_processes:
                    start_next_worker()

            for pid, worker in list(running.items()):
                process = worker["process"]
                filepath = worker["filepath"]

                if not process.is_alive():
                    process.join()
                    if pid in completed_pids:
                        del running[pid]
                        continue

                    completed_pids.add(pid)
                    if process.exitcode != 0:
                        error_files += 1
                        _write_injection_error(
                            error_log_path,
                            filepath,
                            f"Worker exited with code {process.exitcode} without returning a result.",
                        )
                    progress.update(1)
                    del running[pid]
                    continue

                if timeout_seconds and time.monotonic() - worker["started_at"] > timeout_seconds:
                    process.terminate()
                    process.join()
                    completed_pids.add(pid)
                    error_files += 1
                    _write_injection_error(
                        error_log_path,
                        filepath,
                        f"Worker timed out after {timeout_seconds} seconds.",
                    )
                    progress.update(1)
                    del running[pid]

            while pending_args and len(running) < max_processes:
                start_next_worker()

            if running:
                time.sleep(poll_seconds)

    if error_files:
        control.warn(
            f"Finished parallel insertion with {error_files} errors. "
            f"See {error_log_path} for details."
        )
    else:
        control.info("✅ All files inserted in parallel!")
