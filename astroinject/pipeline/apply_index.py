from astroinject.database.dbpool import PostgresConnectionManager
from astroinject.database.gen_index_queries import make_pg_sphere_index, make_q3c_index
from astroinject.database.gen_base_queries import vacuum_query

from logpool import control 

def apply_index(config):
    index_query = make_pg_sphere_index(config["tablename"], config["ra_col"], config["dec_col"])
    vacuum_q = vacuum_query(config["tablename"])
    
    pg_conn = PostgresConnectionManager(use_pool=False, **config["database"])
    control.info(f"executing:\n{index_query}")
    
    pg_conn.execute_query(index_query)
    
    control.info(f"executing:\n{vacuum_q}")
    pg_conn.execute_query_wt_tblock(vacuum_q)
    
    pg_conn.close()
    control.info("done applying indexes.")
    
def apply_index_q3c(config):
    index_query = make_q3c_index(config["tablename"], config["ra_col"], config["dec_col"])
    vacuum_q = vacuum_query(config["tablename"])
    
    pg_conn = PostgresConnectionManager(use_pool=False, **config["database"])
    control.info(f"executing:\n{index_query}")
    
    pg_conn.execute_query(index_query)
    
    control.info(f"executing:\n{vacuum_q}")
    pg_conn.execute_query_wt_tblock(vacuum_q)
    
    pg_conn.close()
    control.info("done applying indexes.")