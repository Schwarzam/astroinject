import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values

class PostgresConnectionPool:
    """PostgreSQL Connection Pool Manager with query execution and bulk inserts."""

    def __init__(self, minconn=1, maxconn=10, **db_params):
        """
        Initialize the PostgreSQL connection pool.
        :param minconn: Minimum connections in pool.
        :param maxconn: Maximum connections in pool.
        :param db_params: Database connection parameters (dbname, user, password, host).
        """
        self.pool = psycopg2.pool.ThreadedConnectionPool(minconn, maxconn, **db_params)
    
    def get_connection(self):
        """Retrieve a connection from the pool."""
        return self.pool.getconn()

    def release_connection(self, conn):
        """Return a connection to the pool."""
        self.pool.putconn(conn)

    def close_pool(self):
        """Close all connections in the pool."""
        self.pool.closeall()

    def execute_query(self, query, params=None, fetch=False):
        """
        Execute a SQL query.
        :param query: SQL query to execute.
        :param params: Parameters for query (tuple or list).
        :param fetch: If True, fetch results.
        :return: Query result if fetch=True, otherwise None.
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    return cur.fetchall()
                conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"❌ Query failed: {e}")
        finally:
            self.release_connection(conn)

    def insert_data(self, table_name, columns, records, id_col=None):
        """
        Bulk insert data into PostgreSQL using `execute_values()`, handling conflicts.
        :param table_name: Target table.
        :param columns: Column names.
        :param records: List of tuples (data).
        :param id_col: Primary key column (for ON CONFLICT handling).
        """
        conn = self.get_connection()
        try:
            columns_str = ", ".join(columns)
            on_conflict_clause = f"ON CONFLICT ({id_col}) DO NOTHING" if id_col else ""

            query = f"""
                INSERT INTO {table_name} ({columns_str})
                VALUES %s
                {on_conflict_clause};
            """
            
            print(query)
            with conn.cursor() as cur:
                execute_values(cur, query, records, page_size=1000)
                conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"❌ Insert failed: {e}")
        finally:
            self.release_connection(conn)