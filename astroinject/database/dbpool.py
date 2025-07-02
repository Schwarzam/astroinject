import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
import io
import numpy as np
import csv

from logpool import control

class PostgresConnectionManager:
    """
    PostgreSQL Connection Manager that supports both connection pooling and single connection modes.
    Set `use_pool=True` to use a connection pool or `use_pool=False` to use a single connection.
    """

    def __init__(self, use_pool=True, minconn=1, maxconn=10, **db_params):
        """
        Initialize the connection manager.
        
        :param use_pool: If True, use a connection pool; otherwise use a single connection.
        :param minconn: Minimum connections in pool (if use_pool is True).
        :param maxconn: Maximum connections in pool (if use_pool is True).
        :param db_params: Database connection parameters (e.g. dbname, user, password, host, etc.).
        """
        self.use_pool = use_pool
        if self.use_pool:
            self.pool = psycopg2.pool.ThreadedConnectionPool(minconn, maxconn, **db_params)
        else:
            self.connection = psycopg2.connect(**db_params)
    
    def get_connection(self):
        """Retrieve a connection from the pool or the single connection."""
        if self.use_pool:
            return self.pool.getconn()
        else:
            return self.connection

    def release_connection(self, conn):
        """Release the connection back to the pool if using a pool (no-op for single connection)."""
        if self.use_pool:
            self.pool.putconn(conn)
    
    def close(self):
        """Close all connections. For pool mode, closes all pooled connections. For single connection, closes that one."""
        if self.use_pool:
            self.pool.closeall()
        else:
            self.connection.close()
            
    def execute_query_wt_tblock(self, query, params=None, fetch=False):
        """
        Execute a SQL query outside a transaction block.
        
        :param query: SQL query to execute.
        :param params: Parameters for query (tuple or list).
        :param fetch: If True, fetch and return results.
        :return: Query result if fetch=True, otherwise None.
        """
        conn = self.get_connection()
        # Enable autocommit so that the query runs outside a transaction block.
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    result = cur.fetchall()
                    return result
                # No need to call conn.commit() in autocommit mode.
        except Exception as e:
            # In autocommit mode, rollback is not needed, but you might log the error.
            control.critical(f"query failed: {e}")
        finally:
            # Reset autocommit to False if your application expects that by default.
            conn.autocommit = False
            self.release_connection(conn)
    
    def execute_query(self, query, params=None, fetch=False):
        """
        Execute a SQL query.
        
        :param query: SQL query to execute.
        :param params: Parameters for query (tuple or list).
        :param fetch: If True, fetch and return results.
        :return: Query result if fetch=True, otherwise None.
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    result = cur.fetchall()
                    return result
                conn.commit()
        except Exception as e:
            conn.rollback()
            control.critical(f"query failed: {e}")
        finally:
            self.release_connection(conn)
    
    def format_pg_array_vectorized(self, values):
        """
        ✅ Fully vectorized function to format PostgreSQL arrays.
        
        - Converts lists/NumPy arrays to PostgreSQL `{}` format.
        - Handles None values (single values → NULL, arrays → 'null' inside).
        
        :param values: An array-like object of values.
        :return: A list of formatted values.
        """
        values = np.array(values, dtype=object)  # Convert input to NumPy array

        # 🔹 Check which elements are lists/arrays
        is_list = np.vectorize(lambda x: isinstance(x, (list, np.ndarray)), otypes=[bool])(values)

        def format_array(arr):
            """Format arrays properly, replacing None with NULL inside PostgreSQL arrays."""
            return "{" + ",".join("NULL" if x is None else str(x) for x in arr) + "}"

        if np.any(is_list):  # 🔹 Only apply vectorization if there are list-like elements
            # values[is_list] = np.vectorize(format_array, otypes=[object])(values[is_list])
            values[is_list] = np.vectorize(format_array, otypes=[object])(values[is_list])

        # 🔹 Handle None values for non-array elements
        values[values == None] = None  # ✅ Keeps None as NULL in SQL

        return values.tolist()
    
    def insert_data_copy_w_idhandling(self, table_name, columns, records, id_col):
        """
        Bulk insert data using COPY with temporary table to handle primary key conflicts.
        
        :param table_name: Target table name.
        :param columns: List of column names.
        :param records: List of tuples containing the data.
        :param id_col: The primary key column name (for conflict handling).
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                # Create a temporary table based on the target table
                temp_table = f"{table_name}_temp"
                cur.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {table_name} INCLUDING ALL) ON COMMIT DROP;")
                
                # Format records using the vectorized function
                records_np = np.array(records, dtype=object)
                formatted_records = self.format_pg_array_vectorized(records_np)
                
                # Convert formatted records to CSV-like text in memory
                csv_data = io.StringIO("\n".join(["\t".join(map(str, row)) for row in formatted_records]) + "\n")
                
                # Copy data into the temporary table
                copy_query = f"COPY {temp_table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t', NULL 'None')"
                cur.copy_expert(copy_query, csv_data)
                
                # Merge data from the temporary table into the main table, handling conflicts
                column_list = ", ".join(columns)
                
                conflict_case = f"ON CONFLICT ({id_col}) DO NOTHING" if id_col else ""
                insert_query = f"""
                    INSERT INTO {table_name} ({column_list})
                    SELECT {column_list} FROM {temp_table}
                    {conflict_case};
                """
                cur.execute(insert_query)
                conn.commit()
                print(f"✅ Inserted {len(records)} rows into {table_name} using COPY with vectorized array handling.")
        except Exception as e:
            conn.rollback()
            control.critical(f"COPY insert failed: {e}")
        finally:
            self.release_connection(conn)
    
    def insert_data_copy(self, table_name, columns, records):
        """
        Bulk insert data using COPY without conflict handling for maximum performance.
        
        :param table_name: Target table name.
        :param columns: List of column names.
        :param records: List of tuples containing the data.
        """
        conn = self.get_connection()
        try:
            # Format records using the vectorized function    
            records_np = np.array(records, dtype=object)
            formatted_records = self.format_pg_array_vectorized(records_np)
                
            with conn.cursor() as cur:
                # Convert formatted records to CSV-like text in memory
                csv_data = io.StringIO()
                writer = csv.writer(csv_data, delimiter='\t', lineterminator='\n', quoting=csv.QUOTE_NONE, escapechar='\\')
                writer.writerows(formatted_records)
                csv_data.seek(0)
                
                # Copy data directly into the main table
                copy_query = f"""COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t', NULL '')"""                
                cur.copy_expert(copy_query, csv_data)
                conn.commit()
                print(f"✅ Inserted {len(records)} rows into {table_name} using COPY (no conflict handling).")
        except Exception as e:
            conn.rollback()
            control.critical(f"COPY insert failed: {e}")
        finally:
            self.release_connection(conn)
    
    def insert_data(self, table_name, columns, records, id_col=None):
        """
        Bulk insert data using execute_values(), optionally handling conflicts on a given primary key.
        
        :param table_name: Target table name.
        :param columns: List of column names.
        :param records: List of tuples containing the data.
        :param id_col: The primary key column name for ON CONFLICT handling (if provided).
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