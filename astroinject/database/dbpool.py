import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values

import io

import numpy as np 

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

    def format_pg_array_vectorized(self, values):
        """
        ✅ Fully vectorized function to format PostgreSQL arrays.
        - Converts NumPy arrays & lists to PostgreSQL `{}` format.
        - Handles None values (converts to NULL).
        """
        values = np.array(values, dtype=object)  # Convert input to NumPy array
        
        # 🔹 Convert lists/arrays to PostgreSQL `{}` format
        is_list = np.vectorize(lambda x: isinstance(x, (list, np.ndarray)))(values)
        values[is_list] = np.vectorize(lambda x: "{" + ",".join(map(str, x)) + "}")(values[is_list])

        # 🔹 Handle `None` values (convert to NULL)
        is_none = values == None  # NumPy-safe None check
        values[is_none] = "NULL"

        return values.tolist()

    def insert_data_copy_w_idhandling(self, table_name, columns, records, id_col):
        """
        ✅ Bulk insert data using COPY with vectorized array handling.
        :param table_name: Target table.
        :param columns: Column names.
        :param records: List of tuples (data).
        :param id_col: Primary Key column name (to handle conflicts).
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                # 🔹 Create TEMP table to handle PK conflicts
                temp_table = f"{table_name}_temp"
                cur.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {table_name} INCLUDING ALL) ON COMMIT DROP;")

                # 🔥 Convert records to NumPy & vectorize array formatting
                records_np = np.array(records, dtype=object)
                formatted_records = self.format_pg_array_vectorized(records_np)

                # 🔹 Convert to CSV format in memory
                csv_data = io.StringIO("\n".join(["\t".join(map(str, row)) for row in formatted_records]) + "\n")

                # 🔥 COPY data into the TEMP table
                copy_query = f"COPY {temp_table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t')"
                cur.copy_expert(copy_query, csv_data)

                # 🔹 Merge into the main table, handling conflicts
                column_list = ", ".join(columns)
                insert_query = f"""
                    INSERT INTO {table_name} ({column_list})
                    SELECT {column_list} FROM {temp_table}
                    ON CONFLICT ({id_col}) DO NOTHING;
                """
                cur.execute(insert_query)

                conn.commit()
                print(f"✅ Inserted {len(records)} rows into {table_name} using COPY with vectorized array handling.")

        except Exception as e:
            conn.rollback()
            print(f"❌ COPY insert failed: {e}")
        finally:
            self.release_connection(conn)
            
    def insert_data_copy(self, table_name, columns, records):
        """
        ✅ Bulk insert data using COPY without conflict handling (for max performance).
        :param table_name: Target table.
        :param columns: Column names.
        :param records: List of tuples (data).
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                # 🔥 Convert records to NumPy & vectorize array formatting
                records_np = np.array(records, dtype=object)
                formatted_records = self.format_pg_array_vectorized(records_np)

                # 🔹 Convert to CSV format in memory
                csv_data = io.StringIO("\n".join(["\t".join(map(str, row)) for row in formatted_records]) + "\n")

                # 🔥 COPY data into the main table (⚡ NO Conflict Handling ⚡)
                copy_query = f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t')"
                cur.copy_expert(copy_query, csv_data)

                conn.commit()
                print(f"✅ Inserted {len(records)} rows into {table_name} using COPY (no conflict handling).")

        except Exception as e:
            conn.rollback()
            print(f"❌ COPY insert failed: {e}")
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