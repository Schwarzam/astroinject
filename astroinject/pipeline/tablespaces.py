"""Move the indexes of an existing PostgreSQL table to a tablespace."""

import logpool as control

from astroinject.database.dbpool import PostgresConnectionManager
from astroinject.database.tablespaces import get_tablespace, quote_identifier


_TABLE_INDEXES_QUERY = """
    SELECT index_namespace.nspname, index_class.relname
    FROM pg_index
    JOIN pg_class AS table_class ON table_class.oid = pg_index.indrelid
    JOIN pg_namespace AS table_namespace ON table_namespace.oid = table_class.relnamespace
    JOIN pg_class AS index_class ON index_class.oid = pg_index.indexrelid
    JOIN pg_namespace AS index_namespace ON index_namespace.oid = index_class.relnamespace
    WHERE table_namespace.nspname = %s AND table_class.relname = %s
    ORDER BY index_namespace.nspname, index_class.relname
"""


def split_table_name(table_name):
    """Return a schema/table pair; unqualified names are in ``public``."""
    parts = table_name.split(".")
    if len(parts) == 1:
        return "public", parts[0]
    if len(parts) == 2 and all(parts):
        return parts
    raise ValueError("tablename must be in the form 'table' or 'schema.table'")


def move_table_indexes(config, table_name=None, dry_run=False):
    """Move every index belonging to a table to ``tablespaces.index``.

    This includes primary-key, unique, spatial, and ordinary indexes. PostgreSQL
    tablespaces must already exist and the configured user needs permission to
    create objects in the destination tablespace.
    """
    tablespace = get_tablespace(config, "index")
    if tablespace is None:
        raise ValueError("tablespaces.index must be configured to move indexes")

    schema, table = split_table_name(table_name or config["tablename"])
    pg_conn = PostgresConnectionManager(use_pool=False, **config["database"])
    try:
        indexes = pg_conn.execute_query(
            _TABLE_INDEXES_QUERY, (schema, table), fetch=True
        ) or []
        if not indexes:
            control.warn(f"No indexes found for {schema}.{table}.")
            return []

        statements = []
        for index_schema, index_name in indexes:
            statement = (
                f"ALTER INDEX {quote_identifier(index_schema)}.{quote_identifier(index_name)} "
                f"SET TABLESPACE {quote_identifier(tablespace)};"
            )
            statements.append(statement)
            if dry_run:
                control.info(f"[dry-run] {statement}")
            else:
                control.info(f"executing:\n{statement}")
                pg_conn.execute_query(statement)
        return statements
    finally:
        pg_conn.close()
