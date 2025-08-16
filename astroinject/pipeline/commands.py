import argparse

from astroinject.config import load_config
from logpool import control
from astroinject.pipeline.apply_index import apply_pgsphere_index, apply_q3c_index, apply_btree_index

def index_schema_pgsphere_command():
    """
    Create / recreate / drop pgsphere GiST indexes across a schema or a single table.

    Examples:
      # create missing pgsphere indexes across a schema
      astroinject index_schema_pgsphere -b base.yaml -s public

      # restrict by name pattern
      astroinject index_schema_pgsphere -b base.yaml -s sky --name_like 'obj_%%'

      # rebuild (drop+create) all pgsphere indexes in schema
      astroinject index_schema_pgsphere -b base.yaml -s cat --recreate

      # only drop all pgsphere indexes (no re-create, no PK)
      astroinject index_schema_pgsphere -b base.yaml -s cat --drop_only

      # operate on a single table
      astroinject index_schema_pgsphere -b base.yaml -s public.mytable --recreate
      astroinject index_schema_pgsphere -b base.yaml -s public.mytable --drop_only
    """
    import sys
    import psycopg2
    from psycopg2 import sql

    parser = argparse.ArgumentParser(
        description="Manage pgsphere GiST indexes (create/recreate/drop) across a schema or a single table.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("-b", "--baseconfig", required=True, help="Base database config file")
    parser.add_argument("-s", "--schema", required=True,
                        help="Schema name OR schema.table to target a single table")
    parser.add_argument("--name_like", help="SQL LIKE filter for table names (ignored if schema.table is used)")
    parser.add_argument("-r", "--ra_candidates",
                        default="ra,ra_deg,raj2000,ra_j2000,alpha,alpha_j2000",
                        help="Candidate names for RA column (comma/semicolon/pipe-separated)")
    parser.add_argument("-d", "--dec_candidates",
                        default="dec,dec_deg,dej2000,dec_j2000,delta,delta_j2000",
                        help="Candidate names for DEC column (comma/semicolon/pipe-separated)")
    parser.add_argument("--include_partitions", action="store_true",
                        help="Also consider partitioned tables (relkind='p').")
    parser.add_argument("--recreate", action="store_true",
                        help="Drop existing pgsphere GiST indexes and recreate them.")
    parser.add_argument("--ensure_pk", action="store_true",
                        help="If table has no PRIMARY KEY but has a NOT NULL 'id' column, make it the PK via a CONCURRENT unique index.")
    parser.add_argument("--drop_only", action="store_true",
                        help="ONLY drop existing pgsphere GiST indexes; do NOT recreate; ignores --ensure_pk.")
    parser.add_argument("--dry_run", action="store_true",
                        help="Print intended actions without executing any DDL.")
    args = parser.parse_args()

    base_config = load_config(args.baseconfig)

    def parse_list(s):
        if not s:
            return []
        for sep in (",", ";", "|"):
            if sep in s:
                return [x.strip().lower() for x in s.split(sep) if x.strip()]
        return [s.strip().lower()]

    ra_list  = parse_list(args.ra_candidates)
    dec_list = parse_list(args.dec_candidates)

    # --- if user provided schema.table, split + lock to single-table mode
    if "." in args.schema:
        target_schema, target_table = args.schema.split(".", 1)
        single_table_mode = True
    else:
        target_schema, target_table = args.schema, None
        single_table_mode = False

    # --- queries ---
    PGS_INDEX_QUERY = """
    SELECT i.relname AS index_name
    FROM   pg_index ix
    JOIN   pg_class  i   ON i.oid = ix.indexrelid
    JOIN   pg_class  t   ON t.oid = ix.indrelid
    JOIN   pg_namespace n ON n.oid = t.relnamespace
    JOIN   pg_am     am  ON am.oid = i.relam
    LEFT JOIN unnest(ix.indclass) WITH ORDINALITY AS ic(opclass_oid, ord) ON TRUE
    LEFT JOIN pg_opclass opc   ON opc.oid = ic.opclass_oid
    WHERE  n.nspname = %s
    AND  t.relname = %s
    AND  am.amname = 'gist'
    AND (
            opc.opcname = 'gist_spoint_ops'                 -- canonical pgsphere opclass
            OR pg_get_indexdef(ix.indexrelid) ILIKE '%%spoint(%%'  -- heuristic fallback
        )
    GROUP BY i.relname, ix.indexrelid
    """

    COLS_QUERY = """
    SELECT attname, attnotnull
    FROM   pg_attribute
    WHERE  attrelid = %s::regclass AND attnum > 0 AND NOT attisdropped
    """

    HAS_PKEY_QUERY = """
    SELECT 1
    FROM   pg_constraint c
    JOIN   pg_class t ON t.oid = c.conrelid
    JOIN   pg_namespace n ON n.oid = t.relnamespace
    WHERE  n.nspname = %s AND t.relname = %s AND c.contype = 'p'
    LIMIT 1
    """

    UNIQUE_IDX_ON_ID_QUERY = """
    SELECT i.relname
    FROM   pg_index ix
    JOIN   pg_class i ON i.oid = ix.indexrelid
    JOIN   pg_class t ON t.oid = ix.indrelid
    JOIN   pg_namespace n ON n.oid = t.relnamespace
    WHERE  n.nspname = %s AND t.relname = %s
      AND  ix.indisunique = true
      AND  ix.indnatts = 1
      AND  (SELECT a.attname FROM pg_attribute a
            WHERE a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)) = 'id'
    LIMIT 1
    """

    def list_pgsphere_indexes(conn, schema, table):
        with conn.cursor() as cur:
            cur.execute(PGS_INDEX_QUERY, (schema, table))
            return [r[0] for r in cur.fetchall()]

    def has_primary_key(conn, schema, table):
        with conn.cursor() as cur:
            cur.execute(HAS_PKEY_QUERY, (schema, table))
            return cur.fetchone() is not None

    def column_map(conn, schema, table):
        regclass = f"{schema}.{table}"
        with conn.cursor() as cur:
            cur.execute(COLS_QUERY, (regclass,))
            # {lower_name: (orig_name, attnotnull_bool)}
            return {row[0].lower(): (row[0], bool(row[1])) for row in cur.fetchall()}

    def find_ra_dec(conn, schema, table):
        cmap = column_map(conn, schema, table)
        ra  = next((cmap[c][0] for c in ra_list  if c in cmap), None)
        dec = next((cmap[c][0] for c in dec_list if c in cmap), None)
        return ra, dec

    def drop_indexes(conn, qualified_index_names):
        from psycopg2 import sql
        for qname in qualified_index_names:
            if "." in qname:
                sch, idx = qname.split(".", 1)
                stmt = sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {}.{}").format(
                    sql.Identifier(sch), sql.Identifier(idx)
                )
            else:
                stmt = sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {}").format(sql.Identifier(qname))
            with conn.cursor() as cur:
                cur.execute(stmt)

    # PK helper (unchanged safety; skipped when --drop_only)
    def ensure_pk_on_id(conn, schema, table):
        if has_primary_key(conn, schema, table):
            return "skip_pkey_exists"
        cmap = column_map(conn, schema, table)
        if "id" not in cmap:
            return "skip_no_safe_id"
        id_orig, id_notnull = cmap["id"]
        if not id_notnull:
            return "skip_no_safe_id"

        with conn.cursor() as cur:
            cur.execute(UNIQUE_IDX_ON_ID_QUERY, (schema, table))
            row = cur.fetchone()
            if row:
                uniq_idx = row[0]
            else:
                uniq_idx = f"ux_{schema}_{table}_id"
                if args.dry_run:
                    control.info(f"[dry-run] CREATE UNIQUE INDEX CONCURRENTLY {schema}.{uniq_idx} ON {schema}.{table} ({id_orig})")
                else:
                    cur.execute(sql.SQL("CREATE UNIQUE INDEX CONCURRENTLY {} ON {}.{} ({})")
                                .format(sql.Identifier(uniq_idx),
                                        sql.Identifier(schema),
                                        sql.Identifier(table),
                                        sql.Identifier(id_orig)))
        pkey_name = f"{table}_pkey"
        if args.dry_run:
            control.info(f"[dry-run] ALTER TABLE {schema}.{table} ADD CONSTRAINT {pkey_name} PRIMARY KEY USING INDEX {uniq_idx}")
            return "would_attach_pk"
        else:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("ALTER TABLE {}.{} ADD CONSTRAINT {} PRIMARY KEY USING INDEX {}")
                            .format(sql.Identifier(schema),
                                    sql.Identifier(table),
                                    sql.Identifier(pkey_name),
                                    sql.Identifier(uniq_idx)))
            return "attached_pk"

    # connect
    cfg = base_config.get("database", base_config)
    if "dsn" in cfg:
        conn = psycopg2.connect(cfg["dsn"])
    else:
        conn = psycopg2.connect(
            host=cfg.get("host"),
            port=cfg.get("port"),
            dbname=cfg.get("dbname"),
            user=cfg.get("user"),
            password=cfg.get("password"),
        )
    conn.autocommit = True

    # Only need extension check if we might CREATE. For drop-only, skip this.
    if not args.drop_only:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_extension WHERE extname IN ('pgsphere','pg_sphere') LIMIT 1")
                if cur.fetchone() is None:
                    control.info("pgsphere extension is not installed. Run: CREATE EXTENSION pgsphere;")
                    conn.close()
                    sys.exit(1)
        except Exception as e:
            control.info(f"Failed to verify pgsphere extension: {e}")
            conn.close()
            sys.exit(1)

    # figure out tables to process
    if single_table_mode:
        tables = [target_table]
    else:
        RELKINDS = ["'r'"]
        if args.include_partitions:
            RELKINDS.append("'p'")
        relkind_clause = ",".join(RELKINDS)
        LIST_TABLES = f"""
        SELECT t.relname
        FROM   pg_class t
        JOIN   pg_namespace n ON n.oid = t.relnamespace
        WHERE  n.nspname = %s
          AND  t.relkind IN ({relkind_clause})
          {{name_filter}}
        ORDER BY t.relname;
        """
        params = [target_schema]
        name_filter_sql = ""
        if args.name_like:
            name_filter_sql = "AND t.relname LIKE %s"
            params.append(args.name_like)
        list_sql = LIST_TABLES.format(name_filter=name_filter_sql)
        try:
            with conn.cursor() as cur:
                cur.execute(list_sql, params)
                tables = [r[0] for r in cur.fetchall()]
        except Exception as e:
            control.info(f"Could not list tables in schema '{target_schema}': {e}")
            conn.close()
            sys.exit(1)

    control.info(
        f"Target: {target_schema}{('.' + target_table) if single_table_mode else ''} ; "
        f"count={len(tables)} ; options: drop_only={args.drop_only}, recreate={args.recreate}, "
        f"ensure_pk={args.ensure_pk and not args.drop_only}, dry_run={args.dry_run}"
    )

    created=dropped=skipped_exists=skipped_missing_cols=pks_attached=pks_skipped=failed=0

    for i, tbl in enumerate(tables, start=1):
        try:
            existing = list_pgsphere_indexes(conn, target_schema, tbl)
            qnames = [f"{target_schema}.{name}" if "." not in name else name for name in existing]

            # drop-only path
            if args.drop_only:
                if existing:
                    if args.dry_run:
                        control.info(f"[dry-run] would drop pgsphere index(es) on {target_schema}.{tbl}: {existing}")
                    else:
                        drop_indexes(conn, qnames)
                        dropped += len(existing)
                    control.info(f"[{i}] {target_schema}.{tbl}: dropped {len(existing)} pgsphere index(es).")
                else:
                    control.info(f"[{i}] {target_schema}.{tbl}: no pgsphere index found — nothing to drop.")
                continue  # skip all other actions

            # optional PK attach (safe path only)
            if args.ensure_pk:
                pk_status = ensure_pk_on_id(conn, target_schema, tbl)
                if pk_status in ("attached_pk", "would_attach_pk"):
                    pks_attached += 1
                    control.info(f"[{i}] {target_schema}.{tbl}: primary key on id attached.")
                else:
                    pks_skipped += 1
                    control.info(f"[{i}] {target_schema}.{tbl}: skipped PK ({pk_status}).")

            # recreate: drop existing first
            if args.recreate and existing:
                if args.dry_run:
                    control.info(f"[dry-run] would drop pgsphere index(es) on {target_schema}.{tbl}: {existing}")
                else:
                    drop_indexes(conn, qnames)
                    dropped += len(existing)
                existing = []

            # already has pgsphere index?
            if existing:
                skipped_exists += 1
                control.info(f"[{i}] {target_schema}.{tbl}: pgsphere index already exists ({existing}) — skipping create.")
                continue

            # create new if we got here
            ra_col, dec_col = find_ra_dec(conn, target_schema, tbl)
            if not (ra_col and dec_col):
                skipped_missing_cols += 1
                control.info(f"[{i}] {target_schema}.{tbl}: could not find RA/DEC columns — skipping.")
                continue

            cfg_to_apply = dict(base_config)
            cfg_to_apply["tablename"] = f"{target_schema}.{tbl}"
            cfg_to_apply["ra_col"] = ra_col
            cfg_to_apply["dec_col"] = dec_col
            cfg_to_apply["additional_btree_index"] = []

            if args.dry_run:
                control.info(f"[dry-run] would create pgsphere GiST index on {target_schema}.{tbl} (ra={ra_col}, dec={dec_col})")
            else:
                control.info(f"[{i}] {target_schema}.{tbl}: creating pgsphere GiST index (ra={ra_col}, dec={dec_col})")
                apply_pgsphere_index(cfg_to_apply)
                created += 1

        except Exception as e:
            failed += 1
            control.info(f"[{i}] {target_schema}.{tbl}: failed with error: {e}")

    control.info(
        f"Finished. Created: {created}, Dropped: {dropped}, "
        f"Skipped (exists): {skipped_exists}, Skipped (no RA/DEC): {skipped_missing_cols}, "
        f"PK attached: {pks_attached}, PK skipped: {pks_skipped}, Failed: {failed}"
    )
    conn.close()