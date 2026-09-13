# AstroInject

AstroInject loads astronomical catalogues into PostgreSQL. It reads FITS, CSV,
and Parquet files, normalizes column names, creates a table from the input
schema, bulk-loads the rows, and optionally creates primary-key, B-tree, and
spatial indexes.

It is intended for catalogue-sized loads where the source is split across many
files. File insertion can run in parallel; table creation and index creation
run once per target table.

## Install

```bash
pip install astroinject
```

For development from this repository:

```bash
pip install -e .
```

PostgreSQL is required. Install and enable `pgsphere` or `q3c` in the target
database before selecting the respective spatial index type.

## Load a catalogue

AstroInject combines a base database configuration with a table configuration:

```bash
astroinject -b base.yaml -c sources.yaml
```

`base.yaml` holds the connection and worker settings:

```yaml
database:
  host: localhost
  port: 5432
  dbname: astronomy
  user: astroinject
  password: change-me

general:
  injection_processes: 4
  injection_worker_timeout_seconds: 3600
  injection_error_log: injection_error.log
  injection_worker_print_log: false
  maintenance_jobs: 2
```

`sources.yaml` describes one target table:

```yaml
folder: /data/catalogues/sources
pattern: "*.fits"
format: fits                 # optional for FITS, CSV, and Parquet

tablename: public.sources
id_col: id                   # optional; creates the primary key
ra_col: ra
dec_col: dec
index_type: pgsphere         # pgsphere, q3c, or null
additional_btree_index:
  - field

force_cast_correction: false
rename_columns: {}
delete_columns: []
patterns_to_replace: []
mask_value: null

tablespaces:
  table: pg_default
  index: storage
```

The loader accepts FITS, CSV, and Parquet data. It lowercases and sanitizes
column names, applies the optional column transformations above, infers
PostgreSQL types from the input, and uses `COPY` for bulk insertion.

`tablespaces` is optional. When present, `table` is used for table data and
`index` is used for the primary-key, B-tree, pgsphere, and q3c indexes. The
tablespaces must already exist and be usable by the database user.

See [config.examples](config.examples) for more table configurations and
[the notebook example](dev/example.ipynb) for an in-memory one-table load.

## Indexes and tablespaces

Move every existing index belonging to a configured table—including primary-key
and unique indexes—to `tablespaces.index`:

```bash
move_indexes_tablespace -b base.yaml -c sources.yaml --dry-run
move_indexes_tablespace -b base.yaml -c sources.yaml
```

Use `-st public.sources` to select a table explicitly. The command uses
`ALTER INDEX ... SET TABLESPACE`; it does not recreate the indexes.

Create one index manually when needed:

```bash
create_index -b base.yaml -i pgsphere -st public.sources -ra ra -dec dec
create_index -b base.yaml -i btree -st public.sources -c field
```

For schema-wide pgsphere/q3c index management, use `index_schema --help`.

## Production maintenance

Run `VACUUM ANALYZE` across a schema:

```bash
vacuum_schema -b base.yaml -s public --jobs 2 --dry-run
vacuum_schema -b base.yaml -s public --jobs 2
```

Each worker uses its own PostgreSQL session, so separate tables can be
maintained concurrently. `VACUUM ANALYZE` also vacuums the table's indexes and
refreshes planner statistics. Keep `--jobs` modest; it overrides
`general.maintenance_jobs`.

For statistics only:

```bash
vacuum_schema -b base.yaml -s public --analyze-only
```

## Other commands

```bash
create_schema -b base.yaml -s public
map_table -b base.yaml -c sources.yaml
execute_query -b base.yaml -q "SELECT count(*) FROM public.sources"
```

Run any command with `--help` to see its full options.
