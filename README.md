# AstroInject

## Description

AstroInject is a Python package designed to manage the injection of astronomical data into a PostgreSQL database with proper pgsphere coordinates indexes. It provides utilities to search for files based on specific patterns, process those files into Pandas DataFrames, and then inject them into a database for further use or analysis.

## Features

- File Search: Search for files in a directory based on specific patterns.
- Data Processing: Convert FITS tables and CSV files to Pandas DataFrames.
- Data Injection: Insert processed DataFrames into a PostgreSQL database.
- Indexing and Key Management: Automatically apply primary keys and indexes (including pgsphere) to database tables.
- Configuration: Easy configuration via YAML files and command-line options.

## Installation

You can install AstroInject using pip:

```bash
pip install astroinject
```

## Usage

### Command-line Interface

```bash
# Inject data into a database using a configuration file
python -m astroinject --config=config.yaml

# Print an example configuration file
python -m astroinject --getconfig
```

### Configuration File Example

```yaml
database:
  host: localhost
  database: postgres
  user: postgres
  password: postgres
  schema: astroinject
  tablename: astroinject

operations: [
  {"name": "find_pattern", "pattern": "*.fits"},
  {"name": "insert_files"}
]
```

### API

```python
from astroinject.inject import conn, funcs

# Establish a database connection
connection = conn.Connection({
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postgres'
})
connection.connect()

# Find files based on a pattern
files = funcs.find_files_with_pattern("/path/to/files", "*.fits")

# Process and inject files
funcs.inject_files_procedure(files, connection, {"_format": "fits"}, config_object)
```

### Making insertions from the command line

```bash
astroinject -u {user} -p {password} -C {config_file}
```

There are some examples of config files in the `config.examples/` directory.

### Backup and restore

It's possible to create backups with astroinject. 

```bash
astroinject -u {user} -p {password} --backup {database} {schema} {outfile}
```

Then to restore:

```bash
astroinject -u {user} -p {password} --restore {database} {infile}
```
