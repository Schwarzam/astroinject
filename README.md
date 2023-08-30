# AstroInject

## Description

AstroInject is a Python package designed to manage the injection of astronomical data into a PostgreSQL database. It provides utilities to search for files based on specific patterns, process those files into Pandas DataFrames, and then inject them into a database for further use or analysis.

## Features

- File Search: Search for files in a directory based on specific patterns.
- Data Processing: Convert FITS tables and CSV files to Pandas DataFrames.
- Data Injection: Insert processed DataFrames into a PostgreSQL database.
- Indexing and Key Management: Automatically apply primary keys and indexes to database tables.
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

### Backup and restore

It's possible to create backups with astroinject. 

```bash
astroinject --backup {database} {schema} {outfile}
```

Then to restore:

```bash
astroinject --restore {database} {infile}
```


## Documentation

For detailed documentation, please refer to the `docs/` directory.

## Contributing

We welcome contributions! Please see the `CONTRIBUTING.md` file for guidelines.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

