import argparse

from astroinject.pipeline.apply_index import apply_index
from astroinject.pipeline.injection import injection_procedure, create_table, parallel_insertion

from astroinject.utils import find_files_with_pattern
from astroinject.config import load_config

from astroinject.pipeline.map_tap_schema import map_table

import warnings
from logpool import control

warnings.filterwarnings("ignore")

def injection():
    parser = argparse.ArgumentParser(description="Inject data from a CSV file into a database")
    
    parser.add_argument("-b", "--baseconfig", help="Base database config file")
    parser.add_argument("-c", "--tableconfig", help="Table specifig config file")
    args = parser.parse_args()

    config = load_config(args.baseconfig)
    config.update(load_config(args.tableconfig))

    control.info("starting injection procedure")
    control.info(f"config: \n{config}")
    
    files = find_files_with_pattern(
        config["folder"],
        config["pattern"]
    )
    
    control.info(f"found {len(files)} files to inject")
    
    parallel_insertion(files, config)
    apply_index(config)
    
    
def map_table_command():
    parser = argparse.ArgumentParser(description="map a table to the TAP_SCHEMA")
    
    parser.add_argument("-b", "--baseconfig", help="Base database config file")
    parser.add_argument("-c", "--tableconfig", help="Table specifig config file")
    args = parser.parse_args()

    config = load_config(args.baseconfig)
    config.update(load_config(args.tableconfig))
    
    control.info("starting mapping procedure")
    map_table(config)
    control.info("finished mapping procedure")
