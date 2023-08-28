import logging
import astroinject.inject.conn as inconn
import astroinject.inject.funcs as funcs

import yaml
import sys
import argparse
import os 



parser = argparse.ArgumentParser(description='Inject data into database')
parser.add_argument('-C' ,'--config', type=str, default=None, help='Configuration file')
parser.add_argument('-dd', '--getconfig', action='store_true', help='Get configuration file example')

logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)

def main():
    args = parser.parse_args()

    if args.getconfig:
        print(""" 
database:
    host: localhost
    database: postgres
    user: postgres
    password: postgres
    schema: astroinject
    tablename: astroinject

operations: [
    {"name", "pattern"}
]

""")
        return

    logging.info("Starting astroinject")

    if args.config is None or not os.path.exists(args.config):
        logging.error("Config file not found")
        sys.exit(1)

    with open(args.config, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    
    conn = inconn.Connection(config["database"])
    conn.connect()
    
    logging.info("Connected to database")
    
    # TODO: handle config file operations
    
    print(config["operations"])
    if not "operations" in config:
        logging.error("No operations found in config file")
        sys.exit(1)

    if not isinstance(config["operations"], list):
        logging.error("Operations must be a list")
        sys.exit(1)

    if len(config["operations"]) == 0:
        logging.error("Operations empty in config file.")
        sys.exit(1)
    
    files = []
    for key, operation in enumerate(config["operations"]):
        
        if operation["name"] == "find_pattern":
            logging.info(f"Searching for pattern {operation['pattern']}")

            filtered_args = funcs.filter_args(funcs.find_files_with_pattern, operation)
            files = funcs.find_files_with_pattern(**filtered_args)
            
            logging.info(f"Found {len(files)} files with pattern {operation['pattern']}")
        
        elif operation["name"] == "insert_files":
        # TODO: 

            if len(files) == 0:
                logging.error("No files found to insert")
                continue

            logging.info(f"Inserting {len(files)} tables into database")

            for key, file in enumerate(files):
                filtered_args = funcs.filter_args(funcs.process_file_to_dataframe, operation)
                df = funcs.process_file_to_dataframe(file, **filtered_args)
                
                if df is False:
                    #TODO: handle error, file could not be opened
                    continue

                res = conn.inject(df)
                
                if res is False:
                    logging.error(f"Error injecting file {file}")
                    continue
                
                if key == 0:
                    logging.info(f"Creating keys on {conn._tablename} {conn._schema}")

                    filtered_args = funcs.filter_args(conn.apply_pkey, operation)
                    conn.apply_pkey(**filtered_args)
                     
                    filtered_args = funcs.filter_args(conn.apply_coords_index, operation)
                    conn.apply_coords_index(**filtered_args)

                    filtered_args = funcs.filter_args(conn.apply_field_index, operation)
                    conn.apply_field_index(**filtered_args)

                logging.info(f"File {os.path.basename(file)} injected successfully")

