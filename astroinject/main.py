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
parser.add_argument('-u', '--user', type=str, default=None, help='Database user')
parser.add_argument('-p', '--password', type=str, default=None, help='Database password')

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
    
    if args.user is not None:
        config["database"]["user"] = args.user
    if args.password is not None:
        config["database"]["password"] = args.password

    conn = inconn.Connection(config["database"])
    conn.connect()
    
    logging.info("Connected to database")
    
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

            if 'filters' in operation:
                files = {}
                for filter in operation['filters']:
                    filtered_args = funcs.filter_args(funcs.find_files_with_pattern, operation)
                    filtered_args['pattern'] = filtered_args['pattern'].format(filter = filter)
                    logging.info(f"Searching for pattern {filtered_args['pattern']}")
                    files[filter] = funcs.find_files_with_pattern(**filtered_args)
                    logging.info(f"Found {len(files[filter])} files with pattern {operation['pattern']}")

            else:
                filtered_args = funcs.filter_args(funcs.find_files_with_pattern, operation)

                logging.info(f"Searching for pattern {filtered_args['pattern']}")
                files = funcs.find_files_with_pattern(**filtered_args)
            
                logging.info(f"Found {len(files)} files with pattern {operation['pattern']}")
        
        elif operation["name"] == "insert_files":
            if isinstance(files, dict):
                for filter in files:
                    if len(files[filter]) == 0:
                        logging.error(f"No files found for filter {filter}")
                        continue

                    logging.info(f"Inserting {len(files[filter])} tables into database for filter {filter}")
                    
                    if "{filter}" in config["database"]["tablename"]:
                        conn._tablename = config["database"]["tablename"].format(filter = operation['filters_names'][filter].lower())
                    
                    funcs.inject_files_procedure(files[filter], conn, operation, config)

            else:
                if len(files) == 0:
                    logging.error("No files found to insert")
                    continue

                logging.info(f"Inserting {len(files)} tables into database")
                funcs.inject_files_procedure(files, conn, operation)
