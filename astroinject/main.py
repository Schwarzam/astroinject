import logging
import astroinject.inject.conn as inconn
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

    for operation in config["operations"]:
        print(operation)

