import logging
import astroinject.inject.conn as inconn

logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)

def main():
    logging.info("Starting astroinject")
    
    conn = inconn.Connection()
    conn.connect()
    
    logging.info("Connected to database")
    conn.create_schema()
    
