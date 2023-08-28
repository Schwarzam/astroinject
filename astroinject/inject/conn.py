import psycopg2
import psycopg2.extras
import logging 
import sys

from psycopg2 import OperationalError

class Connection:
    def __init__(self, conndict = {}):

        self._database = conndict.get('database', 'postgres')
        self._login = conndict.get('login', 'postgres')
        self._password = conndict.get('password', 'postgres')
        self._tablename = conndict.get('tablename', 'astroinject')
        self._schema = conndict.get('schema', 'astroinject')
        self._host = conndict.get('host', 'localhost')
        
        self._connection = None 
        self._cursor = None
        logging.info(f"Connection to {self._host} {self._database} {self._login} {self._password} {self._tablename} {self._schema}")        

    def connect(self):
        try:
            self._connection = psycopg2.connect(host=self._host, database=self._database, user=self._login, password=self._password)
            self._cursor = self._connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            return self._connection
        except OperationalError as e:
            logging.error(f"{e}")
            sys.exit(1)

    def create_schema(self):
        self._cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
        self._connection.commit()

def __main__():
    conn = Connection()

    conn.connect()
    conn.create_schema()
