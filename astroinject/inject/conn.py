import psycopg2
import psycopg2.extras
import logging 
import sys
import sqlalchemy

import pandas as pd
from astroinject.inject.funcs import *
from psycopg2 import OperationalError

class Connection:
    """
    Class for handling PostgreSQL database connections and operations.

    Parameters
    ----------
    conndict : dict, optional
        Dictionary containing connection parameters. Default is an empty dictionary.

    Attributes
    ----------
    _database : str
        The database name.
    _user : str
        The username for connecting to the database.
    _password : str
        The password for connecting to the database.
    _tablename : str
        The name of the table to operate on.
    _schema : str
        The schema for the table.
    _host : str
        The host address for connecting to the database.
    _connection : psycopg2 connection object
        The psycopg2 connection object.
    _cursor : psycopg2 cursor object
        The psycopg2 cursor object.

    Methods
    -------
    connect()
        Establishes a connection to the PostgreSQL database.
    _check_connection()
        Checks and establishes a connection to the database if not connected.
    create_schema()
        Creates a new schema in the database if it doesn't exist.
    execute(query)
        Executes a SQL query on the database.
    apply_coords_index(ra_col="RA", dec_col="DEC")
        Applies a coordinates index on the table.
    apply_field_index(field_col="Field")
        Applies a field index on the table.
    apply_pkey(pkey_col="ID")
        Applies a primary key on the table.
    replace_id_pattern(col="ID", pattern=" ", replacement="_")
        Replaces a pattern in the ID column of the table.
    inject(dataframe)
        Injects a Pandas DataFrame into the database table.
    """
    def __init__(self, conndict = {}):
        
        self._database = conndict.get('database', 'postgres')
        self._user = conndict.get('user', 'postgres')
        self._password = conndict.get('password', 'postgres')
        self._tablename = conndict.get('tablename', 'astroinject')
        self._schema = conndict.get('schema', 'astroinject')
        self._host = conndict.get('host', 'localhost')
        
        self._connection = None 
        self._cursor = None

    def connect(self):
        try:
            self._connection = psycopg2.connect(host=self._host, database=self._database, user=self._user, password=self._password)
            self._cursor = self._connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            return self._connection
        except OperationalError as e:
            logging.error(f"{e}")
            sys.exit(1)

    def _check_connection(self):
        self.connect()

    def create_schema(self):
        self._check_connection()
        try:
            self._cursor.execute(f"""CREATE SCHEMA IF NOT EXISTS "{self._schema}" """)
            self._connection.commit()
        except Exception as e:
            logging.error(f"{e}")
            sys.exit(1)

    def execute(self, query):
        self._check_connection()
        try:
            self._cursor.execute(query)
            self._connection.commit()
            return True
        except Exception as e:
            logging.error(f"{e}")
            return False 
    
    def apply_coords_index(self, ra_col="RA", dec_col="DEC"):
        self._check_connection()

        res = self.execute(f"""
            CREATE INDEX "{self._tablename}_coords_idx" ON "{self._schema}"."{self._tablename}" 
            USING gist (public.st_point("{ra_col}", "{dec_col}"))
            TABLESPACE pg_default;
        """)

        if res == False:
            logging.error(f"Failed to create coords index on {self._tablename} {self._schema}")
            

    def apply_field_index(self, field_col = 'Field'):
        self._check_connection()

        res = self.execute(f"""
            CREATE INDEX "{self._tablename}_field" ON "{self._schema}"."{self._tablename}" ("{field_col}");
        """)

        if res == False:
            logging.error(f"Failed to create field index on {self._tablename} {self._schema}")
    
    def apply_pkey(self, pkey_col = 'ID'):
        self._check_connection()

        res = self.execute(f"""
            ALTER TABLE "{self._schema}"."{self._tablename}" ADD PRIMARY KEY ("{pkey_col}");
        """)

        if res == False:
            logging.error(f"Failed to create primary key {self._tablename} {self._schema}")

    
    def replace_id_pattern(self, col = 'ID', pattern = ' ', replacement = '_'):
        self._check_connection()

        res = self.execute(f"""
            UPDATE "{self._schema}"."{self._tablename}" SET "{col}" = replace("ID", '{pattern}', '{replacement}');
        """)

        if res == False:
            logging.error(f"Failed to replace ID pattern {self._tablename} {self._schema}")

    def inject(self, dataframe):
        self._check_connection()
        self.create_schema() 
        
        try:
            engine = sqlalchemy.create_engine(f'postgresql://{self._user}:{self._password}@{self._host}/{self._database}')
            dataframe.to_sql(
                name=self._tablename, 
                schema=self._schema, 
                if_exists='append', 
                index=False,
                con = engine
            )
            return True
        except Exception:
            logging.debug(f"Error injecting table. ")
            return False

       

    def map_table(self):
        self._check_connection()
 
        res = self._cursor.execute(f"""select *
               from information_schema.columns
               where table_schema NOT IN ('information_schema', 'pg_catalog')
               order by table_schema, table_name""")
        
        dic = {}
        count = 0
        for row in self._cursor:
            if row['table_name'] == self._tablename:
                typ = row['data_type'].upper()
                
                if 'DOUBLE' in str(typ).upper():
                    count = count + 1
                    dic[row['column_name']] = 'DOUBLE'
                    
                elif 'REAL' in str(typ).upper() or 'FLOAT' in str(typ).upper():
                    count = count + 1
                    dic[row['column_name']] = 'REAL'
                    
                elif 'SMALLINT' in str(typ).upper() or 'SHORT' in str(typ).upper():
                    count = count + 1
                    dic[row['column_name']] = 'SMALLINT'
                
                elif 'INTEGER' in str(typ).upper():
                    count = count + 1
                    dic[row['column_name']] = 'INTEGER'
                    
                elif 'LONG' in str(typ).upper() or 'BIGINT' in str(typ).upper():
                    count = count + 1
                    dic[row['column_name']] = 'BIGINT'
                
                elif 'INT' in str(typ).upper():
                    count = count + 1
                    dic[row['column_name']] = 'BIGINT'
                    
                elif 'CHAR' in str(typ).upper() or 'TEXT' in str(typ).upper():
                    count = count + 1
                    dic[row['column_name']] = 'VARCHAR'
                
                else:
                    logging.error('Did not found type representation for: ', typ, ' on column: ', row['column_name'])

        ##Insert Columns
        querycols = []
        for column in dic:
            if column in dic:
                if column == 'ID' or column == 'RA' or column == 'DEC':
                    principal = 1
                    indexed = 0
                    query = f"""INSERT INTO "TAP_SCHEMA"."columns" VALUES ('{self._tablename}', '{column}', 'description', NULL, NULL, NULL, '{dic[column]}', -1, {principal}, {indexed}, 1, NULL);"""
                    querycols.append(query)
                else:
                    principal = 0
                    indexed = 0
                    query = f"""INSERT INTO "TAP_SCHEMA"."columns" VALUES ('{self._tablename}', '{column}', 'description', NULL, NULL, NULL, '{dic[column]}', -1, {principal}, {indexed}, 1, NULL);"""
                    querycols.append(query)

        for com in querycols:
            self.execute(com)

        ##Insert schema 
        try:
            query = f"""INSERT INTO "TAP_SCHEMA"."schemas" VALUES ('{self._schema}', NULL, NULL, NULL);"""
            self.execute(query)
        except:
            logging.error(f'Couldnt insert schema {self._schema}')

        ##Insert Table
        query = f"""INSERT INTO "TAP_SCHEMA"."tables" VALUES ('{self._schema}', '{self._tablename}', 'table', NULL, NULL, NULL);"""
        self.execute(query)

