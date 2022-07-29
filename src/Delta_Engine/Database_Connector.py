import os
import sys
from sqlalchemy import create_engine
import pyodbc as odbc
import pandas as pd
import warnings
import urllib


warnings.simplefilter(action='ignore', category=UserWarning)
########################## Pandas Settings ################################
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


###########################################################################


class ODBC_Connector():

    def __init__(self, backup_path, server, database, username, password, driver,connection_type=''):

        self.backup_path = backup_path
        self.server   = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        self.connection_type = connection_type

    @property
    def validate_parms(self):
        return {'backup_path': self.backup_path,
                'server': self.server,
                "database": self.database,
                "username": self.username,
                "password": 'value is secret ####',
                "driver": self.driver,
                "conn_type":self.connection_type
                }

    @property
    def connection_str(self):
        return f'DRIVER={self.driver};SERVER={self.server};PORT=1433;DATABASE={self.database};UID={self.username};PWD={self.password};'

    @property
    def url_encoded_connection_str(self):
        return urllib.parse.quote_plus(f'DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password};')

    @property
    def sql_alchemy_connection(self):
        try:
            return create_engine(f'mssql+pyodbc:///?odbc_connect={self.url_encoded_connection_str}')
        except Exception as e:
            print('unable to create sqlalchemy connection engine, error is ', e )

    @property
    def create_connection(self):
        try:
            if self.connection_type != 'Windows':
                self.connection = odbc.connect(
                    f'DRIVER={self.driver};SERVER={self.server};PORT=1433;DATABASE={self.database};UID={self.username};PWD={self.password};')
                return self.connection
            else:
                self.connection = odbc.connect(
                    f'DRIVER={self.driver};SERVER={self.server};PORT=1433;DATABASE={self.database};UID={self.username};PWD={self.password};Trusted_Connection=yes;')
                return self.connection
        except Exception as e:
            raise ValueError("unable to establish connection \n cause by error:",e)
            exit(-1)


    def execute_sql_cursor(self,sql_statement:str)->str:
        try:
            self.sql_statement  = sql_statement
            self.cursor         = self.create_connection.cursor()
            self.cursor.execute(self.sql_statement)
            self.cursor.commit()
            self.create_connection.close()

        except Exception as e:
            raise ValueError("unable to run query \n cause by error:", e)
            exit(-1)

    def execute_sql_pandas(self,sql_statement:str)->pd.DataFrame:
        try:
            self.sql_statement = sql_statement
            self.df = pd.read_sql(f"""{sql_statement}""", con=self.create_connection)
            return self.df
        except Exception as e:
            raise ValueError("unable to run query \n cause by error:", e)
            exit(-1)

    def restore_database(self,database_path):
        self.database_path = database_path
        self.restore_query = []

        for root, dirs, files in os.walk(self.backup_path, topdown=False):
            for name in files:
                fqn = os.path.join(root, name)
                db_name = os.path.basename(fqn).split('.')[0].replace(' ', '_')

                sql = fr"""
                    USE [master]
                    RESTORE DATABASE {db_name} FROM  DISK = N'{fqn}' WITH  FILE = 1,  MOVE N'teekay' TO N'E:\Databases\MSSQL15.AZ_DEV_VESSEL\DATA\{db_name}.mdf',  MOVE N'teekay_log' TO '{self.database_path}\{db_name}_log.ldf',  KEEP_REPLICATION,  NOUNLOAD,  REPLACE,  STATS = 5
                    ;"""
                self.restore_query.append(sql)
        self.restore_query =  " ".join(self.restore_query)
        return self.restore_query


