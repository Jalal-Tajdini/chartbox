import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from psycopg2 import sql
import psycopg2.extras as extras 
import json


class Controller():
    def __init__(self, db_name:str="postgres"):
        """
        This function initiallizes the database class.
        input: db_name(str)
        """
        db_name = db_name.lower()
        try:
            self.credentials = {
                    'database': db_name,
                    'user': "postgres",
                    'password': "123",
                    'host': "localhost",
                    'port':"5432"
                }
            try:
                # establishing the connection
                self.connection = psycopg2.connect(**self.credentials)
                self.connection.autocommit = True
                self.cursor = self.connection.cursor()
                self.connected_db = db_name
                print(f'Connected to "{db_name}"')
            except Exception as e:
                print(f'Can not connect to the database "{db_name}":\n {e}')
                temp_connection = Controller()
                if temp_connection.db_exists(db_name, close_connection=False):
                    print(f'Database {db_name} exists but conenction to it failed.')
                else:
                    temp_connection.create_new_db(db_name, close_connection=False)
                temp_connection.close_connection()
                self.connection = psycopg2.connect(**self.credentials)
                self.connection.autocommit = True
                self.cursor = self.connection.cursor()
                self.connected_db = db_name

                print(f'Connected to "{db_name}"')
        except Exception as e:
            print(f'Unhandled exception occured in __init__ function of Controller class:\n{e}')
    
    def initialize(self, configuration_file_address:str='config.json', close_connection:bool=True)->bool:
        """
        This function reads necessary data from database to initialize application.
        input: None
        output: success - boolean
        """
        try:
            try:
                with open(configuration_file_address) as config_file:
                    config_data = json.load(config_file)
                last_active_db = config_data['last_active_db']
                db_exists = self.db_exists(last_active_db, close_connection=False)
                if db_exists is None or not db_exists:
                    print(f'last active database "{db_exists}" was not found in existing databases.\n')
                    self.create_new_db(last_active_db, close_connection=False)
                else:
                    print(f'last active database "{last_active_db}" loaded')
                self.current_db = last_active_db
            except:
                i = 0
                while not self.create_new_db(f'default_db{i}', close_connection=False):
                    i += 1
                self.current_db = f'default_db{i}'
                print(('Loading previous database failed.\n'
                        f'A default database with the name "default_db{i}" is created.\n'))
            success = True
        except Exception as e:
            print(f'Initializing database failed: {e}')
            success = False
        finally:
            if close_connection:
                self.close_connection()
            return success
                
    def create_new_db(self, new_db_name:str, close_connection:bool=True)->bool:
        """
        This function creates a new database.
        input: new database name- string
        output: None
        """
        try:
            #Preparing query to create a database
            sql = f"CREATE database {new_db_name}"
            #Creating a database
            self.cursor.execute(sql)
            print(f"Database '{new_db_name}' created successfully")
            Controller(new_db_name).create_initial_tables(new_db_name)

            success = True
        except psycopg2.Error as e:
            print(f"Error creating the database ({new_db_name}): {e}")
            success = False
        finally:
            if close_connection:
                self.close_connection()
            return success

    def create_initial_tables(self, db_name: str, dataframe: pd.DataFrame, close_connection: bool = True) -> bool:
        """
        This function creates tables of a database and initializes it.
        input: db-name : string
               dataframe : pd.DataFrame
        output: success state: boolean
        """
        success = False
        try:
            # Get columns and corresponding data types from the DataFrame
            columns_and_types = [
                f'{col} {self.map_pandas_dtype_to_pg_dtype(dataframe.dtypes[col])}'
                for col in dataframe.columns
            ]

            # Create a table based on DataFrame columns and data types
            create_table_query = sql.SQL('''
                CREATE TABLE IF NOT EXISTS data_table (
                    {}
                )
            ''').format(sql.SQL(', ').join(map(sql.SQL, columns_and_types)))

            self.cursor.execute(create_table_query)
            self.connection.commit()
            print("Initial table created successfully!")
            success = True
        except Exception as e:
            print(f'Cannot create the initial table in database "{db_name}":\n {e}')
        finally:
            if close_connection:
                self.close_connection()
            return success

    def map_pandas_dtype_to_pg_dtype(self, pandas_dtype):
        # Map pandas data types to PostgreSQL data types
        dtype_mapping = {
            'int64': 'INTEGER',
            'float64': 'NUMERIC',
            'object': 'TEXT',  # You can customize this mapping based on your needs
            'datetime64[ns]': 'TIMESTAMP',
            'bool': 'BOOLEAN',
        }
        return dtype_mapping.get(str(pandas_dtype), 'TEXT')  # Default to TEXT for unknown types



    def import_to_db(self, table_name: str, dataframe: pd.DataFrame, close_connection: bool = True) -> bool:
        conn = self.connection
        cursor = self.cursor

        # Convert NumPy types to Python types
        tuples = [tuple(map(lambda x: x.item() if isinstance(x, (pd._libs.tslibs.timestamps.Timestamp, pd._libs.tslibs.nattype.NaTType)) else x, row)) for row in dataframe.to_numpy()]

        cols = ','.join(list(dataframe.columns))
        # SQL query to execute
        query = "INSERT INTO %s(%s) VALUES %%s" % (table_name, cols)
        cursor = conn.cursor()
        try:
            extras.execute_values(cursor, query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("The DataFrame is inserted")
        cursor.close()
    


    def close_connection(self)->bool:
        """
        This function closes the connection.
        output: success - bool
        """
        try:
            self.cursor.close()
            self.connection.close()
            success = True
        except Exception as e:
            print('Could not close the connection:\n', e)
            success = False
        finally:
            return success
        
    def db_exists(self, db_name:str, close_connection=True)->bool:
        """
        This function checks if a certain database exists or not
        """
        try:
            # Check if the database exists
            exists_query = sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s;")
            self.cursor.execute(exists_query, (db_name,))
            database_exists = self.cursor.fetchone()  
        except Exception as e:
            print(f"Error while checking db existance: {e}")
            database_exists = None
        finally:
            if close_connection:
                self.close_connection()
            return database_exists is not None