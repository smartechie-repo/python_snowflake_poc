#from snowflake.connector.connection import SnowflakeConnection

def init_environment(conn ,warehouse: str, database: str, schema: str, tables: list):
  # Creating a Database, Schema, and Warehouse
  #conn.cursor().execute(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse.upper()}")
  conn.cursor().execute(f"USE WAREHOUSE {warehouse.upper()}")

  #conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {database.upper()}}")
  conn.cursor().execute(f"USE DATABASE {database.upper()}")

  conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema.upper()}")
  conn.cursor().execute(f"USE SCHEMA {database.upper()}.{schema.upper()}")

  conn.cursor().execute(
        '''
        CREATE TABLE IF NOT EXISTS DATAVERSE_WHITE_BOARD (
            TENANT_ID     VARCHAR(100),
            TENANT_NAME   VARCHAR(100),
            CITY          VARCHAR(100),
            ZIP           NUMBER(9),
            CREATED_DATE  DATE,
            UPDATED_DATE  DATE
            )
                CLUSTER BY (TENANT_ID)
                COMMENT = 'The DATAVERSE_WHITE_BOARD Dummy table for flow testing by Sudhir';
        ''')

