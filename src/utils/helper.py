#from snowflake.connector.connection import SnowflakeConnection

def init_environment(conn ,warehouse: str, database: str, schema: str, tables_info: dict[str, str]):
  # Creating a Database, Schema, and Warehouse
  #conn.cursor().execute(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse.upper()}")
  conn.cursor().execute(f"USE WAREHOUSE {warehouse.upper()}")

  #conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {database.upper()}}")
  conn.cursor().execute(f"USE DATABASE {database.upper()}")

  conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema.upper()}")
  conn.cursor().execute(f"USE SCHEMA {database.upper()}.{schema.upper()}")


  for table_def in tables_info:
    conn.cursor().execute(table_def)

