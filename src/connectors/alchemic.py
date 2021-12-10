from sqlalchemy.engine.base import Engine
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from urllib.parse import quote
#from multipledispatch import dispatch

#class SqlAlchemyConnector:

#@dispatch(object)
def sqlalchemy_snowflake_connector(cred):
  return create_engine(URL(
    account = quote(f'{cred["account"]}'),
    user = quote(f'{cred["userid"]}'),
    password = quote(f'{cred["password"]}')
))

#@dispatch(object, str, str, str, str)
def sqlalchemy_snowflake_connector_with(cred: dict, warehouse, database, schema, role):
  return create_engine(URL(
    account = quote(f'{cred["account"]}'),
    user = quote(f'{cred["userid"]}'),
    password = quote(f'{cred["password"]}'),
    database = quote(f'{database}'),
    schema = quote(f'{schema}'),
    warehouse = quote(f'{warehouse}'),
    role = quote(f'{role}')
    ))
