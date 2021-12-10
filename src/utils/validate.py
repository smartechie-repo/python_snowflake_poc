import json
from sqlalchemy import create_engine
from urllib.parse import quote

with open("cred.json","r") as f:
  cred = json.load(f)

  engine = create_engine(
    'snowflake://{user}:{password}@{account_identifier}/'.format(
        user=quote(f'{cred["userid"]}'),
        password=quote(f'{cred["password"]}'),
        account_identifier=f'{cred["account"]}',
    )
  )
  try:
    connection = engine.connect()
    results = connection.execute('select current_version()').fetchone()
    print(results[0])
  finally:
    connection.close()
    engine.dispose()