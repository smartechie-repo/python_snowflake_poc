import snowflake.connector
class SnowflakeConnector:
  def get_instance(cred: dict):
    return snowflake.connector.connect(
        user=cred["userid"],
        password=cred["password"],
        account=cred["account"],
        session_parameters={
                "QUERY_TAG": "rr-dev-sudhir",
        }
    )