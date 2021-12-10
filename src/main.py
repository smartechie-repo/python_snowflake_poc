import json
from utils.helper import init_environment
from connectors.snowflaker import SnowflakeConnector
from connectors.alchemic import sqlalchemy_snowflake_connector, sqlalchemy_snowflake_connector_with

def load_using_snowflake_sqlalchemy(cred: dict, metadata: dict):

    test_table_name = metadata['tables'][0]

    engine = sqlalchemy_snowflake_connector_with(cred=cred, warehouse=metadata["warehouse"], \
        database=metadata["database"], schema=metadata["schema"], role="sysadmin")
    connection = engine.connect()

    try:
        # Read the table
        result = connection.execute(f"SELECT * FROM {test_table_name}")
        for row in result:
            print(row)
    finally:
        connection.close()
        engine.dispose()

def load_using_snowflake_connector(cred: dict, metadata: dict):
    conn = SnowflakeConnector.get_instance(cred)

    """
    You can also set session parameters by executing the SQL statement ALTER SESSION SET ... after connecting:
    con.cursor().execute("ALTER SESSION SET QUERY_TAG = '<some tag name>'")

    """
    data = {}
    i = 0
    data.update({i: "Connection Close script end..."})
    i += 1

    print("success in connecting")
    conn.cursor().execute("use role sysadmin")

    test_table_name = metadata['tables'][0]

    init_environment(conn=conn, warehouse=metadata["warehouse"], database=metadata["database"], \
        schema=metadata["schema"], tables_info=metadata["meta_info"])

    # Creating Tables and Inserting Data
    conn.cursor().execute(
        f'''
        insert into {test_table_name} 
            values ('T000011', 'srp', 'SFO', '94016', CURRENT_DATE(), CURRENT_DATE())
        ''')


    # Putting Data
    conn.cursor().execute(f'PUT file://./data/speakers* @{metadata["database"]}.{metadata["schema"]}.%{test_table_name}')


    conn.cursor().execute(f"COPY INTO {test_table_name} FROM @{metadata['database']}.{metadata['schema']}.%{test_table_name}/speakers.csv.gz \
                        file_format = (type = csv field_delimiter=',')  pattern = '.*.csv.gz' on_error= 'skip_file'")

    # For S3

    # Copying Data
    # con.cursor().execute("""
    # COPY INTO testtable FROM s3://<your_s3_bucket>/data/
    #     CREDENTIALS = (
    #         aws_key_id='{aws_access_key_id}',
    #         aws_secret_key='{aws_secret_access_key}')
    #     FILE_FORMAT=(field_delimiter=',')
    # """.format(
    #     aws_access_key_id=AWS_ACCESS_KEY_ID,
    #     aws_secret_access_key=AWS_SECRET_ACCESS_KEY))


    # Querying Data
    cur = conn.cursor()

    try:
        cur.execute(f"SELECT TENANT_ID, TENANT_NAME, CITY, ZIP, CREATED_DATE, UPDATED_DATE FROM {test_table_name} ORDER BY TENANT_ID")
        for (TENANT_ID, TENANT_NAME, CITY, ZIP, CREATED_DATE, UPDATED_DATE) in cur:
            print('{0}, {1}, {2}, {3}, {4}, {5}'.format(TENANT_ID, TENANT_NAME, CITY, ZIP, CREATED_DATE, UPDATED_DATE))
            data.update({i: '{0}, {1}, {2}, {3}, {4}, {5}\n'.format(TENANT_ID, TENANT_NAME, CITY, ZIP, CREATED_DATE, UPDATED_DATE)})
            i += 1
    finally:
        cur.close()

    # Use fetchone or fetchmany if the result set is too large to fit into memory.

    # results = conn.cursor().execute("SELECT TENANT_ID, TENANT_NAME, CITY, ZIP, CREATED_DATE, UPDATED_DATE FROM {test_table_name}").fetchall()

    # for rec in results:
    #     print('%s, %s, %s, %s, %s, %s' % (rec[0], rec[1], rec[3], rec[4], rec[5], rec[6]))
    #     data.update({i: '%s, %s, %s, %s, %s, %s \n' % (rec[0], rec[1], rec[3], rec[4], rec[5], rec[6])})
    #     i += 1

    # conn.close()

    data.update({i: "Connection Close script end..."})

    with open("out_record_using_connector.json","w") as f1:
        json.dump(data,f1)
    print("Connection Close script end")


def main():
    with open("cred.json","r") as c:
        cred: dict = json.load(c)
    
    with open("metadata.json","r") as m:
        metadata: dict = json.load(m)

    #load_using_snowflake_connector(cred, metadata = metadata)
    load_using_snowflake_sqlalchemy(cred=cred, metadata = metadata)

if __name__ == "__main__":
    main()