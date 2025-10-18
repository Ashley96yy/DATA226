from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

with DAG(
    dag_id='SessionToSnowflake',
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,
    catchup=False,
    tags=['snowflake', 'etl']
) as dag:

    # Step 1: Create the stage in Snowflake
    set_stage = SnowflakeOperator(
        task_id='set_stage',
        sql="""
            CREATE OR REPLACE STAGE raw.blob_stage
            url = 's3://s3-geospatial/readonly/'
            file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
        """,
        snowflake_conn_id='snowflake_conn'
    )

    # Step 2: Load data into Snowflake tables
    load = SnowflakeOperator(
        task_id='load',
        sql=[
            """CREATE TABLE IF NOT EXISTS raw.user_session_channel (
                userId int not NULL,
                sessionId varchar(32) primary key,
                channel varchar(32) default 'direct'
            );""",
            """CREATE TABLE IF NOT EXISTS raw.session_timestamp (
                sessionId varchar(32) primary key,
                ts timestamp
            );""",
            """COPY INTO raw.user_session_channel
               FROM @raw.blob_stage/user_session_channel.csv;""",
            """COPY INTO raw.session_timestamp
               FROM @raw.blob_stage/session_timestamp.csv;"""
        ],
        snowflake_conn_id='snowflake_conn'
    )

    # Task dependency
    set_stage >> load
