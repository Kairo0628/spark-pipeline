from airflow.sdk import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from datetime import datetime

with DAG(
    dag_id = 'create_dim_table_dag',
    description = 'Create Dimension Table In BigQuery',
    start_date = datetime(2026, 2, 28),
    schedule = '30 6 1 * *', # 매월 1일. UTC: 06:30, KST: 15:30
    tags = ['Monthly', 'BigQuery']
) as dag:
    
    parquet_to_dim_table = SSHOperator(
        task_id = 'parquet_to_dim_table',
        ssh_conn_id = 'ssh_conn_id',
        cmd_timeout = None,
        command = """
            /opt/spark/bin/spark-submit \
            /opt/spark/scripts/create_dim_table.py \
            --ds {{ ds }}
        """
    )

    #create_dim_stop_dong_table = SSHOperator(
    #    task_id = 'dim_table_to_stop_dong_table',
    #    ssh_conn_id = 'ssh_conn_id',
    #    cmd_timeout = 'None',
    #    command = """
    #        /opt/spark/bin/spark-submit \
    #        /opt/spark/scripts/create_dim_stop_dong.py
    #    """
    #)

    create_dim_stop_dong_table = BigQueryInsertJobOperator(
        task_id = 'dim_table_to_stop_dong_table',
        gcp_conn_id = 'gcp_conn_id',
        configuration = {
            'query': {
                'query': """
                    WITH temp_stop AS (
                        SELECT *
                        FROM (
                            SELECT STOP_ID, STOP_NM, STOP_TYPE, LOT, LAT, UPDATED_AT,
                                ROW_NUMBER() OVER(PARTITION BY STOP_ID ORDER BY UPDATED_AT DESC) AS ROW_NUM
                            FROM `data-engineering-478006.spark_dataset.dim_bus_stop`
                        )
                        WHERE ROW_NUM = 1
                    ), temp_dong AS (
                        SELECT *
                        FROM (
                            SELECT DONG_ID, SGG_NM, DONG_NM, GEOMETRY,
                                ROW_NUMBER() OVER(PARTITION BY DONG_ID ORDER BY UPDATED_AT DESC) AS ROW_NUM
                            FROM `data-engineering-478006.spark_dataset.dim_dong`
                        )
                        WHERE ROW_NUM = 1
                    )
                    SELECT
                        s.STOP_ID, s.STOP_NM, s.STOP_TYPE,
                        d.DONG_ID, d.SGG_NM, d.DONG_NM,
                        s.UPDATED_AT
                    FROM temp_stop s
                    JOIN temp_dong d
                    ON ST_CONTAINS(d.GEOMETRY, ST_GEOGPOINT(s.LOT, s.LAT))
                """,
                'destinationTable': {
                    'projectId': 'data-engineering-478006',
                    'datasetId': 'spark_dataset',
                    'tableId': 'dim_stop_dong'
                },
                'writeDisposition': 'WRITE_TRUNCATE',
                'useLegacySql': False,
            }
        }
    )

    parquet_to_dim_table >> create_dim_stop_dong_table
