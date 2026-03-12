from airflow.sdk import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

from datetime import datetime

with DAG(
    dag_id = 'create_dim_table_dag',
    description = 'Create Dimension Table In BigQuery',
    start_date = datetime(2026, 2, 28),
    schedule = '30 6 1 * *', # 매월 1일. UTC: 06:30, KST: 15:30
    tags = ['Monthly', 'BigQuery']
) as dag:
    
    t1 = SSHOperator(
        task_id = 'parquet_to_dim_table',
        ssh_conn_id = 'ssh_conn_id',
        cmd_timeout = None,
        command = """
            /opt/spark/bin/spark-submit \
            --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0 \
            /opt/spark/scripts/create_dim_table.py \
            --ds {{ ds }}
        """
    )

    t1
