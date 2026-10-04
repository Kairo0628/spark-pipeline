from airflow.sdk import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

from datetime import datetime

with DAG(
    dag_id = 'create_fact_table_dag',
    description = 'Create Fact Table In BigQuery',
    start_date = datetime(2026, 2, 28),
    schedule = '40 6 * * *', # 매일. UTC: 06:40, KST: 15:40
    tags = ['Daily', 'BigQuery', 'Fact']
) as dag:
    
    create_fact_table = SSHOperator(
        task_id = 'parquet_to_fact_table',
        ssh_conn_id = 'ssh_conn_id',
        cmd_timeout = None,
        command = """
            /opt/spark/bin/spark-submit \
            /opt/spark/scripts/create_fact_table_spark.py \
            --ds {{ ds }}
        """
    )

    create_fact_table
