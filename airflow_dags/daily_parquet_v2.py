from airflow.sdk import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

from datetime import datetime

with DAG(
    dag_id = 'daily_parquet_dag_v2',
    description = 'Daily Raw Data to Parquet',
    start_date = datetime(2026, 2, 28),
    schedule = '0 6 * * *', # 매일. UTC: 06:00, KST: 15:00
    tags = ['Daily', 'parquet']
) as dag:
    
    daily_parquet_spark = SSHOperator(
        task_id = 'gcs_daily_raw_to_parquet',
        ssh_conn_id = 'ssh_conn_id',
        cmd_timeout = None,
        command = """
            /opt/spark/bin/spark-submit \
            /opt/spark/scripts/daily_parquet_spark.py \
            --ds {{ ds }}
        """
    )

    test_daily_parquet = SSHOperator(
        task_id = 'test_daily_parquet',
        ssh_conn_id = 'ssh_conn_id',
        cmd_timeout = None,
        command = """
            source venv/bin/activate && \
            cd /opt/spark/pytest && \
            pytest -v --ds {{ ds }} *daily*.py && \
            deactivate
        """
    )

    daily_parquet_spark >> test_daily_parquet
