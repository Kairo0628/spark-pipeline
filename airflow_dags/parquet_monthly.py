from airflow.sdk import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

from datetime import datetime

with DAG(
    dag_id = 'monthly_parquet_dag',
    description = 'Monthly Raw Data to Parquet',
    start_date = datetime(2026, 2, 28),
    schedule = '0 6 5 * *', # 매월 5일. UTC: 06:00, KST: 15:00
    tags = ['Monthly', 'parquet']
) as dag:
    
    parquet_monthly_spark = SSHOperator(
        task_id = 'gcs_monthly_raw_to_parquet',
        ssh_conn_id = 'ssh_conn_id',
        cmd_timeout = None,
        command = """
            /opt/spark/bin/spark-submit \
            /opt/spark/scripts/parquet_monthly_spark.py \
            --ds {{ ds }}
        """
    )

    test_parquet_monthly = SSHOperator(
        task_id = 'test_parquet_monthly',
        ssh_conn_id = 'ssh_conn_id',
        cmd_timeout = None,
        command = """
            source venv/bin/activate && \
            cd /opt/spark/pytest && \
            pytest -v --ds {{ ds }} *monthly*.py && \
            deactivate
        """
    )

    parquet_monthly_spark >> test_parquet_monthly
