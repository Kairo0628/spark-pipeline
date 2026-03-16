from airflow.sdk import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

from datetime import datetime

with DAG(
    dag_id = 'monthly_parquet_dag',
    description = 'Monthly Raw Data to Parquet',
    start_date = datetime(2026, 2, 28),
    schedule = '0 6 1 * *', # 매월 1일. UTC: 06:00, KST: 15:00
    tags = ['Monthly', 'parquet']
) as dag:
    
    monthly_parquet_spark = SSHOperator(
        task_id = 'gcs_monthly_raw_to_parquet',
        ssh_conn_id = 'ssh_conn_id',
        cmd_timeout = None,
        command = """
            /opt/spark/bin/spark-submit \
            /opt/spark/scripts/monthly_parquet_spark.py \
            --ds {{ ds }}
        """
    )

    test_monthly_parquet = SSHOperator(
        task_id = 'test_monthly_parquet',
        ssh_conn_id = 'ssh_conn_id',
        cmd_timeout = None,
        command = """
            source venv/bin/activate && \
            cd /opt/spark/pytest && \
            pytest -v --ds {{ ds }} *monthly*.py && \
            deactivate
        """
    )

    monthly_parquet_spark >> test_monthly_parquet
