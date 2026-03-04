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
    
    t1 = SSHOperator(
        task_id = 'gcs_monthly_raw_to_parquet',
        ssh_conn_id = 'ssh_conn_id',
        cmd_timeout = None,
        command = """
            /opt/spark/bin/spark-submit \
            --master spark://10.128.0.8:7077 \
            --conf 'spark.driver.userClassPathFirst=true' \
            --conf 'spark.executor.userClassPathFirst=true' \
            --packages com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5 \
            /opt/spark/scripts/monthly_parquet_spark.py \
            --ds {{ ds }}
        """
    )

    t1
