from airflow.sdk import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

from datetime import datetime

with DAG(
    dag_id = 'start_spark_cluster_dag',
    description = 'Start Spark Cluster',
    start_date = datetime(2026, 2, 28),
    schedule = '0 5 * * *', # 매일 UTC: 05:00, KST: 14:00
    tags = ['spark']
) as dag:

    t1 = SSHOperator(
        task_id = 'start_spark_cluster',
        ssh_conn_id = 'ssh_conn_id',
        command = """
            echo 'Start Spark Cluster...' && \
            /opt/spark/sbin/start-all.sh && \
            echo 'Spark Cluster Started'
        """
    )

    t1
