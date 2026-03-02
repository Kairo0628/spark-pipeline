from airflow.sdk import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

from datetime import datetime

with DAG(
    dag_id = 'start_spark_cluster_dag',
    description = 'Start Spark Cluster',
    start_date = datetime(2026, 3, 1),
    schedule = '0 9 * * *', # 매일 UTC: 00:00, KST: 09:00
    tags = ['spark']
) as dag:

    t1 = SSHOperator(
        task_id = 'start_spark_cluster',
        ssh_conn_id = 'ssh_conn_id',
        command = ' echo "Start Spark Cluster..." && /opt/spark/sbin/start-all.sh && echo "Spark Cluster Started!" '
    )

    t1
