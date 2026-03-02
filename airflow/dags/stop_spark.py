from airflow.sdk import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

from datetime import datetime

with DAG(
    dag_id = 'stop_spark_cluster_dag',
    description = 'Stop Spark Cluster',
    start_date = datetime(2026, 2, 28),
    schedule = '0 3 * * *', # 매일 UTC: 03:00, KST: 12:00
    tags = ['spark']
) as dag:

    t1 = SSHOperator(
        task_id = 'stop_spark_cluster',
        ssh_conn_id = 'ssh_conn_id',
        command = ' echo "Stop Spark Cluster..." && /opt/spark/sbin/stop-all.sh && echo "Spark Cluster Stopped!" '
    )

    t1
