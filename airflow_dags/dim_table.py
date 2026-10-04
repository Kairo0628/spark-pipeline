from airflow.sdk import DAG, task, Variable, get_current_context
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from datetime import datetime
import logging

@task.branch(task_id = 'check_table_and_branch')
def check_table_and_branch():
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    
    hook = BigQueryHook(gcp_conn_id = 'gcp_conn_id')
    logging.info('Get BigQueryHook')

    logging.info('Checking Table Exists')
    table_exists = hook.table_exists(
        project_id = Variable.get('project_id'),
        dataset_id = 'spark_dataset',
        table_id = 'dim_stop',
    )
    if not table_exists:
        return 'create_dim_table'
    
    logging.info('Create BigQuery Job')
    job = hook.insert_job(
        configuration = {
            'query': {
                'query': 'SELECT COUNT(*) AS cnt FROM spark_dataset.dim_stop'
            }
        },
        project_id = Variable.get('project_id'),
        location = Variable.get('location')
    )

    logging.info('Get Job Result')
    result = hook.get_query_results(
        job_id = job.job_id,
        project_id = Variable.get('project_id'),
        location = Variable.get('location')
    )
    logging.info(f'query result: {result}')

    count = result[0]['cnt']
    logging.info(f'dim_stop Count: {count}')
    if count > 0:
        return 'create_staging_dim_table'
    else:
        return 'create_dim_table'
    
@task.short_circuit(task_id = 'check_logical_date')
def check_logical_date():
    context = get_current_context()
    ds = context['ds']

    if ds[-2:] == '05':
        return True
    else:
        return False

with DAG(
    dag_id = 'create_dim_table_dag',
    description = 'Create Dimension Table In BigQuery',
    start_date = datetime(2026, 2, 28),
    schedule = '30 6 * * *', # 매일. UTC: 06:30, KST: 15:30
    tags = ['Daily', 'BigQuery', 'Dim']
) as dag:
    
    branch = check_table_and_branch()

    create_dim_table = SSHOperator(
        task_id = 'create_dim_table',
        ssh_conn_id = 'ssh_conn_id',
        cmd_timeout = None,
        command = """
            /opt/spark/bin/spark-submit \
            /opt/spark/scripts/create_dim_table_spark.py \
            --ds {{ ds }} --is_upsert n
        """
    )

    create_staging_dim_table = SSHOperator(
        task_id = 'create_staging_dim_table',
        ssh_conn_id = 'ssh_conn_id',
        cmd_timeout = None,
        command = """
            /opt/spark/bin/spark-submit \
            /opt/spark/scripts/create_dim_table_spark.py \
            --ds {{ ds }} --is_upsert y
        """
    )

    check_ds = check_logical_date()

    upsert_dim_stop = BigQueryInsertJobOperator(
        task_id = 'upsert_dim_stop',
        gcp_conn_id = 'gcp_conn_id',
        project_id = '{{ var.value.project_id }}',
        location = '{{ var.value.location }}',
        configuration = {
            'query': {
                'query': """
                    MERGE INTO spark_dataset.dim_stop d
                    USING (
                        SELECT
                            s.STOP_ID AS MERGE_KEY,
                            s.*
                        FROM spark_dataset.staging_dim_stop s
                        
                        UNION ALL
                        
                        SELECT
                            CAST(NULL AS INTEGER) AS MERGE_KEY,
                            s.*
                        FROM spark_dataset.staging_dim_stop s
                        JOIN spark_dataset.dim_stop d
                        ON s.STOP_ID = d.STOP_ID AND d.IS_CURRENT
                        WHERE s.STOP_SK != d.STOP_SK
                    ) s
                    ON d.STOP_ID = s.MERGE_KEY AND d.IS_CURRENT
                    
                    WHEN MATCHED AND d.STOP_SK != s.STOP_SK THEN
                    UPDATE SET
                        IS_CURRENT = FALSE,
                        END_DATE = s.START_DATE
                                    
                    WHEN NOT MATCHED THEN
                    INSERT (STOP_SK, STOP_ID, STOP_NM, STOP_ARS_NO, STOP_TYPE,
                            LAT, LOT, BUS_ARVL_INFO_GUIDEM_INSTL, START_DATE, END_DATE, IS_CURRENT)
                    VALUES (s.STOP_SK, s.STOP_ID, s.STOP_NM, s.STOP_ARS_NO, s.STOP_TYPE,
                            s.LAT, s.LOT, s.BUS_ARVL_INFO_GUIDEM_INSTL, s.START_DATE, s.END_DATE, s.IS_CURRENT)
                        
                    WHEN NOT MATCHED BY SOURCE AND d.IS_CURRENT THEN
                    UPDATE SET
                        IS_CURRENT = FALSE,
                        END_DATE = CAST('{{ ds }}' AS DATE)
                """,
                'useLegacySql': False,
            }
        }
    )

    clean_staging_dim_stop = BigQueryInsertJobOperator(
        task_id = 'clean_staging_dim_stop',
        gcp_conn_id = 'gcp_conn_id',
        project_id = '{{ var.value.project_id }}',
        location = '{{ var.value.location }}',
        configuration = {
            'query': {
                'query': """
                    TRUNCATE TABLE spark_dataset.staging_dim_stop
                """,
                'useLegacySql': False,
            }
        }
    )

    upsert_dim_route = BigQueryInsertJobOperator(
        task_id = 'upsert_dim_route',
        gcp_conn_id = 'gcp_conn_id',
        project_id = '{{ var.value.project_id }}',
        location = '{{ var.value.location }}',
        configuration = {
            'query': {
                'query': """
                    MERGE INTO spark_dataset.dim_route d
                    USING (
                        SELECT
                            s.RTE_ID_8 AS MERGE_KEY,
                            s.*
                        FROM spark_dataset.staging_dim_route s
                        
                        UNION ALL
                        
                        SELECT
                            CAST(NULL AS INTEGER) AS MERGE_KEY,
                            s.*
                        FROM spark_dataset.staging_dim_route s
                        JOIN spark_dataset.dim_route d
                        ON s.RTE_ID_8 = d.RTE_ID_8 AND d.IS_CURRENT
                        WHERE s.RTE_SK != d.RTE_SK
                    ) s
                    ON d.RTE_ID_8 = s.MERGE_KEY AND d.IS_CURRENT
                    
                    WHEN MATCHED AND d.RTE_SK != s.RTE_SK THEN
                    UPDATE SET
                        IS_CURRENT = FALSE,
                        END_DATE = s.START_DATE
                                    
                    WHEN NOT MATCHED THEN
                    INSERT (RTE_SK, RTE_ID, RTE_ID_8, RTE_NM, RTE_NM_DETAIL,
                            TRFC_MNS_TYPE_CD, TRFC_MNS_TYPE_NM, START_DATE, END_DATE, IS_CURRENT)
                    VALUES (s.RTE_SK, s.RTE_ID, s.RTE_ID_8, s.RTE_NM, s.RTE_NM_DETAIL,
                            s.TRFC_MNS_TYPE_CD, s.TRFC_MNS_TYPE_NM, s.START_DATE, s.END_DATE, s.IS_CURRENT)
                        
                    WHEN NOT MATCHED BY SOURCE AND d.IS_CURRENT THEN
                    UPDATE SET
                        IS_CURRENT = FALSE,
                        END_DATE = CAST('{{ ds }}' AS DATE)
                """,
                'useLegacySql': False,
            }
        }
    )

    clean_staging_dim_route = BigQueryInsertJobOperator(
        task_id = 'clean_staging_dim_route',
        gcp_conn_id = 'gcp_conn_id',
        project_id = '{{ var.value.project_id }}',
        location = '{{ var.value.location }}',
        configuration = {
            'query': {
                'query': """
                    TRUNCATE TABLE spark_dataset.staging_dim_route
                """,
                'useLegacySql': False,
            }
        }
    )

    branch >> [create_dim_table, create_staging_dim_table]
    create_staging_dim_table >> check_ds >> upsert_dim_stop >> clean_staging_dim_stop
    create_staging_dim_table >> upsert_dim_route >> clean_staging_dim_route
