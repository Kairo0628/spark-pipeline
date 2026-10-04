from airflow.sdk import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from datetime import datetime

with DAG(
    dag_id = 'ML_pipeline',
    description = 'Create feature_table and Train ML Model',
    start_date = datetime(2026, 2, 28),
    schedule = None,
    tags = ['BigQuery', 'Feature']
) as dag:
    
    staging_stop_dong = BigQueryInsertJobOperator(
        task_id = 'spatial_join_stop_dong',
        gcp_conn_id = 'gcp_conn_id',
        project_id = '{{ var.value.project_id }}',
        location = '{{ var.value.location }}',
        configuration = {
            'query': {
                'query': """
                    CREATE OR REPLACE TABLE spark_dataset.staging_stop_dong AS (
                        SELECT
                            s.STOP_SK,
                            s.STOP_ID,
                            s.STOP_ARS_NO,
                            s.STOP_TYPE,
                            s.LAT,
                            s.LOT,
                            s.BUS_ARVL_INFO_GUIDEM_INSTL,
                            d.DONG_ID,
                            d.DONG_NM,
                            d.SGG_NM
                        FROM spark_dataset.dim_stop s
                        JOIN spark_dataset.dim_dong d
                        ON ST_CONTAINS(d.GEOMETRY, ST_GEOGPOINT(s.LOT, s.LAT))
                        WHERE s.IS_CURRENT
                    )
                """,
                'useLegacySql': False,
            }
        }
    )

    create_feature_table_spark = SSHOperator(
        task_id = 'create_feature_table',
        ssh_conn_id = 'ssh_conn_id',
        cmd_timeout = None,
        command = """
            /opt/spark/bin/spark-submit \
            /opt/spark/scripts/create_feature_table_spark.py
        """
    )

    trunc_staging_stop_dong = BigQueryInsertJobOperator(
        task_id = 'trunc_staging_stop_dong',
        gcp_conn_id = 'gcp_conn_id',
        project_id = '{{ var.value.project_id }}',
        location = '{{ var.value.location }}',
        configuration = {
            'query': {
                'query': """
                    TRUNCATE TABLE spark_dataset.staging_stop_dong
                """,
                'useLegacySql': False,
            }
        }
    )

    off_cols = []
    on_cols = []
    peak_cols = []
    dong_cols = []
    for i in range(24):
        n = str(i).zfill(2)
        off_cols.append(f'MONTHLY_RTE_STOP_{n}_OFF_PSNG')
        on_cols.append(f'MONTHLY_RTE_STOP_{n}_ON_PSNG')

        dong_cols.append(f'DONG_BUS_PSNG_{n}')

        if i in(7, 8, 9, 17, 18, 19):
            peak_cols.append(f'MONTHLY_RTE_STOP_{n}_OFF_PSNG')
            peak_cols.append(f'MONTHLY_RTE_STOP_{n}_ON_PSNG')
        
    all_off_sql = ',\n'.join(off_cols)
    all_on_sql = ',\n'.join(on_cols)
    total_off_sql = ' + '.join(off_cols)
    total_on_sql = ' + '.join(on_cols)
    peak_sql = ' + '.join(peak_cols)
    dong_sql = ',\n'.join(dong_cols)

    generate_features = BigQueryInsertJobOperator(
        task_id = 'generate_features_in_table',
        gcp_conn_id = 'gcp_conn_id',
        project_id = '{{ var.value.project_id }}',
        location = '{{ var.value.location }}',
        configuration = {
            'query': {
                'query': f"""
                    SELECT
                        DATE,
                        RTE_NM_DETAIL,
                        TRFC_MNS_TYPE_NM,
                        STOP_ID,
                        STOP_ARS_NO,
                        STOP_SEQ,
                        STOP_TYPE,
                        BUS_ARVL_INFO_GUIDEM_INSTL,
                        LAT,
                        LOT,
                        DONG_NM,
                        SGG_NM,
                        {all_off_sql},
                        {all_on_sql},
                        DONG_TOTAL_BUS_PSNG,
                        {dong_sql},

                        EXTRACT(DAY FROM PARSE_DATE('%Y%m%d', CAST(DATE AS STRING))) AS DAY,
                        EXTRACT(MONTH FROM PARSE_DATE('%Y%m%d', CAST(DATE AS STRING))) AS MONTH,
                        EXTRACT(DAYOFWEEK FROM PARSE_DATE('%Y%m%d', CAST(DATE AS STRING))) AS DAY_OF_WEEK,
                        CASE
                            WHEN EXTRACT(DAYOFWEEK FROM PARSE_DATE('%Y%m%d', CAST(DATE AS STRING))) IN(1, 7) THEN TRUE
                            ELSE FALSE
                        END AS IS_WEEKEND,
                        {total_off_sql} AS MONTHLY_TOTAL_OFF,
                        {total_on_sql} AS MONTHLY_TOTAL_ON,
                        ({peak_sql}) / ({total_off_sql} + {total_on_sql}) AS MONTHLY_PEAK_PSNG_RATIO,
                        STOP_SEQ / MAX(STOP_SEQ) OVER(PARTITION BY DATE, RTE_NM_DETAIL) AS RTE_STOP_RATIO,
                        CONCAT(STOP_TYPE, '_', BUS_ARVL_INFO_GUIDEM_INSTL) AS STOP_TYPE_INFRA,
                        CONCAT(STOP_TYPE, '_', TRFC_MNS_TYPE_NM) AS STOP_RTE_TYPE,

                        (RTE_STOP_TOTAL_OFF_PSNG + RTE_STOP_TOTAL_ON_PSNG) / RTE_STOP_TOTAL_OPR AS TARGET
                    FROM spark_dataset.feature_route_stop_psng_opr
                    WHERE RTE_STOP_TOTAL_OPR != 0
                """,
                'destinationTable': {
                    'projectId': '{{ var.value.project_id }}',
                    'datasetId': 'spark_dataset',
                    'tableId': 'staging_feature_route_stop_psng_opr'
                },
                'writeDisposition': 'WRITE_TRUNCATE',
                'useLegacySql': False,
            }
        }
    )

    train_model_and_save = SSHOperator(
        task_id = 'train_model_and_save',
        ssh_conn_id = 'ssh_conn_id',
        cmd_timeout = None,
        command = """
            /opt/spark/bin/spark-submit \
            /opt/spark/scripts/ml_pipeline_spark.py
        """
    )

    trunc_staging_features = BigQueryInsertJobOperator(
        task_id = 'trunc_staging_features',
        gcp_conn_id = 'gcp_conn_id',
        project_id = '{{ var.value.project_id }}',
        location = '{{ var.value.location }}',
        configuration = {
            'query': {
                'query': """
                    TRUNCATE TABLE spark_dataset.staging_feature_route_stop_psng_opr
                """,
                'useLegacySql': False,
            }
        }
    )

    staging_stop_dong >> create_feature_table_spark >> trunc_staging_stop_dong \
        >> generate_features >> train_model_and_save >> trunc_staging_features
