from pyspark.sql import SparkSession
from pyspark import SparkConf

import argparse
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
from dotenv import load_dotenv

load_dotenv()
PROJECT_ID = os.getenv('PROJECT_ID')

def create_spark_session():
    conf = SparkConf()
    conf.set('spark.app.name', 'Create Dimension Table')

    spark = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
    
    return spark

def save(df, table, is_upsert):
    if is_upsert:
        df.write\
            .format('bigquery')\
            .option('temporaryGcsBucket', 'my-spark-pipeline-bucket')\
            .option('temporaryGcsPath', 'temp')\
            .mode('overwrite')\
            .save(f'{PROJECT_ID}.spark_dataset.staging_{table}')
    else:
        df.write\
            .format('bigquery')\
            .option('temporaryGcsBucket', 'my-spark-pipeline-bucket')\
            .option('temporaryGcsPath', 'temp')\
            .mode('overwrite')\
            .save(f'{PROJECT_ID}.spark_dataset.{table}')

def main(ds, is_upsert):
    spark = create_spark_session()

    parquet_dir = 'gs://my-spark-pipeline-bucket/parquet'
    if ds[-2:] == '05':
        monthly_dt = ds
    elif ds[-2:] < '05':
        curr = datetime.strptime(ds, '%Y-%m-%d')
        monthly_dt = (curr - relativedelta(months = 1)).strftime('%Y-%m-05')
    else:
        monthly_dt = ds[:-2] + '05'

    if is_upsert == 'y':
        is_upsert = True
    else:
        is_upsert = False

    # dim_stop
    stop_master = spark.read.parquet(f'{parquet_dir}/monthly/stop_master')
    stop_master.createOrReplaceTempView('stop_master')

    route_stop_hour_passenger = spark.read.parquet(f'{parquet_dir}/monthly/route_stop_hour_passenger')
    route_stop_hour_passenger.createOrReplaceTempView('route_stop_hour_passenger')
    route_stop_hour_passenger.cache()

    # Surrogate Key: STOP_ID + UPDATED_AT
    # 해당 데이터들은 월간 데이터이므로 5일인 경우에만 수행
    if ds[-2:] == '05':
        dim_stop = spark.sql("""
            WITH route_stop_hour_passenger_drop_dup AS (
                SELECT
                    DISTINCT STOP_ID
                FROM route_stop_hour_passenger
                WHERE dt = :monthly_dt
            )
            SELECT
                MD5(CONCAT(CAST(s.STOP_ID AS STRING), STOP_NM, STOP_ARS_NO)) AS STOP_SK,
                s.STOP_ID,
                s.STOP_NM,
                s.STOP_ARS_NO,
                s.STOP_TYPE,
                s.LAT,
                s.LOT,
                s.BUS_ARVL_INFO_GUIDEM_INSTL,
                s.dt AS START_DATE,
                TO_DATE('9999-12-31', 'yyyy-MM-dd') AS END_DATE,
                TRUE AS IS_CURRENT
            FROM stop_master s
            JOIN route_stop_hour_passenger_drop_dup r
            ON s.STOP_ID = r.STOP_ID
            WHERE s.dt = :monthly_dt
        """,
        args = {'monthly_dt': monthly_dt})

        dim_stop.show(5)
        dim_stop.printSchema()
        save(dim_stop, 'dim_stop', is_upsert)
                
    # dim_route
    route_id = spark.read.parquet(f'{parquet_dir}/monthly/route_id')
    route_id.createOrReplaceTempView('route_id')

    route_stop_total_passenger = spark.read.parquet(f'{parquet_dir}/daily/route_stop_total_passenger')
    route_stop_total_passenger.createOrReplaceTempView('route_stop_total_passenger')
    
    dim_route = spark.sql("""
        WITH route_stop_hour_passenger_drop_dup AS (
            SELECT
                RTE_NM_DETAIL,
                TRFC_MNS_TYPE_CD,
                TRFC_MNS_TYPE_NM
            FROM (
                SELECT
                    RTE_NM_DETAIL,
                    TRFC_MNS_TYPE_CD,
                    TRFC_MNS_TYPE_NM,
                    ROW_NUMBER() OVER(PARTITION BY RTE_NM_DETAIL ORDER BY STOP_SEQ) AS rn
                FROM route_stop_hour_passenger
                WHERE dt = :monthly_dt
            )
            WHERE rn = 1
        ), route_stop_total_passenger_drop_dup AS (
            SELECT
                RTE_ID_8,
                RTE_NM,
                RTE_NM_DETAIL
            FROM (
                SELECT
                    RTE_ID_8,
                    RTE_NM,
                    RTE_NM_DETAIL,
                    ROW_NUMBER() OVER(PARTITION BY RTE_ID_8 ORDER BY STOP_SEQ) AS rn
                FROM route_stop_total_passenger
                WHERE dt = :ds
            )
            WHERE rn = 1
        )
        SELECT
            MD5(CONCAT(CAST(r.RTE_ID AS STRING), CAST(t.RTE_ID_8 AS STRING), t.RTE_NM, t.RTE_NM_DETAIL)) AS RTE_SK,
            r.RTE_ID,
            t.RTE_ID_8,
            r.RTE_NM,
            h.RTE_NM_DETAIL,
            h.TRFC_MNS_TYPE_CD,
            h.TRFC_MNS_TYPE_NM,
            r.dt AS START_DATE,
            TO_DATE('9999-12-31', 'yyyy-MM-dd') AS END_DATE,
            TRUE AS IS_CURRENT
        FROM route_stop_hour_passenger_drop_dup h
        JOIN route_stop_total_passenger_drop_dup t ON h.RTE_NM_DETAIL = t.RTE_NM_DETAIL
        JOIN route_id r ON t.RTE_NM = r.RTE_NM
        WHERE r.dt = :monthly_dt
    """,
    args = {'monthly_dt': monthly_dt,
            'ds': ds})

    dim_route.show(5)
    dim_route.printSchema()
    save(dim_route, 'dim_route', is_upsert)

    route_stop_hour_passenger.unpersist()

    # dim_date
    dim_date = spark.sql("""
        SELECT
            CAST(DATE_FORMAT(CAST(:ds AS DATE), 'yyyyMMdd') AS LONG) AS DATE,
            CAST(:ds AS DATE) AS YMD,
            CAST(YEAR(CAST(:ds AS DATE)) AS LONG) AS YEAR,
            CAST(MONTH(CAST(:ds AS DATE)) AS LONG) AS MONTH,
            CAST(DAY(CAST(:ds AS DATE)) AS LONG) AS DAY,
            CAST(DAYOFWEEK(CAST(:ds AS DATE)) AS LONG) AS DAY_OF_WEEK,
            CASE
                WHEN DAYOFWEEK(CAST(:ds AS DATE)) IN (1, 7) THEN TRUE
                ELSE FALSE
            END AS IS_WEEKEND
    """,
    args = {'ds': ds})

    dim_date.show(5)
    dim_date.printSchema()

    dim_date.write\
        .format('bigquery')\
        .option('temporaryGcsBucket', 'my-spark-pipeline-bucket')\
        .option('temporaryGcsPath', 'temp')\
        .mode('append')\
        .save(f'{PROJECT_ID}.spark_dataset.dim_date')

    spark.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ds',
                        required = True,
                        help = 'Airflow의 Logical Date(ds)')
    parser.add_argument('--is_upsert',
                        required = True,
                        help = 'Upsert 적재 방식을 사용하는지 여부 (y, n)')
    args = parser.parse_args()

    main(args.ds, args.is_upsert)
