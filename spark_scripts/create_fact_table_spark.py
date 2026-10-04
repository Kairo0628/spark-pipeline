from pyspark.sql import SparkSession
from pyspark import SparkConf

import argparse
import os
from dotenv import load_dotenv

load_dotenv()
PROJECT_ID = os.getenv('PROJECT_ID')

def create_spark_session():
    conf = SparkConf()
    conf.set('spark.app.name', 'Create Fact Table')

    spark = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
    
    return spark

def main(ds):
    spark = create_spark_session()

    parquet_dir = 'gs://my-spark-pipeline-bucket/parquet'
    bigquery_dir = f'{PROJECT_ID}.spark_dataset'

    # fact_dong_hour_passenger
    dong_hour_passenger = spark.read.parquet(f'{parquet_dir}/daily/dong_hour_passenger')
    dong_hour_passenger.createOrReplaceTempView('dong_hour_passenger')

    psng_cols = []
    opr_cols = []
    for i in range(24):
        n = str(i).zfill(2)
        psng_cols.append(f'BUS_PSNG_{n}')
        opr_cols.append(f's.BUS_OPR_{n}')
    psng_sql = ',\n'.join(psng_cols)
    opr_sql = ',\n'.join(opr_cols)

    fact_dong_hour_passenger = spark.sql(f"""
        SELECT
            CAST(DATE_FORMAT(USE_YMD, 'yyyyMMdd') AS LONG) AS DATE,
            DONG_ID,
            TOTAL_BUS_PSNG,
            {psng_sql}
        FROM dong_hour_passenger
        WHERE dt = :ds
    """,
    args = {'ds': ds})

    fact_dong_hour_passenger.show(5)
    fact_dong_hour_passenger.printSchema()

    fact_dong_hour_passenger.write\
        .format('bigquery')\
        .option('temporaryGcsBucket', 'my-spark-pipeline-bucket')\
        .option('temporaryGcsPath', 'temp')\
        .mode('append')\
        .save(f'{bigquery_dir}.fact_dong_hour_passenger')
    
    # fact_route_stop_passenger_opr
    route_stop_total_passenger = spark.read.parquet(f'{parquet_dir}/daily/route_stop_total_passenger')
    route_stop_total_passenger.createOrReplaceTempView('route_stop_total_passenger')

    dim_route = spark.read.format('bigquery')\
        .load(f'{bigquery_dir}.dim_route')
    dim_route.createOrReplaceTempView('dim_route')

    route_stop_sequence = spark.read.parquet(f'{parquet_dir}/daily/route_stop_sequence')
    route_stop_sequence.createOrReplaceTempView('route_stop_sequence')

    dim_stop = spark.read.format('bigquery')\
        .load(f'{bigquery_dir}.dim_stop')
    dim_stop.createOrReplaceTempView('dim_stop')

    fact_route_stop_passenger_opr = spark.sql(f"""
        WITH temp1 AS (
            SELECT
                t.USE_YMD,
                r.RTE_SK,
                r.RTE_ID,
                t.STOP_ID,
                t.GET_OFF,
                t.GET_ON
            FROM route_stop_total_passenger t
            JOIN dim_route r
            ON t.RTE_NM_DETAIL = r.RTE_NM_DETAIL
            WHERE t.dt = :ds
        ), temp2 AS (
            SELECT
                t.USE_YMD,
                t.RTE_SK,
                t.RTE_ID,
                s.STOP_SK,
                t.STOP_ID,
                t.GET_OFF,
                t.GET_ON
            FROM temp1 t
            JOIN dim_stop s
            ON t.STOP_ID = s.STOP_ID
        )
        SELECT
            CAST(DATE_FORMAT(t.USE_YMD, 'yyyyMMdd') AS LONG) AS DATE,
            t.RTE_SK,
            t.STOP_SK,
            s.STOP_SEQ,
            t.GET_OFF,
            t.GET_ON,
            s.TOTAL_BUS_OPR,
            {opr_sql}
        FROM route_stop_sequence s
        JOIN temp2 t
            ON s.USE_YMD = t.USE_YMD
            AND s.STOP_ID = t.STOP_ID
            AND s.RTE_ID = t.RTE_ID
        WHERE s.dt = :ds
    """,
    args = {'ds': ds})

    fact_route_stop_passenger_opr.show(5)
    fact_route_stop_passenger_opr.printSchema()

    fact_route_stop_passenger_opr.write\
        .format('bigquery')\
        .option('temporaryGcsBucket', 'my-spark-pipeline-bucket')\
        .option('temporaryGcsPath', 'temp')\
        .mode('append')\
        .save(f'{bigquery_dir}.fact_route_stop_passenger_opr')
    
    spark.stop()
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ds',
                        required = True,
                        help = 'Airflow의 Logical Date(ds)')
    args = parser.parse_args()

    main(args.ds)
