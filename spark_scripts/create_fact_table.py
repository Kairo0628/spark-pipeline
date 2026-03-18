from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as f

import argparse

def create_spark_session():
    conf = SparkConf()
    conf.set('spark.app.name', 'Create Fact Table')

    spark = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
    
    return spark

def preprocessing(ds):
    spark = create_spark_session()

    base_dir = 'gs://spark-pipeline-bucket/parquet'

    # dim_date 테이블은 운행 날짜를 기록하므로 매일 업데이트
    bus_dong_passenger = spark.read.parquet(f'{base_dir}/daily/bus_dong_passenger/dt={ds}')
    bus_dong_passenger.createOrReplaceTempView('bus_dong_passenger')
    dim_date = spark.sql("""
        SELECT
            BASE_YMD,
            SUBSTRING(BASE_YMD, 1, 4) AS YEAR,
            SUBSTRING(BASE_YMD, 5, 2) AS MONTH,
            SUBSTRING(BASE_YMD, 7, 2) AS DAY,
            DAYOFWEEK(TO_DATE(BASE_YMD, 'yyyyMMdd')) AS DAY_OF_WEEK,
            CASE
                WHEN DAYOFWEEK(TO_DATE(BASE_YMD, 'yyyyMMdd')) IN (1, 7) THEN TRUE
                ELSE FALSE
            END AS IS_WEEKEND,
            CURRENT_DATE() AS UPDATED_AT
        FROM bus_dong_passenger
        LIMIT 1
    """)

    dim_date.show(1)
    dim_date.printSchema()
    print('Partitions:', dim_date.rdd.getNumPartitions())

    dim_date.write\
            .format('bigquery')\
            .option('temporaryGcsBucket', 'spark-pipeline-bucket')\
            .option('temporaryGcsPath', 'temp')\
            .mode('append')\
            .save('data-engineering-478006.spark_dataset.dim_date')
    
    # fact1: fact_bus_stop_passenger
    bus_stop_passenger = spark.read.parquet(f'{base_dir}/daily/bus_stop_passenger/dt={ds}')
    bus_stop_passenger.createOrReplaceTempView('bus_stop_passenger')
    fact_bus_stop_passenger = spark.sql("""
        SELECT
            BASE_YMD,
            RTE_ID,
            STOP_ID,
            GTON_TNOPE,
            GTOFF_TNOPE,
            CURRENT_DATE() AS UPDATED_AT
        FROM bus_stop_passenger
    """)

    fact_bus_stop_passenger.cache()
    fact_bus_stop_passenger.show(1)
    fact_bus_stop_passenger.printSchema()
    print('Partitions:', fact_bus_stop_passenger.rdd.getNumPartitions())

    fact_bus_stop_passenger.write\
        .format('bigquery')\
        .option('temporaryGcsBucket', 'spark-pipeline-bucket')\
        .option('temporaryGcsPath', 'temp')\
        .mode('append')\
        .save('data-engineering-478006.spark_dataset.fact_bus_stop_passenger')
    fact_bus_stop_passenger.unpersist()
    
    # fact2: fact_bus_stop_trip_count
    bus_stop_trip_count = spark.read.parquet(f'{base_dir}/daily/bus_stop_trip_count/dt={ds}')
    bus_stop_trip_count = bus_stop_trip_count.drop('STOP_SEQ')
    bus_stop_trip_count.createOrReplaceTempView('bus_stop_trip_count')
    fact_bus_stop_trip_count = spark.sql("""
        SELECT
            *,
            CURRENT_DATE() AS UPDATED_AT
        FROM bus_stop_trip_count
    """)

    fact_bus_stop_trip_count.cache()
    fact_bus_stop_trip_count.show(1)
    fact_bus_stop_trip_count.printSchema()
    print('Partitions:', fact_bus_stop_trip_count.rdd.getNumPartitions())

    fact_bus_stop_trip_count.write\
        .format('bigquery')\
        .option('temporaryGcsBucket', 'spark-pipeline-bucket')\
        .option('temporaryGcsPath', 'temp')\
        .mode('append')\
        .save('data-engineering-478006.spark_dataset.fact_bus_stop_trip_count')
    fact_bus_stop_trip_count.unpersist()
    
    spark.stop()
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ds', required = True, help = 'Airflow의 Execution Date(Logical Date)')
    args = parser.parse_args()

    preprocessing(args.ds)
