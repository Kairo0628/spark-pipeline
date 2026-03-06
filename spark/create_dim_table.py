from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as f

import argparse

def create_spark_session():
    conf = SparkConf()
    conf.set('spark.app.name', 'Create Dimension Table')
    conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")

    spark = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
    
    return spark

def preprocessing(ds):
    spark = create_spark_session()

    base_dir = 'gs://spark-pipeline-bucket/parquet'

    # dim1 : dim_bus_route
    bus_route = spark.read.parquet(f'{base_dir}/monthly/bus_route/dt={ds}')
    bus_route.createOrReplaceTempView('bus_route')
    bus_stop_passenger = spark.read.parquet(f'{base_dir}/daily/bus_stop_passenger/dt={ds}')
    bus_stop_passenger.createOrReplaceTempView('bus_stop_passenger')

    dim_bus_route = spark.sql("""
        SELECT 
            r.RTE_NM,
            r.RTE_ID AS RTE_ID_1,
            p.RTE_ID AS RTE_ID_2,
            p.RTE_NM_DETAIL,
            r.RTE_TYPE,
            r.DIST,
            CURRENT_DATE() AS UPDATED_AT
        FROM bus_route r
        LEFT JOIN bus_stop_passenger p
        ON r.RTE_NM = p.RTE_NM
    """)
    regex = r'.*\((\S+)~(\S+)\)'
    dim_bus_route = dim_bus_route.withColumn('RTE_START', f.regexp_extract(f.col('RTE_NM_DETAIL'), regex, 1))\
                                .withColumn('RTE_END', f.regexp_extract(f.col('RTE_NM_DETAIL'), regex, 2))\
                                .drop('RTE_NM_DETAIL')

    dim_bus_route.show(1)
    dim_bus_route.printSchema()
    print('Partitions:', dim_bus_route.rdd.getNumPartitions())

    dim_bus_route.write\
                .format('bigquery')\
                .option('temporaryGcsBucket', 'spark-pipeline-bucket')\
                .option('temporaryGcsPath', 'temp')\
                .mode('append')\
                .save('data-engineering-478006.spark_dataset.dim_bus_route')
                
    # dim2 : dim_bus_stop
    bus_stop = spark.read.parquet(f'{base_dir}/monthly/bus_stop/dt={ds}')
    bus_stop.createOrReplaceTempView('bus_stop')
    dim_bus_stop = spark.sql("""
        SELECT
            *,
            CURRENT_DATE() AS UPDATED_AT
        FROM bus_stop
    """)

    dim_bus_stop.show(1)
    dim_bus_stop.printSchema()
    print('Partitions:', dim_bus_stop.rdd.getNumPartitions())

    dim_bus_stop.write\
                .format('bigquery')\
                .option('temporaryGcsBucket', 'spark-pipeline-bucket')\
                .option('temporaryGcsPath', 'temp')\
                .mode('append')\
                .save('data-engineering-478006.spark_dataset.dim_bus_stop')
    
    # Relation Table: bridge_bus_route_seq
    bus_route_stop = spark.read.parquet(f'{base_dir}/monthly/bus_route_stop/dt={ds}')
    bus_route_stop.createOrReplaceTempView('bus_route_stop')
    bridge_bus_route_seq = spark.sql("""
        SELECT
            *,
            CURRENT_DATE() AS UPDATED_AT
        FROM bus_route_stop
        ORDER BY RTE_ID, STOP_ID, STOP_SEQ
    """)

    bridge_bus_route_seq.show(1)
    bridge_bus_route_seq.printSchema()
    print('Partitions:', bridge_bus_route_seq.rdd.getNumPartitions())

    bridge_bus_route_seq.write\
                .format('bigquery')\
                .option('temporaryGcsBucket', 'spark-pipeline-bucket')\
                .option('temporaryGcsPath', 'temp')\
                .mode('append')\
                .save('data-engineering-478006.spark_dataset.bridge_bus_route_seq')

    spark.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ds', required = True, help = 'Airflow의 Execution Date(Logical Date)')
    args = parser.parse_args()

    preprocessing(args.ds)
