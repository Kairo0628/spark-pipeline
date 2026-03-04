from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

import argparse

def create_spark_session():
    conf = SparkConf()
    conf.set('spark.app.name', 'Monthly Raw Data to Parquet')
    conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")

    spark = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
    
    return spark

def preprocessing(ds):
    spark = create_spark_session()

    base_dir = 'gs://spark-pipeline-bucket/raw_data/monthly'
    bus_route = spark.read.json(f'{base_dir}/dt={ds}/bus_route.json')
    bus_stop = spark.read.json(f'{base_dir}/dt={ds}/bus_stop.json')
    dong_info = spark.read.json(f'{base_dir}/dt={ds}/dong_info.json')

    bus_route = bus_route.withColumn('RTE_ID', f.col('RTE_ID').cast('int'))\
                            .withColumn('dt', f.lit(ds))
    bus_route.printSchema()
    bus_route.show(1)

    bus_stop = bus_stop.withColumn('STOPS_ID', f.col('NODE_ID').cast('int'))\
                        .withColumn('STOPS_NO', f.col('STOPS_NO').cast('int'))\
                        .withColumn('XCRD', f.col('XCRD').cast('float'))\
                        .withColumn('YCRD', f.col('YCRD').cast('float'))\
                        .withColumn('dt', f.lit(ds))\
                        .drop('NODE_ID')
    bus_stop.printSchema()
    bus_stop.show(1)

    dong_info = dong_info.withColumn('DONG_ID', f.col('DONG_ID').cast('int'))\
                            .withColumn('dt', f.lit(ds))
    dong_info.printSchema()
    dong_info.show(1)

    write_base_dir = 'gs://spark-pipeline-bucket/parquet/monthly'
    bus_route.write\
                .mode('overwrite')\
                .partitionBy('dt')\
                .parquet(f'{write_base_dir}/bus_route')
    bus_stop.write\
                .mode('overwrite')\
                .partitionBy('dt')\
                .parquet(f'{write_base_dir}/bus_stop')
    dong_info.write\
                .mode('overwrite')\
                .partitionBy('dt')\
                .parquet(f'{write_base_dir}/dong_info')

    spark.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ds', required = True, help = 'Airflow의 Execute Date(Logical Date)')
    args = parser.parse_args()

    preprocessing(args.ds)
