from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

import argparse

def create_spark_session():
    conf = SparkConf()
    conf.set('spark.app.name', 'Monthly Raw Data to Parquet')
    conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

    spark = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
    
    return spark

def preprocessing(ds):
    spark = create_spark_session()

    base_dir = 'gs://spark-pipeline-bucket/raw_data/monthly'
    write_base_dir = 'gs://spark-pipeline-bucket/parquet/monthly'

    # 노선 정류장 마스터
    bus_route_stop = spark.read.json(f'{base_dir}/dt={ds}/bus_route_stop.json')
    bus_route_stop = bus_route_stop.withColumn('RTE_ID', f.col('RTE_ID').cast('int'))\
                                .withColumn('STOP_ID', f.col('CRTR_ID').cast('int'))\
                                .withColumn('LNKG_LEN', f.col('LNKG_LEN').cast('int'))\
                                .withColumn('STOP_SEQ', f.col('CRTR_SEQ').cast('int'))\
                                .withColumn('dt', f.lit(ds))\
                                .drop('CRTR_ID', 'CRTR_SEQ')

    bus_route_stop.show(1)
    bus_route_stop.printSchema()
    print('Partitions:', bus_route_stop.rdd.getNumPartitions())

    bus_route_stop = bus_route_stop.repartition(6)
    bus_route_stop.write\
                .mode('overwrite')\
                .partitionBy('dt')\
                .parquet(f'{write_base_dir}/bus_route_stop')
    
    # 정류장 마스터
    bus_stop = spark.read.json(f'{base_dir}/dt={ds}/bus_stop.json')
    bus_stop = bus_stop.withColumn('STOP_ID', f.col('CRTR_ID').cast('int'))\
                    .withColumn('STOP_NM', f.col('CRTR_NM'))\
                    .withColumn('STOP_TYPE', f.col('CRTR_TYPE'))\
                    .withColumn('STOP_NO', f.col('CRTR_NO').cast('int'))\
                    .withColumn('LAT', f.col('LAT').cast('float'))\
                    .withColumn('LOT', f.col('LOT').cast('float'))\
                    .withColumn('ARR_INFO', f.trim(f.col('BUS_ARVL_INFO_GUIDEM_INSTL')))\
                    .withColumn('dt', f.lit(ds))\
                    .drop('CRTR_ID', 'CRTR_NM', 'CRTR_TYPE', 'CRTR_NO', 'BUS_ARVL_INFO_GUIDEM_INSTL')
    
    bus_stop.show(1)
    bus_stop.printSchema()
    print('Partitions:', bus_stop.rdd.getNumPartitions())

    bus_stop.write\
            .mode('overwrite')\
            .partitionBy('dt')\
            .parquet(f'{write_base_dir}/bus_stop')
    
    # 노선 마스터
    bus_route = spark.read.json(f'{base_dir}/dt={ds}/bus_route.json')
    bus_route = bus_route.withColumn('RTE_ID', f.col('RTE_ID').cast('int'))\
                        .withColumn('DIST', f.col('DIST').cast('float'))\
                        .withColumn('dt', f.lit(ds))
                        
    bus_route.show(1)
    bus_route.printSchema()
    print('Partitions:', bus_route.rdd.getNumPartitions())

    bus_route.write\
                .mode('overwrite')\
                .partitionBy('dt')\
                .parquet(f'{write_base_dir}/bus_route')

    spark.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ds', required = True, help = 'Airflow의 Execute Date(Logical Date)')
    args = parser.parse_args()

    preprocessing(args.ds)
