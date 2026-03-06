from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as f

import argparse

def create_spark_session():
    conf = SparkConf()
    conf.set('spark.app.name', 'Daily Raw Data to Parquet')
    conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")

    spark = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
    
    return spark

def preprocessing(ds):
    spark = create_spark_session()

    base_dir = 'gs://spark-pipeline-bucket/raw_data/daily'
    dong_foot_traffic = spark.read.json(f'{base_dir}/dt={ds}/dong_foot_traffic.json')
    bus_stop_passenger = spark.read.json(f'{base_dir}/dt={ds}/bus_stop_passenger.json')
    bus_stop_trip_count = spark.read.json(f'{base_dir}/dt={ds}/bus_stop_trip_count.json')
    bus_dong_passenger = spark.read.json(f'{base_dir}/dt={ds}/bus_dong_passenger.json')

    dong_foot_traffic.createOrReplaceTempView('dong_foot_traffic')
    bus_stop_passenger.createOrReplaceTempView('bus_stop_passenger')
    bus_stop_trip_count.createOrReplaceTempView('bus_stop_trip_count')
    bus_dong_passenger.createOrReplaceTempView('bus_dong_passenger')

    dong_foot_traffic = spark.sql("""
        SELECT
            STDR_DE_ID AS BASE_YMD,
            TMZON_PD_SE AS HOUR,
            CAST(ADSTRD_CODE_SE AS INT) AS DONG_ID,
            CAST(TOT_LVPOP_CO AS FLOAT) AS TOT_LVPOP_CO,
            CAST(MALE_F0T9_LVPOP_CO AS FLOAT) AS MALE_F0T9_LVPOP_CO,
            (CAST(MALE_F10T14_LVPOP_CO AS FLOAT) + CAST(MALE_F15T19_LVPOP_CO AS FLOAT)) AS MALE_F10T19_LVPOP_CO,
            (CAST(MALE_F20T24_LVPOP_CO AS FLOAT) + CAST(MALE_F25T29_LVPOP_CO AS FLOAT)) AS MALE_F20T29_LVPOP_CO,
            (CAST(MALE_F30T34_LVPOP_CO AS FLOAT) + CAST(MALE_F35T39_LVPOP_CO AS FLOAT)) AS MALE_F30T39_LVPOP_CO,
            (CAST(MALE_F40T44_LVPOP_CO AS FLOAT) + CAST(MALE_F45T49_LVPOP_CO AS FLOAT)) AS MALE_F40T49_LVPOP_CO,
            (CAST(MALE_F50T54_LVPOP_CO AS FLOAT) + CAST(MALE_F55T59_LVPOP_CO AS FLOAT)) AS MALE_F50T59_LVPOP_CO,
            (CAST(MALE_F60T64_LVPOP_CO AS FLOAT) + CAST(MALE_F65T69_LVPOP_CO AS FLOAT)) AS MALE_F60T69_LVPOP_CO,
            CAST(MALE_F70T74_LVPOP_CO AS FLOAT) AS MALE_F70T74_LVPOP_CO,
            CAST(FEMALE_F0T9_LVPOP_CO AS FLOAT) AS FEMALE_F0T9_LVPOP_CO,
            (CAST(FEMALE_F10T14_LVPOP_CO AS FLOAT) + CAST(FEMALE_F15T19_LVPOP_CO AS FLOAT)) AS FEMALE_F10T19_LVPOP_CO,
            (CAST(FEMALE_F20T24_LVPOP_CO AS FLOAT) + CAST(FEMALE_F25T29_LVPOP_CO AS FLOAT)) AS FEMALE_F20T29_LVPOP_CO,
            (CAST(FEMALE_F30T34_LVPOP_CO AS FLOAT) + CAST(FEMALE_F35T39_LVPOP_CO AS FLOAT)) AS FEMALE_F30T39_LVPOP_CO,
            (CAST(FEMALE_F40T44_LVPOP_CO AS FLOAT) + CAST(FEMALE_F45T49_LVPOP_CO AS FLOAT)) AS FEMALE_F40T49_LVPOP_CO,
            (CAST(FEMALE_F50T54_LVPOP_CO AS FLOAT) + CAST(FEMALE_F55T59_LVPOP_CO AS FLOAT)) AS FEMALE_F50T59_LVPOP_CO,
            (CAST(FEMALE_F60T64_LVPOP_CO AS FLOAT) + CAST(FEMALE_F65T69_LVPOP_CO AS FLOAT)) AS FEMALE_F60T69_LVPOP_CO,
            CAST(FEMALE_F70T74_LVPOP_CO AS FLOAT) AS FEMALE_F70T74_LVPOP_CO
        FROM dong_foot_traffic
    """)
    dong_foot_traffic = dong_foot_traffic.withColumn('dt', f.lit(ds))
    dong_foot_traffic.show(1)
    dong_foot_traffic.printSchema()
    print('Partitions:', dong_foot_traffic.rdd.getNumPartitions())

    bus_stop_passenger = spark.sql("""
        SELECT
            USE_YMD AS BASE_YMD,
            CAST(RTE_ID AS INT) AS RTE_ID,
            RTE_NO,
            RTE_NM AS RTE_NM_DETAIL,
            CAST(STOPS_ID AS INT) AS STOP_ID,
            CAST(STOPS_ARS_NO AS INT) AS STOP_NO,
            SBWY_STNS_NM AS STOPS_NM_DETAIL,
            CAST(GTON_TNOPE AS FLOAT) AS GTON_TNOPE,
            CAST(GTOFF_TNOPE AS FLOAT) AS GTOFF_TNOPE
        FROM bus_stop_passenger
    """)
    bus_stop_passenger = bus_stop_passenger.withColumn('RTE_NM', f.col('RTE_NO'))\
                                            .withColumn('dt', f.lit(ds))\
                                            .drop('RTE_NO')
    bus_stop_passenger.show(1)
    bus_stop_passenger.printSchema()
    print('Partitions:', bus_stop_passenger.rdd.getNumPartitions())

    bus_stop_trip_count = spark.sql("""
        SELECT
            CRTR_DD AS BASE_YMD,
            CAST(RTE_ID AS INT) AS RTE_ID,
            CAST(STOPS_ID AS INT) AS STOP_ID,
            CAST(BUS_OPR AS FLOAT) AS BUS_OPR,
            CAST(BUS_OPR_00 AS FLOAT) AS BUS_OPR_00,
            CAST(BUS_OPR_01 AS FLOAT) AS BUS_OPR_01,
            CAST(BUS_OPR_02 AS FLOAT) AS BUS_OPR_02,
            CAST(BUS_OPR_03 AS FLOAT) AS BUS_OPR_03,
            CAST(BUS_OPR_04 AS FLOAT) AS BUS_OPR_04,
            CAST(BUS_OPR_05 AS FLOAT) AS BUS_OPR_05,
            CAST(BUS_OPR_06 AS FLOAT) AS BUS_OPR_06,
            CAST(BUS_OPR_07 AS FLOAT) AS BUS_OPR_07,
            CAST(BUS_OPR_08 AS FLOAT) AS BUS_OPR_08,
            CAST(BUS_OPR_09 AS FLOAT) AS BUS_OPR_09,
            CAST(BUS_OPR_10 AS FLOAT) AS BUS_OPR_10,
            CAST(BUS_OPR_11 AS FLOAT) AS BUS_OPR_11,
            CAST(BUS_OPR_12 AS FLOAT) AS BUS_OPR_12,
            CAST(BUS_OPR_13 AS FLOAT) AS BUS_OPR_13,
            CAST(BUS_OPR_14 AS FLOAT) AS BUS_OPR_14,
            CAST(BUS_OPR_15 AS FLOAT) AS BUS_OPR_15,
            CAST(BUS_OPR_16 AS FLOAT) AS BUS_OPR_16,
            CAST(BUS_OPR_17 AS FLOAT) AS BUS_OPR_17,
            CAST(BUS_OPR_18 AS FLOAT) AS BUS_OPR_18,
            CAST(BUS_OPR_19 AS FLOAT) AS BUS_OPR_19,
            CAST(BUS_OPR_20 AS FLOAT) AS BUS_OPR_20,
            CAST(BUS_OPR_21 AS FLOAT) AS BUS_OPR_21,
            CAST(BUS_OPR_22 AS FLOAT) AS BUS_OPR_22,
            CAST(BUS_OPR_23 AS FLOAT) AS BUS_OPR_23,
            CAST(STOPS_SEQ AS FLOAT) AS STOP_SEQ
        FROM bus_stop_trip_count
    """)
    bus_stop_trip_count = bus_stop_trip_count.withColumn('dt', f.lit(ds))
    bus_stop_trip_count.show(1)
    bus_stop_trip_count.printSchema()
    print('Partitions:', bus_stop_trip_count.rdd.getNumPartitions())

    bus_dong_passenger = spark.sql("""
        SELECT
            CRTR_DD AS BASE_YMD,
            CAST(DONG_ID AS INT) AS DONG_ID,
            CAST(BUS_PSNG AS FLOAT) AS BUS_PSNG,
            CAST(BUS_PSNG_00 AS FLOAT) AS BUS_PSNG_00,
            CAST(BUS_PSNG_01 AS FLOAT) AS BUS_PSNG_01,
            CAST(BUS_PSNG_02 AS FLOAT) AS BUS_PSNG_02,
            CAST(BUS_PSNG_03 AS FLOAT) AS BUS_PSNG_03,
            CAST(BUS_PSNG_04 AS FLOAT) AS BUS_PSNG_04,
            CAST(BUS_PSNG_05 AS FLOAT) AS BUS_PSNG_05,
            CAST(BUS_PSNG_06 AS FLOAT) AS BUS_PSNG_06,
            CAST(BUS_PSNG_07 AS FLOAT) AS BUS_PSNG_07,
            CAST(BUS_PSNG_08 AS FLOAT) AS BUS_PSNG_08,
            CAST(BUS_PSNG_09 AS FLOAT) AS BUS_PSNG_09,
            CAST(BUS_PSNG_10 AS FLOAT) AS BUS_PSNG_10,
            CAST(BUS_PSNG_11 AS FLOAT) AS BUS_PSNG_11,
            CAST(BUS_PSNG_12 AS FLOAT) AS BUS_PSNG_12,
            CAST(BUS_PSNG_13 AS FLOAT) AS BUS_PSNG_13,
            CAST(BUS_PSNG_14 AS FLOAT) AS BUS_PSNG_14,
            CAST(BUS_PSNG_15 AS FLOAT) AS BUS_PSNG_15,
            CAST(BUS_PSNG_16 AS FLOAT) AS BUS_PSNG_16,
            CAST(BUS_PSNG_17 AS FLOAT) AS BUS_PSNG_17,
            CAST(BUS_PSNG_18 AS FLOAT) AS BUS_PSNG_18,
            CAST(BUS_PSNG_19 AS FLOAT) AS BUS_PSNG_19,
            CAST(BUS_PSNG_20 AS FLOAT) AS BUS_PSNG_20,
            CAST(BUS_PSNG_21 AS FLOAT) AS BUS_PSNG_21,
            CAST(BUS_PSNG_22 AS FLOAT) AS BUS_PSNG_22,
            CAST(BUS_PSNG_23 AS FLOAT) AS BUS_PSNG_23
        FROM bus_dong_passenger
    """)
    bus_dong_passenger = bus_dong_passenger.withColumn('dt', f.lit(ds))
    bus_dong_passenger.show(1)
    bus_dong_passenger.printSchema()
    print('Partitions:', bus_dong_passenger.rdd.getNumPartitions())

    write_base_dir = 'gs://spark-pipeline-bucket/parquet/daily'
    
    dong_foot_traffic = dong_foot_traffic.repartition(3)
    bus_stop_passenger = bus_stop_passenger.repartition(3)
    bus_stop_trip_count = bus_stop_trip_count.repartition(6)
    dong_foot_traffic.write\
                    .mode('overwrite')\
                    .partitionBy('dt')\
                    .parquet(f'{write_base_dir}/dong_foot_traffic')
    bus_stop_passenger.write\
                    .mode('overwrite')\
                    .partitionBy('dt')\
                    .parquet(f'{write_base_dir}/bus_stop_passenger')
    bus_stop_trip_count.write\
                    .mode('overwrite')\
                    .partitionBy('dt')\
                    .parquet(f'{write_base_dir}/bus_stop_trip_count')
    bus_dong_passenger.write\
                    .mode('overwrite')\
                    .partitionBy('dt')\
                    .parquet(f'{write_base_dir}/bus_dong_passenger')
    
    spark.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ds', required = True, help = 'Airflow의 Execution Date(Logical Date)')
    args = parser.parse_args()

    preprocessing(args.ds)
