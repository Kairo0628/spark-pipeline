import argparse

from pyspark import SparkConf
from pyspark.sql import SparkSession

def create_spark_session():
    conf = SparkConf()
    conf.set('spark.app.name', 'Monthly raw data to Parquet')

    spark = SparkSession.builder\
        .config(conf = conf)\
        .getOrCreate()
    
    return spark

def main(ds):
    spark = create_spark_session()

    raw_dir = 'gs://my-spark-pipeline-bucket/raw_data/monthly'
    parquet_dir = 'gs://my-spark-pipeline-bucket/parquet/monthly'

    # 서울시 정류장마스터 정보 - raw_stop_master
    stop_master = spark.read.json(f'{raw_dir}/dt={ds}/raw_stop_master.json')
    stop_master.createOrReplaceTempView('stop_master')

    stop_master = spark.sql("""
        SELECT
            CAST(:ds AS DATE) AS dt,
            CAST(CRTR_ID AS LONG) AS STOP_ID,
            CRTR_NM AS STOP_NM,
            LPAD(CAST(CRTR_NO AS STRING), 5, '0') AS STOP_ARS_NO,
            CRTR_TYPE AS STOP_TYPE,
            LAT,
            LOT,
            TRIM(BUS_ARVL_INFO_GUIDEM_INSTL) AS BUS_ARVL_INFO_GUIDEM_INSTL
        FROM stop_master
    """,
    args = {'ds': ds})

    stop_master.show(5)
    stop_master.printSchema()

    stop_master.write\
        .mode('overwrite')\
        .partitionBy('dt')\
        .parquet(f'{parquet_dir}/stop_master')

    # 서울시 버스노선 기본정보 항목정보 - raw_route_id
    route_id = spark.read.json(f'{raw_dir}/dt={ds}/raw_route_id.json')
    route_id.createOrReplaceTempView('route_id')

    route_id = spark.sql("""
        SELECT
            CAST(:ds AS DATE) AS dt,
            CAST(RTE_ID AS LONG) AS RTE_ID,
            RTE_NM
        FROM route_id
    """,
    args = {'ds': ds})

    route_id.show(5)
    route_id.printSchema()

    route_id.write\
        .mode('overwrite')\
        .partitionBy('dt')\
        .parquet(f'{parquet_dir}/route_id')
    
    # 서울시 버스노선별 정류장별 시간대별 승하차 인원 정보 - raw_route_stop_hour_passenger
    route_stop_hour_passenger = spark.read.json(f'{raw_dir}/dt={ds}/raw_route_stop_hour_passenger.json')
    route_stop_hour_passenger.createOrReplaceTempView('route_stop_hour_passenger')

    on_off_cols = []
    for i in range(24):
        if i == 1:
            on_off_cols.append(f'HR_{i}_GET_OFF_NOPE HR_{i}_GET_OFF')
            on_off_cols.append(f'HR_{i}_GET_ON_NOPE HR_{i}_GET_ON')       
        else:
            on_off_cols.append(f'HR_{i}_GET_OFF_TNOPE AS HR_{i}_GET_OFF')
            on_off_cols.append(f'HR_{i}_GET_ON_TNOPE AS HR_{i}_GET_ON')
    on_off_sql = ',\n'.join(on_off_cols)

    route_stop_hour_passenger = spark.sql(f"""
        SELECT
            CAST(:ds AS DATE) AS dt,
            USE_YM,
            RTE_NO AS RTE_NM,
            RTE_NM AS RTE_NM_DETAIL,
            CAST(STOPS_ID AS LONG) AS STOP_ID,
            STOPS_ARS_NO AS STOP_ARS_NO,
            REGEXP_EXTRACT(SBWY_STNS_NM, '^(.*)\\\\(\\\\d{{5}}\\\\)$', 1) AS STOP_NM,
            CAST(REGEXP_EXTRACT(SBWY_STNS_NM, '\\\\((\\\\d{{5}})\\\\)$', 1) AS LONG) AS STOP_SEQ,
            TRFC_MNS_TYPE_CD,
            TRFC_MNS_TYPE_NM,
            {on_off_sql}
        FROM route_stop_hour_passenger
    """,
    args = {'ds': ds})

    route_stop_hour_passenger.show(5)
    route_stop_hour_passenger.printSchema()

    route_stop_hour_passenger.repartition(6)\
        .write\
        .mode('overwrite')\
        .partitionBy('dt')\
        .parquet(f'{parquet_dir}/route_stop_hour_passenger')
    
    spark.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ds',
                        required = True,
                        help = 'Airflow의 Logical Date(ds)')
    args = parser.parse_args()

    main(args.ds)
