import argparse

from pyspark import SparkConf
from pyspark.sql import SparkSession

def create_spark_session():
    conf = SparkConf()
    conf.set('spark.app.name', 'Daily raw data to Parquet')

    spark = SparkSession.builder\
        .config(conf = conf)\
        .getOrCreate()
    
    return spark

def main(ds):
    spark = create_spark_session()

    raw_dir = 'gs://my-spark-pipeline-bucket/raw_data/daily'
    parquet_dir = 'gs://my-spark-pipeline-bucket/parquet/daily'

    # 서울시 버스노선별 정류장별 승하차 인원 정보 - raw_route_stop_total_passenger
    route_stop_total_passenger = spark.read.json(f'{raw_dir}/dt={ds}/raw_route_stop_total_passenger.json')
    route_stop_total_passenger.createOrReplaceTempView('route_stop_total_passenger')

    route_stop_total_passenger = spark.sql("""
        SELECT
            CAST(:ds AS DATE) AS dt,
            TO_DATE(USE_YMD, 'yyyyMMdd') AS USE_YMD,
            CAST(RTE_ID AS LONG) AS RTE_ID_8,
            RTE_NO AS RTE_NM,
            RTE_NM AS RTE_NM_DETAIL,
            CAST(STOPS_ID AS LONG) AS STOP_ID,
            STOPS_ARS_NO AS STOP_ARS_NO,
            REGEXP_EXTRACT(SBWY_STNS_NM, '^(.*)\\\\(\\\\d{5}\\\\)$', 1) AS STOP_NM,
            CAST(REGEXP_EXTRACT(SBWY_STNS_NM, '\\\\((\\\\d{5})\\\\)$', 1) AS LONG) AS STOP_SEQ,
            GTOFF_TNOPE AS GET_OFF,
            GTON_TNOPE AS GET_ON
        FROM route_stop_total_passenger
    """,
    args = {'ds': ds})

    route_stop_total_passenger.show(5)
    route_stop_total_passenger.printSchema()

    route_stop_total_passenger.repartition(6)\
        .write\
        .mode('overwrite')\
        .partitionBy('dt')\
        .parquet(f'{parquet_dir}/route_stop_total_passenger')

    # 서울시 행정동별 버스 총 승차 승객수 정보 - dong_hour_passenger
    dong_hour_passenger = spark.read.json(f'{raw_dir}/dt={ds}/raw_dong_hour_passenger.json')
    dong_hour_passenger.createOrReplaceTempView('dong_hour_passenger')

    psng_cols = []
    opr_cols = []
    for i in range(24):
        n = str(i).zfill(2)
        psng_cols.append(f'BUS_PSNG_{n}')
        opr_cols.append(f'BUS_OPR_{n}')
    psng_sql = ',\n'.join(psng_cols)
    opr_sql = ',\n'.join(opr_cols)

    dong_hour_passenger = spark.sql(f"""
        SELECT
            CAST(:ds AS DATE) AS dt,
            TO_DATE(CRTR_DD, 'yyyyMMdd') AS USE_YMD,
            CAST(DONG_ID AS LONG) AS DONG_ID,
            BUS_PSNG AS TOTAL_BUS_PSNG,
            {psng_sql}
        FROM dong_hour_passenger
    """,
    args = {'ds': ds})

    dong_hour_passenger.show(5)
    dong_hour_passenger.printSchema()

    dong_hour_passenger.write\
        .mode('overwrite')\
        .partitionBy('dt')\
        .parquet(f'{parquet_dir}/dong_hour_passenger')

    # 서울시 노선별 정류장별 총 버스 운행횟수 정보 - route_stop_sequence
    route_stop_sequence = spark.read.json(f'{raw_dir}/dt={ds}/raw_route_stop_sequence.json')
    route_stop_sequence.createOrReplaceTempView('route_stop_sequence')

    route_stop_sequence = spark.sql(f"""
        SELECT
            CAST(:ds AS DATE) AS dt,
            TO_DATE(CRTR_DD, 'yyyyMMdd') AS USE_YMD,
            CAST(RTE_ID AS LONG) AS RTE_ID,
            CAST(STOPS_ID AS LONG) AS STOP_ID,
            CAST(STOPS_SEQ AS LONG) AS STOP_SEQ,
            BUS_OPR AS TOTAL_BUS_OPR,
            {opr_sql}
        FROM route_stop_sequence
    """,
    args = {'ds': ds})

    route_stop_sequence.show(5)
    route_stop_sequence.printSchema()

    route_stop_sequence.repartition(6)\
        .write\
        .mode('overwrite')\
        .partitionBy('dt')\
        .parquet(f'{parquet_dir}/route_stop_sequence')

    spark.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ds',
                        required = True,
                        help = 'Airflow의 Logical Date(ds)')
    args = parser.parse_args()

    main(args.ds)
