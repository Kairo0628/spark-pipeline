from pyspark import SparkConf
from pyspark.sql import SparkSession

import os
from dotenv import load_dotenv

load_dotenv()
PROJECT_ID = os.getenv('PROJECT_ID')

def create_spark_session():
    conf = SparkConf()
    conf.set('spark.app.name', 'Create Wide Table')

    spark = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
    
    return spark

def main():
    spark = create_spark_session()

    parquet_dir = 'gs://my-spark-pipeline-bucket/parquet'
    bigquery_dir = f'{PROJECT_ID}.spark_dataset'

    # table load
    dim_stop = spark.read.format('bigquery')\
        .load(f'{bigquery_dir}.dim_stop')
    dim_stop.createOrReplaceTempView('dim_stop')

    staging_stop_dong = spark.read.format('bigquery')\
        .load(f'{bigquery_dir}.staging_stop_dong')
    staging_stop_dong.createOrReplaceTempView('staging_stop_dong')

    fact_dong_hour_passenger = spark.read.format('bigquery')\
        .load(f'{bigquery_dir}.fact_dong_hour_passenger')
    fact_dong_hour_passenger.createOrReplaceTempView('fact_dong_hour_passenger')

    dim_route = spark.read.format('bigquery')\
        .load(f'{bigquery_dir}.dim_route')
    dim_route.createOrReplaceTempView('dim_route')

    fact_route_stop_passenger_opr = spark.read.format('bigquery')\
        .load(f'{bigquery_dir}.fact_route_stop_passenger_opr')
    fact_route_stop_passenger_opr.createOrReplaceTempView('fact_route_stop_passenger_opr')

    # 8월 데이터만 가져옴
    route_stop_hour_passenger = spark.read.parquet(f'{parquet_dir}/monthly/route_stop_hour_passenger/dt=2026-09-05')
    route_stop_hour_passenger.createOrReplaceTempView('route_stop_hour_passenger')

    # 반복 패턴 컬럼 리스트 생성
    opr_cols = []
    opr_cols2 = []

    on_off_cols = []
    on_off_cols2 = []

    psng_cols = []
    psng_cols2 = []

    for i in range(24):
        n = str(i).zfill(2)
        opr_cols.append(f'f.BUS_OPR_{n} AS RTE_STOP_{n}_OPR')
        opr_cols2.append(f'r.RTE_STOP_{n}_OPR')

        on_off_cols.append(f'r.HR_{i}_GET_OFF AS MONTHLY_RTE_STOP_{n}_OFF_PSNG')
        on_off_cols.append(f'r.HR_{i}_GET_ON AS MONTHLY_RTE_STOP_{n}_ON_PSNG')
        on_off_cols2.append(f'r.MONTHLY_RTE_STOP_{n}_OFF_PSNG')
        on_off_cols2.append(f'r.MONTHLY_RTE_STOP_{n}_ON_PSNG')

        psng_cols.append(f'f.BUS_PSNG_{n} AS DONG_BUS_PSNG_{n}')
        psng_cols2.append(f'd.DONG_BUS_PSNG_{n}')

    opr_sql = ',\n'.join(opr_cols)
    opr_sql2 = ',\n'.join(opr_cols2)

    on_off_sql = ',\n'.join(on_off_cols)
    on_off_sql2 = ',\n'.join(on_off_cols2)

    psng_sql = ',\n'.join(psng_cols)
    psng_sql2 = ',\n'.join(psng_cols2)

    feature_route_stop_psng_opr = spark.sql(f"""
        WITH route_stop_psng_opr_add_stop AS (
            SELECT
                d.STOP_ID,
                f.*
            FROM dim_stop d
            JOIN fact_route_stop_passenger_opr f
            ON d.STOP_SK = f.STOP_SK
            WHERE d.IS_CURRENT                         
        ),                                   
        temp_route_stop_passenger_opr AS (
            SELECT
                f.DATE,
                d.RTE_NM,
                d.RTE_NM_DETAIL,
                d.TRFC_MNS_TYPE_NM,
                f.STOP_SK,
                f.STOP_ID,
                f.STOP_SEQ,
                f.GET_OFF AS RTE_STOP_TOTAL_OFF_PSNG,
                f.GET_ON AS RTE_STOP_TOTAL_ON_PSNG,
                f.TOTAL_BUS_OPR AS RTE_STOP_TOTAL_OPR,
                {opr_sql}
            FROM dim_route d
            JOIN route_stop_psng_opr_add_stop f
            ON d.RTE_SK = f.RTE_SK
            WHERE d.IS_CURRENT
        ),
        route_stop_psng_opr_add_monthly_psng AS (
            SELECT
                t.*,
                {on_off_sql}
            FROM temp_route_stop_passenger_opr t
            JOIN route_stop_hour_passenger r
            ON t.RTE_NM_DETAIL = r.RTE_NM_DETAIL
            AND t.STOP_ID = r.STOP_ID
            AND t.STOP_SEQ = r.STOP_SEQ
        ),
        dong_hour_psng_add_stop AS (
            SELECT
                f.DATE,
                s.STOP_SK,
                s.STOP_ID,
                s.STOP_ARS_NO,
                s.STOP_TYPE,
                s.LAT,
                s.LOT,
                s.BUS_ARVL_INFO_GUIDEM_INSTL,
                s.DONG_ID,
                s.DONG_NM,
                s.SGG_NM,
                f.TOTAL_BUS_PSNG AS DONG_TOTAL_BUS_PSNG,
                {psng_sql}
            FROM staging_stop_dong s
            JOIN fact_dong_hour_passenger f
            ON s.DONG_ID = f.DONG_ID
        )
        SELECT
            r.DATE,
            r.RTE_NM,
            r.RTE_NM_DETAIL,
            r.TRFC_MNS_TYPE_NM,
            r.STOP_ID,
            d.STOP_ARS_NO,
            r.STOP_SEQ,
            d.STOP_TYPE,
            d.LAT,
            d.LOT,
            d.BUS_ARVL_INFO_GUIDEM_INSTL,
            d.DONG_NM,
            d.SGG_NM,
            r.RTE_STOP_TOTAL_OFF_PSNG,
            r.RTE_STOP_TOTAL_ON_PSNG,
            r.RTE_STOP_TOTAL_OPR,
            {opr_sql2},
            {on_off_sql2},
            d.DONG_TOTAL_BUS_PSNG,
            {psng_sql2}
        FROM route_stop_psng_opr_add_monthly_psng r
        JOIN dong_hour_psng_add_stop d
        ON r.STOP_SK = d.STOP_sk
        AND r.DATE = d.DATE
    """)

    feature_route_stop_psng_opr.show(5)
    feature_route_stop_psng_opr.printSchema()

    feature_route_stop_psng_opr.write\
        .format('bigquery')\
        .option('temporaryGcsBucket', 'my-spark-pipeline-bucket')\
        .option('temporaryGcsPath', 'temp')\
        .mode('overwrite')\
        .save(f'{bigquery_dir}.feature_route_stop_psng_opr')
    
    spark.stop()

if __name__ == '__main__':
    main()
