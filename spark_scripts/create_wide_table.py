from pyspark import SparkConf
from pyspark.sql import SparkSession

def create_spark_session():
    conf = SparkConf()
    conf.set('spark.app.name', 'Create Wide Table')

    spark = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
    
    return spark

def create_wide_table():
    spark = create_spark_session()

    dataset = 'data-engineering-478006.spark_dataset'
    
    # Table Load
    bridge_bus_route_seq = spark.read.format('bigquery')\
                            .load(f'{dataset}.bridge_bus_route_seq')
    dim_bus_route = spark.read.format('bigquery')\
                    .load(f'{dataset}.dim_bus_route')
    dim_date = spark.read.format('bigquery')\
                    .load(f'{dataset}.dim_date')
    dim_stop_dong = spark.read.format('bigquery')\
                    .load(f'{dataset}.dim_stop_dong')
    dim_bus_stop = spark.read.format('bigquery')\
                .load(f'{dataset}.dim_bus_stop')
    fact_bus_dong_passenger = spark.read.format('bigquery')\
                                .load(f'{dataset}.fact_bus_dong_passenger')
    fact_bus_stop_passenger = spark.read.format('bigquery')\
                                .load(f'{dataset}.fact_bus_stop_passenger')
    fact_bus_stop_trip_count = spark.read.format('bigquery')\
                                    .load(f'{dataset}.fact_bus_stop_trip_count')
    
    bridge_bus_route_seq.createOrReplaceTempView('bridge_bus_route_seq')
    dim_bus_route.createOrReplaceTempView('dim_bus_route')
    dim_date.createOrReplaceTempView('dim_date')
    dim_stop_dong.createOrReplaceTempView('dim_stop_dong')
    dim_bus_stop.createOrReplaceTempView('dim_bus_stop')
    fact_bus_dong_passenger.createOrReplaceTempView('fact_bus_dong_passenger')
    fact_bus_stop_passenger.createOrReplaceTempView('fact_bus_stop_passenger')
    fact_bus_stop_trip_count.createOrReplaceTempView('fact_bus_stop_trip_count')

    # agg_route_stop
    agg_route_stop = spark.sql("""
        WITH latest_route AS (
            SELECT RTE_NM, RTE_ID_1 AS RTE_ID, RTE_TYPE, DIST
            FROM (SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY RTE_NM, RTE_ID_1 ORDER BY UPDATED_AT DESC) AS ROW_NUM
                FROM dim_bus_route)
            WHERE ROW_NUM = 1
        ), latest_stop_dong AS (
            SELECT STOP_ID, STOP_NM, STOP_TYPE, DONG_ID, SGG_NM, DONG_NM
            FROM (SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY STOP_ID ORDER BY UPDATED_AT DESC) AS ROW_NUM
                FROM dim_stop_dong)
            WHERE ROW_NUM = 1
        ), latest_stop AS (
            SELECT STOP_ID, LAT, LOT
            FROM (SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY STOP_ID ORDER BY UPDATED_AT DESC) AS ROW_NUM
                FROM dim_bus_stop)
            WHERE ROW_NUM = 1
        ), latest_bridge AS (
            SELECT RTE_ID, STOP_ID, STOP_SEQ, LNKG_LEN,
                MAX(STOP_SEQ) OVER(PARTITION BY RTE_ID) AS RTE_STOP_COUNT
            FROM (SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY RTE_ID, STOP_ID, STOP_SEQ ORDER BY UPDATED_AT DESC) AS ROW_NUM
                FROM bridge_bus_route_seq)
            WHERE ROW_NUM = 1
        ), route_dong_stats AS (
            SELECT RTE_ID, COUNT(DISTINCT SGG_NM) AS RTE_SGG_COUNT, COUNT(DISTINCT DONG_ID) RTE_DONG_COUNT
            FROM latest_bridge b
            LEFT JOIN latest_stop_dong d ON b.STOP_ID = d.STOP_ID
            GROUP BY RTE_ID
        )
        SELECT
            b.RTE_ID,
            r.RTE_NM,
            r.RTE_TYPE,
            r.DIST,
            b.RTE_STOP_COUNT,
            b.STOP_ID,
            d.STOP_NM,
            d.STOP_TYPE,
            s.LAT,
            s.LOT,
            b.STOP_SEQ,
            b.LNKG_LEN,
            d.DONG_ID,
            d.SGG_NM,
            d.DONG_NM,
            ds.RTE_SGG_COUNT,
            ds.RTE_DONG_COUNT
        FROM latest_bridge b
        JOIN latest_route r ON b.RTE_ID = r.RTE_ID
        JOIN latest_stop_dong d ON b.STOP_ID = d.STOP_ID
        JOIN latest_stop s ON b.STOP_ID = s.STOP_ID
        JOIN route_dong_stats ds ON b.RTE_ID = ds.RTE_ID
    """)
    agg_route_stop.createOrReplaceTempView('agg_route_stop')

    # agg_stop_passenger
    latest_stop_passenger = spark.sql("""
        SELECT BASE_YMD, RTE_ID, STOP_ID, GTON_TNOPE, GTOFF_TNOPE
        FROM (SELECT
                *,
                ROW_NUMBER() OVER(PARTITION BY BASE_YMD, RTE_ID, STOP_ID ORDER BY UPDATED_AT DESC) AS ROW_NUM
            FROM fact_bus_stop_passenger)
        WHERE ROW_NUM = 1
    """)
    latest_stop_passenger.createOrReplaceTempView('latest_stop_passenger')

    agg_stop_passenger = spark.sql("""
        SELECT
            p.BASE_YMD,
            r.RTE_ID_1 AS RTE_ID,
            p.STOP_ID,
            p.GTON_TNOPE,
            p.GTOFF_TNOPE,
            SUM(GTON_TNOPE) OVER(PARTITION BY RTE_ID) AS TOTAL_ON,
            SUM(GTOFF_TNOPE) OVER(PARTITION BY RTE_ID) AS TOTAL_OFF
        FROM latest_stop_passenger p
        JOIN dim_bus_route r ON p.RTE_ID = r.RTE_ID_2
    """)
    agg_stop_passenger.createOrReplaceTempView('agg_stop_passenger')

    # latest_trip_count
    latest_trip_count = spark.sql("""
        SELECT
            BASE_YMD,
            RTE_ID,
            STOP_ID,
            BUS_OPR,
            BUS_OPR_00,
            BUS_OPR_01,
            BUS_OPR_02,
            BUS_OPR_03,
            BUS_OPR_04,
            BUS_OPR_05,
            BUS_OPR_06,
            BUS_OPR_07,
            BUS_OPR_08,
            BUS_OPR_09,
            BUS_OPR_10,
            BUS_OPR_11,
            BUS_OPR_12,
            BUS_OPR_13,
            BUS_OPR_14,
            BUS_OPR_15,
            BUS_OPR_16,
            BUS_OPR_17,
            BUS_OPR_18,
            BUS_OPR_19,
            BUS_OPR_20,
            BUS_OPR_21,
            BUS_OPR_22,
            BUS_OPR_23
        FROM (SELECT
                *,
                ROW_NUMBER() OVER(PARTITION BY BASE_YMD, RTE_ID, STOP_ID ORDER BY UPDATED_AT DESC) AS ROW_NUM
            FROM fact_bus_stop_trip_count) c
        WHERE ROW_NUM = 1
    """)
    latest_trip_count.createOrReplaceTempView('latest_trip_count')

    # latest_dong_passenger
    latest_dong_passenger = spark.sql("""
        SELECT
            BASE_YMD,
            BUS_PSNG,
            DONG_ID,
            BUS_PSNG_00,
            BUS_PSNG_01,
            BUS_PSNG_02,
            BUS_PSNG_03,
            BUS_PSNG_04,
            BUS_PSNG_05,
            BUS_PSNG_06,
            BUS_PSNG_07,
            BUS_PSNG_08,
            BUS_PSNG_09,
            BUS_PSNG_10,
            BUS_PSNG_11,
            BUS_PSNG_12,
            BUS_PSNG_13,
            BUS_PSNG_14,
            BUS_PSNG_15,
            BUS_PSNG_16,
            BUS_PSNG_17,
            BUS_PSNG_18,
            BUS_PSNG_19,
            BUS_PSNG_20,
            BUS_PSNG_21,
            BUS_PSNG_22,
            BUS_PSNG_23
        FROM (SELECT
                *,
                ROW_NUMBER() OVER(PARTITION BY BASE_YMD, DONG_ID ORDER BY UPDATED_AT DESC) AS ROW_NUM
            FROM fact_bus_dong_passenger)
        WHERE ROW_NUM = 1
    """)
    latest_dong_passenger.createOrReplaceTempView('latest_dong_passenger')

    # fin_wide_table
    fin_wide_table = spark.sql("""
        SELECT
            CAST(d.YEAR AS INT) AS YEAR,
            CAST(d.MONTH AS INT) AS MONTH,
            CAST(d.DAY AS INT) AS DAY,
            CAST(d.DAY_OF_WEEK AS INT) AS DAY_OF_WEEK,
            CAST(d.IS_WEEKEND AS INT) AS IS_WEEKEND,
            c.RTE_ID,
            s.RTE_TYPE,
            s.DIST,
            s.RTE_STOP_COUNT,
            c.STOP_ID,
            s.DONG_ID,
            s.SGG_NM,
            s.RTE_SGG_COUNT,
            s.RTE_DONG_COUNT,
            s.STOP_TYPE,
            s.LAT,
            s.LOT,
            s.STOP_SEQ,
            s.LNKG_LEN,
            p.GTON_TNOPE,
            p.GTOFF_TNOPE,
            p.TOTAL_ON,
            p.TOTAL_OFF,
            c.BUS_OPR,
            c.BUS_OPR_00, c.BUS_OPR_01, c.BUS_OPR_02, c.BUS_OPR_03,
            c.BUS_OPR_04, c.BUS_OPR_05, c.BUS_OPR_06, c.BUS_OPR_07,
            c.BUS_OPR_08, c.BUS_OPR_09, c.BUS_OPR_10, c.BUS_OPR_11,
            c.BUS_OPR_12, c.BUS_OPR_13, c.BUS_OPR_14, c.BUS_OPR_15,
            c.BUS_OPR_16, c.BUS_OPR_17, c.BUS_OPR_18, c.BUS_OPR_19,
            c.BUS_OPR_20, c.BUS_OPR_21, c.BUS_OPR_22, c.BUS_OPR_23,
            dp.BUS_PSNG,
            dp.BUS_PSNG_00, dp.BUS_PSNG_01, dp.BUS_PSNG_02, dp.BUS_PSNG_03,
            dp.BUS_PSNG_04, dp.BUS_PSNG_05, dp.BUS_PSNG_06, dp.BUS_PSNG_07,
            dp.BUS_PSNG_08, dp.BUS_PSNG_09, dp.BUS_PSNG_10, dp.BUS_PSNG_11,
            dp.BUS_PSNG_12, dp.BUS_PSNG_13, dp.BUS_PSNG_14, dp.BUS_PSNG_15,
            dp.BUS_PSNG_16, dp.BUS_PSNG_17, dp.BUS_PSNG_18, dp.BUS_PSNG_19,
            dp.BUS_PSNG_20, dp.BUS_PSNG_21, dp.BUS_PSNG_22, dp.BUS_PSNG_23,
            (p.GTON_TNOPE + p.GTOFF_TNOPE) / NULLIF(c.BUS_OPR, 0) AS TARGET
        FROM agg_stop_passenger p
        JOIN latest_trip_count c
            ON p.BASE_YMD = c.BASE_YMD
            AND p.RTE_ID = c.RTE_ID
            AND p.STOP_ID = c.STOP_ID
        JOIN agg_route_stop s
            ON p.RTE_ID = s.RTE_ID
            AND p.STOP_ID = s.STOP_ID
        JOIN latest_dong_passenger dp
            ON p.BASE_YMD = dp.BASE_YMD
            AND s.DONG_ID = dp.DONG_ID
        JOIN dim_date d
            ON p.BASE_YMD = d.BASE_YMD
    """)
    fin_wide_table.write\
                    .format('bigquery')\
                    .option('temporaryGcsBucket', 'spark-pipeline-bucket')\
                    .option('temporaryGcsPath', 'temp')\
                    .mode('overwrite')\
                    .save('data-engineering-478006.spark_dataset.wide_table')
    
    spark.stop()

if __name__ == '__main__':
    create_wide_table()
