import pytest

import pyspark.sql.functions as f

class TestMonthlyNull():
    base_dir = 'gs://spark-pipeline-bucket/parquet/daily'

    def test_bus_dong_passenger_null(self, spark, ds):
        bus_dong_passenger = spark.read.parquet(f'{self.base_dir}/bus_dong_passenger/dt={ds}')

        assert bus_dong_passenger.count() > 0

        assert bus_dong_passenger.filter(f.col('BASE_YMD').isNull()).limit(1).count() == 0
        assert bus_dong_passenger.filter(f.col('DONG_ID').isNull()).limit(1).count() == 0

    def test_bus_stop_passenger_null(self, spark, ds):
        bus_stop_passenger = spark.read.parquet(f'{self.base_dir}/bus_stop_passenger/dt={ds}')

        assert bus_stop_passenger.count() > 0

        assert bus_stop_passenger.filter(f.col('BASE_YMD').isNull()).limit(1).count() == 0
        assert bus_stop_passenger.filter(f.col('RTE_ID').isNull()).limit(1).count() == 0
        assert bus_stop_passenger.filter(f.col('STOP_ID').isNull()).limit(1).count() == 0

    def test_bus_stop_trip_count_null(self, spark, ds):
        bus_stop_trip_count = spark.read.parquet(f'{self.base_dir}/bus_stop_trip_count/dt={ds}')

        assert bus_stop_trip_count.count() > 0

        assert bus_stop_trip_count.filter(f.col('BASE_YMD').isNull()).limit(1).count() == 0
        assert bus_stop_trip_count.filter(f.col('RTE_ID').isNull()).limit(1).count() == 0
        assert bus_stop_trip_count.filter(f.col('STOP_ID').isNull()).limit(1).count() == 0

    def test_dong_foot_traffic_null(self, spark, ds):
        dong_foot_traffic = spark.read.parquet(f'{self.base_dir}/dong_foot_traffic/dt={ds}')

        assert dong_foot_traffic.count() > 0

        assert dong_foot_traffic.filter(f.col('BASE_YMD').isNull()).limit(1).count() == 0
        assert dong_foot_traffic.filter(f.col('DONG_ID').isNull()).limit(1).count() == 0
