import pytest

import pyspark.sql.functions as f

class TestMonthlyNull():
    base_dir = 'gs://my-spark-pipeline-bucket/parquet/daily'

    def test_route_stop_total_passenger_null(self, spark, ds):
        route_stop_total_passenger = spark.read.parquet(f'{self.base_dir}/route_stop_total_passenger/dt={ds}')

        assert route_stop_total_passenger.count() > 0

        assert route_stop_total_passenger.filter(f.col('USE_YMD').isNull()).limit(1).count() == 0
        assert route_stop_total_passenger.filter(f.col('RTE_ID_8').isNull()).limit(1).count() == 0
        assert route_stop_total_passenger.filter(f.col('STOP_ID').isNull()).limit(1).count() == 0
        assert route_stop_total_passenger.filter(f.col('STOP_NM').isNull()).limit(1).count() == 0
        assert route_stop_total_passenger.filter(f.col('STOP_SEQ').isNull()).limit(1).count() == 0

    def test_dong_hour_passenger_null(self, spark, ds):
        dong_hour_passenger = spark.read.parquet(f'{self.base_dir}/dong_hour_passenger/dt={ds}')

        assert dong_hour_passenger.count() > 0

        assert dong_hour_passenger.filter(f.col('USE_YMD').isNull()).limit(1).count() == 0
        assert dong_hour_passenger.filter(f.col('DONG_ID').isNull()).limit(1).count() == 0

    def test_route_stop_sequence_null(self, spark, ds):
        route_stop_sequence = spark.read.parquet(f'{self.base_dir}/route_stop_sequence/dt={ds}')

        assert route_stop_sequence.count() > 0

        assert route_stop_sequence.filter(f.col('USE_YMD').isNull()).limit(1).count() == 0
        assert route_stop_sequence.filter(f.col('RTE_ID').isNull()).limit(1).count() == 0
        assert route_stop_sequence.filter(f.col('STOP_ID').isNull()).limit(1).count() == 0
        assert route_stop_sequence.filter(f.col('STOP_SEQ').isNull()).limit(1).count() == 0
