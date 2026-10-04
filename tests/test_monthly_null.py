import pytest

import pyspark.sql.functions as f

class TestMonthlyNull():
    base_dir = 'gs://my-spark-pipeline-bucket/parquet/monthly'

    def test_stop_master_null(self, spark, ds):
        stop_master = spark.read.parquet(f'{self.base_dir}/stop_master/dt={ds}')

        assert stop_master.count() > 0

        assert stop_master.filter(f.col('STOP_ID').isNull()).limit(1).count() == 0
        assert stop_master.filter(f.col('BUS_ARVL_INFO_GUIDEM_INSTL').isNull()).limit(1).count() == 0

    def test_route_id_null(self, spark, ds):
        route_id = spark.read.parquet(f'{self.base_dir}/route_id/dt={ds}')

        assert route_id.count() > 0

        assert route_id.filter(f.col('RTE_ID').isNull()).limit(1).count() == 0

    def test_route_stop_hour_passenger_null(self, spark, ds):
        route_stop_hour_passenger = spark.read.parquet(f'{self.base_dir}/route_stop_hour_passenger/dt={ds}')

        assert route_stop_hour_passenger.count() > 0

        assert route_stop_hour_passenger.filter(f.col('STOP_ID').isNull()).limit(1).count() == 0
        assert route_stop_hour_passenger.filter(f.col('STOP_NM').isNull()).limit(1).count() == 0
        assert route_stop_hour_passenger.filter(f.col('STOP_SEQ').isNull()).limit(1).count() == 0
