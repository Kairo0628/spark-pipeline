import pytest

import pyspark.sql.functions as f

class TestMonthlyNull():
    base_dir = 'gs://spark-pipeline-bucket/parquet/monthly'

    def test_bus_route_stop_null(self, spark, ds):
        bus_route_stop = spark.read.parquet(f'{self.base_dir}/bus_route_stop/dt={ds}')

        assert bus_route_stop.count() > 0

        assert bus_route_stop.filter(f.col('RTE_ID').isNull()).limit(1).count() == 0
        assert bus_route_stop.filter(f.col('STOP_ID').isNull()).limit(1).count() == 0

    def test_bus_route_null(self, spark, ds):
        bus_route = spark.read.parquet(f'{self.base_dir}/bus_route/dt={ds}')

        assert bus_route.count() > 0

        assert bus_route.filter(f.col('RTE_ID').isNull()).limit(1).count() == 0

    def test_bus_stop_null(self, spark, ds):
        bus_stop = spark.read.parquet(f'{self.base_dir}/bus_stop/dt={ds}')

        assert bus_stop.count() > 0

        assert bus_stop.filter(f.col('STOP_ID').isNull()).limit(1).count() == 0

    def test_dong_info_null(self, spark, ds):
        dong_info = spark.read.parquet(f'{self.base_dir}/dong_info/dt={ds}')

        assert dong_info.count() > 0

        assert dong_info.filter(f.col('DONG_ID').isNull()).limit(1).count() == 0
