import pytest

from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

class TestMonthlySchema():
    base_dir = 'gs://spark-pipeline-bucket/parquet/monthly'

    def test_bus_route_stop_schema(self, spark, ds):
        bus_route_stop = spark.read.parquet(f'{self.base_dir}/bus_route_stop/dt={ds}')

        schema = StructType([
            StructField('LNKG_LEN', IntegerType(), True),
            StructField('RTE_ID', IntegerType(), True),
            StructField('STOP_ID', IntegerType(), True),
            StructField('STOP_SEQ', IntegerType(), True)
        ])

        assert schema == bus_route_stop.schema

    def test_bus_route_schema(self, spark, ds):
        bus_route = spark.read.parquet(f'{self.base_dir}/bus_route/dt={ds}')

        schema = StructType([
            StructField('DIST', FloatType(), True),
            StructField('RTE_ID', IntegerType(), True),
            StructField('RTE_NM', StringType(), True),
            StructField('RTE_TYPE', StringType(), True)
        ])

        assert schema == bus_route.schema

    def test_bus_stop_schema(self, spark, ds):
        bus_stop = spark.read.parquet(f'{self.base_dir}/bus_stop/dt={ds}')

        schema = StructType([
            StructField('LAT', FloatType(), True),
            StructField('LOT', FloatType(), True),
            StructField('STOP_ID', IntegerType(), True),
            StructField('STOP_NM', StringType(), True),
            StructField('STOP_TYPE', StringType(), True),
            StructField('STOP_NO', IntegerType(), True),
            StructField('ARR_INFO', StringType(), True)
        ])

        assert schema == bus_stop.schema

    def test_dong_info_schema(self, spark, ds):
        dong_info = spark.read.parquet(f'{self.base_dir}/dong_info/dt={ds}')

        schema = StructType([
            StructField('CGG_NM', StringType(), True),
            StructField('CTPV_NM', StringType(), True),
            StructField('DONG_ID', IntegerType(), True),
            StructField('DONG_NM', StringType(), True)
        ])

        assert schema == dong_info.schema
