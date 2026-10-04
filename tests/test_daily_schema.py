import pytest

from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, DateType

class TestDailySchema():
    base_dir = 'gs://my-spark-pipeline-bucket/parquet/daily'

    def test_route_stop_total_passenger_schema(self, spark, ds):
        route_stop_total_passenger = spark.read.parquet(f'{self.base_dir}/route_stop_total_passenger/dt={ds}')

        schema = StructType([
            StructField('USE_YMD', DateType(), True),
            StructField('RTE_ID_8', LongType(), True),
            StructField('RTE_NM', StringType(), True),
            StructField('RTE_NM_DETAIL', StringType(), True),
            StructField('STOP_ID', LongType(), True),
            StructField('STOP_ARS_NO', StringType(), True),
            StructField('STOP_NM', StringType(), True),
            StructField('STOP_SEQ', LongType(), True),
            StructField('GET_OFF', DoubleType(), True),
            StructField('GET_ON', DoubleType(), True),
        ])

        assert schema == route_stop_total_passenger.schema

    def test_dong_hour_passenger_schema(self, spark, ds):
        dong_hour_passenger = spark.read.parquet(f'{self.base_dir}/dong_hour_passenger/dt={ds}')

        schema = StructType([
            StructField('USE_YMD', DateType(), True),
            StructField('DONG_ID', LongType(), True),
            StructField('TOTAL_BUS_PSNG', DoubleType(), True),
        ] + [StructField(f'BUS_PSNG_{i:02d}', DoubleType(), True) for i in range(24)
        ])

        assert schema == dong_hour_passenger.schema

    def test_route_stop_sequence_schema(self, spark, ds):
        route_stop_sequence = spark.read.parquet(f'{self.base_dir}/route_stop_sequence/dt={ds}')

        schema = StructType([
            StructField('USE_YMD', DateType(), True),
            StructField('RTE_ID', LongType(), True),
            StructField('STOP_ID', LongType(), True),
            StructField('STOP_SEQ', LongType(), True),
            StructField('TOTAL_BUS_OPR', DoubleType(), True),
        ] + [StructField(f'BUS_OPR_{i:02d}', DoubleType(), True) for i in range(24)
        ])

        assert schema == route_stop_sequence.schema
