import pytest

from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, DateType

class TestMonthlySchema():
    base_dir = 'gs://my-spark-pipeline-bucket/parquet/monthly'

    def test_stop_master_schema(self, spark, ds):
        stop_master = spark.read.parquet(f'{self.base_dir}/stop_master/dt={ds}')

        schema = StructType([
            StructField('STOP_ID', LongType(), True),
            StructField('STOP_NM', StringType(), True),
            StructField('STOP_ARS_NO', StringType(), True),
            StructField('STOP_TYPE', StringType(), True),
            StructField('LAT', DoubleType(), True),
            StructField('LOT', DoubleType(), True),
            StructField('BUS_ARVL_INFO_GUIDEM_INSTL', StringType(), True),
        ])

        assert schema == stop_master.schema

    def test_route_id_schema(self, spark, ds):
        route_id = spark.read.parquet(f'{self.base_dir}/route_id/dt={ds}')

        schema = StructType([
            StructField('RTE_ID', LongType(), True),
            StructField('RTE_NM', StringType(), True),
        ])

        assert schema == route_id.schema

    def test_route_stop_hour_passenger_schema(self, spark, ds):
        route_stop_hour_passenger = spark.read.parquet(f'{self.base_dir}/route_stop_hour_passenger/dt={ds}')

        schema = StructType([
            StructField('USE_YM', StringType(), True),
            StructField('RTE_NM', StringType(), True),
            StructField('RTE_NM_DETAIL', StringType(), True),
            StructField('STOP_ID', LongType(), True),
            StructField('STOP_ARS_NO', StringType(), True),
            StructField('STOP_NM', StringType(), True),
            StructField('STOP_SEQ', LongType(), True),
            StructField('TRFC_MNS_TYPE_CD', StringType(), True),
            StructField('TRFC_MNS_TYPE_NM', StringType(), True)
        ] + [j for i in range(24)
             for j in (
                StructField(f'HR_{i}_GET_OFF', DoubleType(), True),
                StructField(f'HR_{i}_GET_ON', DoubleType(), True)
             )
        ])

        assert schema == route_stop_hour_passenger.schema
