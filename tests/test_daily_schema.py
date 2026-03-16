import pytest

from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

class TestDailySchema():
    base_dir = 'gs://spark-pipeline-bucket/parquet/daily'

    def test_bus_dong_passenger_schema(self, spark, ds):
        bus_dong_passenger = spark.read.parquet(f'{self.base_dir}/bus_dong_passenger/dt={ds}')

        schema = StructType([
            StructField('BASE_YMD', StringType(), True),
            StructField('DONG_ID', IntegerType(), True),
            StructField('BUS_PSNG', FloatType(), True)
            ] + [StructField(f'BUS_PSNG_{i:02d}', FloatType(), True) for i in range(24)]
        )

        assert schema == bus_dong_passenger.schema

    def test_bus_stop_passenger_schema(self, spark, ds):
        bus_stop_passenger = spark.read.parquet(f'{self.base_dir}/bus_stop_passenger/dt={ds}')

        schema = StructType([
            StructField('BASE_YMD', StringType(), True),
            StructField('RTE_ID', IntegerType(), True),
            StructField('RTE_NM_DETAIL', StringType(), True),
            StructField('STOP_ID', IntegerType(), True),
            StructField('STOP_NO', IntegerType(), True),
            StructField('STOPS_NM_DETAIL', StringType(), True),
            StructField('GTON_TNOPE', FloatType(), True),
            StructField('GTOFF_TNOPE', FloatType(), True),
            StructField('RTE_NM', StringType(), True),
        ])

        assert schema == bus_stop_passenger.schema

    def test_bus_stop_trip_count_schema(self, spark, ds):
        bus_stop_trip_count = spark.read.parquet(f'{self.base_dir}/bus_stop_trip_count/dt={ds}')

        schema = StructType([
            StructField('BASE_YMD', StringType(), True),
            StructField('RTE_ID', IntegerType(), True),
            StructField('STOP_ID', IntegerType(), True),
            StructField('BUS_OPR', FloatType(), True)
        ] + [StructField(f'BUS_OPR_{i:02d}', FloatType(), True) for i in range(24)
        ] + [StructField('STOP_SEQ', FloatType(), True)
        ])

        assert schema == bus_stop_trip_count.schema

    def test_dong_foot_traffic_schema(self, spark, ds):
        dong_foot_traffic = spark.read.parquet(f'{self.base_dir}/dong_foot_traffic/dt={ds}')

        schema = StructType([
            StructField('BASE_YMD', StringType(), True),
            StructField('HOUR', StringType(), True),
            StructField('DONG_ID', IntegerType(), True),
            StructField('TOT_LVPOP_CO', FloatType(), True)
        ] + [StructField(f'MALE_F{i * 10}T{i * 10 + 9}_LVPOP_CO', FloatType(), True) for i in range(7)
        ] + [StructField('MALE_F70T74_LVPOP_CO', FloatType(), True)
        ] + [StructField(f'FEMALE_F{i * 10}T{i * 10 + 9}_LVPOP_CO', FloatType(), True) for i in range(7)
        ] + [StructField('FEMALE_F70T74_LVPOP_CO', FloatType(), True)
        ])

        assert schema == dong_foot_traffic.schema
