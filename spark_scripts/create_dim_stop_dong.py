from pyspark import SparkConf
from pyspark.sql import SparkSession

import pyspark.sql.functions as f
from shapely.wkt import loads as wkt_loads
from shapely.geometry import Point
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_spark_session():
    conf = SparkConf()
    conf.set('spark.app.name', 'Create Stop Dong Dimension Table')

    spark = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
    
    return spark

def create_dim_stop_dong():
    spark = create_spark_session()

    dataset = 'data-engineering-478006.spark_dataset'

    dim_bus_stop = spark.read.format('bigquery')\
                .load(f'{dataset}.dim_bus_stop')
    dim_dong = spark.read.format('bigquery')\
                .load(f'{dataset}.dim_dong')
    
    dim_bus_stop = dim_bus_stop.orderBy(f.col('UPDATED_AT').desc())\
                    .dropDuplicates(['STOP_ID'])
    dim_dong = dim_dong.orderBy(f.col('UPDATED_AT').desc())\
                .dropDuplicates(['DONG_ID'])

    temp_column_schema = StructType([
        StructField('DONG_ID', IntegerType(), True),
        StructField('SGG_NM', StringType(), True),
        StructField('DONG_NM', StringType(), True),
    ])

    dim_dong = dim_dong.select(f.col('DONG_ID'), f.col('SGG_NM'), f.col('DONG_NM'), f.col('GEOMETRY')).collect()
    dong_dict = [{'DONG_ID': i['DONG_ID'],
                'SGG_NM': i['SGG_NM'],
                'DONG_NM': i['DONG_NM'],
                'GEOMETRY': wkt_loads(i['GEOMETRY'])} for i in dim_dong]
    broadcast_dong_dict = spark.sparkContext.broadcast(dong_dict)

    def find_dong(lot, lat):
        if lot is None or lat is None:
            return (None, None, None)

        p = Point(lot, lat)
        for i in broadcast_dong_dict.value:
            if i['GEOMETRY'].contains(p):
                return (i['DONG_ID'], i['SGG_NM'], i['DONG_NM'])

        return (None, None, None)
    find_dong_udf = f.udf(find_dong, temp_column_schema)

    dim_stop_dong = dim_bus_stop.withColumn('temp_col', find_dong_udf(f.col('LOT'), f.col('LAT')))\
                                .withColumn('DONG_ID', f.col('temp_col.DONG_ID'))\
                                .withColumn('SGG_NM', f.col('temp_col.SGG_NM'))\
                                .withColumn('DONG_NM', f.col('temp_col.DONG_NM'))\
                                .select(f.col('STOP_ID'), f.col('STOP_NM'), f.col('STOP_TYPE'),
                                        f.col('DONG_ID'), f.col('SGG_NM'), f.col('DONG_NM'), f.col('UPDATED_AT'))
    
    dim_stop_dong.write\
                .format('bigquery')\
                .option('temporaryGcsBucket', 'spark-pipeline-bucket')\
                .option('temporaryGcsPath', 'temp')\
                .mode('append')\
                .save('data-engineering-478006.spark_dataset.dim_stop_dong')
    
    spark.stop()

if __name__ == '__main__':
    create_dim_stop_dong()
