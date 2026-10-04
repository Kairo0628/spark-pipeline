from pyspark import SparkConf
from pyspark.sql import SparkSession

import os
from dotenv import load_dotenv

load_dotenv()
PROJECT_ID = os.getenv('PROJECT_ID')

def create_spark_session():
    conf = SparkConf()
    conf.set('spark.app.name', 'Create dim_dong Table')

    spark = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
    
    return spark

def main():
    spark = create_spark_session()

    raw_dir = 'gs://my-spark-pipeline-bucket/raw_data'

    hangjeongdong = spark.read\
        .option('multiLine', 'true')\
        .json(f'{raw_dir}/hangjeongdong/HangJeongDong_ver20260701.geojson')
    hangjeongdong.createOrReplaceTempView('hangjeongdong')

    hangjeongdong = spark.sql("""
        WITH exploded AS (
            SELECT
                EXPLODE(features) AS features
            FROM hangjeongdong
        )
        SELECT
            CAST(features.properties.adm_cd AS LONG) AS DONG_ID,
            CAST(features.properties.adm_cd2 AS LONG) AS DONG_ID2,
            REGEXP_EXTRACT(features.properties.adm_nm, '([^ ]+)$', 1) AS DONG_NM,
            features.properties.adm_nm AS ADM_NM,
            CAST(features.properties.sido AS LONG) AS SIDO_ID,
            features.properties.sidonm AS SIDO_NM,
            CAST(features.properties.sgg AS LONG) AS SGG_ID,
            features.properties.sggnm AS SGG_NM,
            TO_JSON(features.geometry) AS GEOMETRY
        FROM exploded
    """)

    hangjeongdong.show(5)
    hangjeongdong.printSchema()
    
    hangjeongdong.write\
        .format('bigquery')\
        .option('temporaryGcsBucket', 'my-spark-pipeline-bucket')\
        .option('temporaryGcsPath', 'temp')\
        .mode('overwrite')\
        .save(f'{PROJECT_ID}.spark_dataset.dim_dong')
    
    spark.stop()

if __name__ == '__main__':
    main()
