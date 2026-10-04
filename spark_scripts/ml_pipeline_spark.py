from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline, PipelineModel

import os
from dotenv import load_dotenv

load_dotenv()
PROJECT_ID = os.getenv('PROJECT_ID')

def create_spark_session():
    conf = SparkConf()
    conf.set('spark.app.name', 'ML Pipeline')

    spark = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
    
    return spark

def ml_pipeline():
    spark = create_spark_session()

    bigquery_dir = f'{PROJECT_ID}.spark_dataset'

    feature_df = spark.read.format('bigquery')\
        .load(f'{bigquery_dir}.staging_feature_route_stop_psng_opr')\
        .repartition(6)\
        .cache()

    # 훈련 데이터셋 분리
    # 날짜를 기준으로 데이터를 먼저 8:2로 분리
    day_threshold = feature_df.approxQuantile('DATE', [0.8], 0)[0]
    train_raw = feature_df.filter(f'DATE <= {day_threshold}')\
        .drop('DATE')\
        .cache()
    test = feature_df.filter(f'DATE > {day_threshold}')\
        .drop('DATE')\
        .cache()
    
    train, valid = train_raw.randomSplit([0.8, 0.2], seed = 42)
    train.cache()

    # Categorical Variables
    cat_cols_input = ['RTE_NM_DETAIL', 'TRFC_MNS_TYPE_NM', 'STOP_ID', 'STOP_ARS_NO', 'STOP_TYPE',
                        'BUS_ARVL_INFO_GUIDEM_INSTL', 'DONG_NM', 'SGG_NM', 'STOP_TYPE_INFRA', 'STOP_RTE_TYPE']
    cat_cols_output = [f'INDEX_{i}' for i in cat_cols_input]
    cat_indexer = StringIndexer(
        inputCols = cat_cols_input,
        outputCols = cat_cols_output,
        handleInvalid = 'keep'
    )

    # Numerical Variables
    num_cols_add = []
    for i in range(24):
        n = str(i).zfill(2)
        num_cols_add.append(f'MONTHLY_RTE_STOP_{n}_OFF_PSNG')
        num_cols_add.append(f'MONTHLY_RTE_STOP_{n}_ON_PSNG')
        num_cols_add.append(f'DONG_BUS_PSNG_{n}')

    num_cols_input = ['STOP_SEQ', 'LAT', 'LOT', 'DONG_TOTAL_BUS_PSNG', 'DAY', 'DAY_OF_WEEK', 'IS_WEEKEND',
                    'MONTHLY_TOTAL_OFF', 'MONTHLY_TOTAL_ON', 'MONTHLY_PEAK_PSNG_RATIO', 'RTE_STOP_RATIO'] + num_cols_add
    num_assembler = VectorAssembler(
        inputCols = num_cols_input,
        outputCol = 'num_features'
    )
    num_scaler = StandardScaler(
        inputCol = 'num_features',
        outputCol = 'scaled_num_features'
    )

    # 최종 벡터 컬럼 생성
    fin_features = cat_cols_output + ['scaled_num_features']
    fin_assembler = VectorAssembler(
        inputCols = fin_features,
        outputCol = 'features'
    )

    rf_reg = RandomForestRegressor(
        featuresCol = 'features',
        labelCol = 'TARGET',
        seed = 42,
        maxBins = 10000
    )

    pipeline = Pipeline(stages = [
        cat_indexer,
        num_assembler,
        num_scaler,
        fin_assembler,
        rf_reg
    ])

    model = pipeline.fit(train)

    test.count()
    feature_df.unpersist()
    train.unpersist()

    pred = model.transform(valid)

    evaluator = RegressionEvaluator(
        predictionCol = 'prediction',
        labelCol = 'TARGET',
        metricName = 'rmse',
    )

    rmse = evaluator.evaluate(pred)
    print('Train RMSE:', rmse)

    # 모델 훈련 및 테스트 완료. 전체 훈련 데이터(Train + Valid)로 다시 훈련
    fin_model = pipeline.fit(train_raw)
    fin_pred = fin_model.transform(test)

    rmse = evaluator.evaluate(fin_pred)
    print('Final RMSE:', rmse)

    # 모델 저장
    fin_model.write()\
            .overwrite()\
            .save('gs://my-spark-pipeline-bucket/ml_model')

    train_raw.unpersist()
    
    # 저장된 모델 불러오기 및 이전 결과와 동일한지 테스트
    load_model = PipelineModel.load('gs://my-spark-pipeline-bucket/ml_model')
    load_pred = load_model.transform(test)

    rmse = evaluator.evaluate(load_pred)
    print('Model Load, RMSE:', rmse)

    spark.stop()

if __name__ == '__main__':
    ml_pipeline()
