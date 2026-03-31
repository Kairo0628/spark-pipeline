from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline, PipelineModel

def create_spark_session():
    conf = SparkConf()
    conf.set('spark.app.name', 'ML Pipeline')

    spark = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
    
    return spark

def ml_pipeline():
    spark = create_spark_session()

    base_table = spark.read.format('bigquery')\
                .load('data-engineering-478006.spark_dataset.base_table')
    base_table = base_table.withColumn('STOP_POS_RATIO', f.col('STOP_SEQ') / f.col('RTE_STOP_COUNT'))

    # numerical: DIST, RTE_STOP_COUNT, RTE_SGG_COUNT, RTE_DONG_COUNT,
    #            LAT, LOT, LNKG_LEN, AVG_7DAY_*, BUS_PSNG*, STOP_POS_RATIO
    # numerical, no_scaled: STOP_SEQ

    # categorical: MONTH, DAY, DAY_OF_WEEK, IS_WEEKEND
    # categorical, string_index: RTE_ID, RTE_TYPE, STOP_ID, DONG_ID, SGG_NM, STOP_TYPE

    # 훈련 날짜 지정. 여기서는 최신 2일치를 테스트셋으로 분리
    train_raw = base_table.filter('BASE_YMD <= "20260311"')
    test = base_table.filter('BASE_YMD > "20260311"')

    train, valid = train_raw.randomSplit([0.8, 0.2], seed = 42)
    train = train.drop('BASE_YMD', 'YEAR')
    valid = valid.drop('BASE_YMD', 'YEAR')

    cat_inputCols = ['RTE_ID', 'RTE_TYPE', 'STOP_ID', 'DONG_ID', 'SGG_NM', 'STOP_TYPE']
    cat_outputCols = [f'{i}_INDEX' for i in cat_inputCols]
    categories = StringIndexer(inputCols = cat_inputCols, outputCols = cat_outputCols,
                               handleInvalid = 'keep')
    
    num_inputCols = [i for i in train.columns if i not in
                     cat_inputCols + ['MONTH', 'DAY', 'DAY_OF_WEEK', 'IS_WEEKEND', 'STOP_SEQ', 'TARGET']]
    num_assembler = VectorAssembler(inputCols = num_inputCols, outputCol = 'num_features')
    numerics = StandardScaler(inputCol = 'num_features', outputCol = 'scaled_features')

    fin_features = cat_outputCols + ['scaled_features'] + ['MONTH', 'DAY', 'DAY_OF_WEEK', 'IS_WEEKEND', 'STOP_SEQ']
    fin_assembler = VectorAssembler(inputCols = fin_features, outputCol = 'features')

    rf_clf = RandomForestClassifier(
        featuresCol = 'features',
        labelCol = 'TARGET',
        predictionCol = 'prediction',
        maxDepth = 5,
        maxBins = 11000,
        numTrees = 20,
        seed = 42
    )

    pipeline = Pipeline(stages = [
        categories,
        num_assembler,
        numerics,
        fin_assembler,
        rf_clf
    ])

    evaluator = MulticlassClassificationEvaluator(
        predictionCol = 'prediction',
        labelCol = 'TARGET',
        metricName = 'accuracy'
    )

    train_model = pipeline.fit(train)
    valid_pred = train_model.transform(valid)
    print('Train Accuracy:', evaluator.evaluate(valid_pred))

    fin_model = pipeline.fit(train_raw)
    fin_pred = fin_model.transform(test)
    print('Final Accuracy:', evaluator.evaluate(fin_pred))

    fin_model.write()\
            .overwrite()\
            .save('gs://spark-pipeline-bucket/ml_model')
    
    load_model = PipelineModel.load('gs://spark-pipeline-bucket/ml_model')
    load_pred = load_model.transform(test)
    print('Model Save Test, Accuracy:', evaluator.evaluate(load_pred))

    spark.stop()

if __name__ == '__main__':
    ml_pipeline()
