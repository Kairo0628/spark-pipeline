import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession

def pytest_addoption(parser):
    parser.addoption(
        '--ds',
        action = 'store',
        default = None,
        help = 'Airflow의 Execution Date(Logical Date)'
    )

@pytest.fixture
def ds(request):
    return request.config.getoption('--ds')

@pytest.fixture
def spark():
    conf = SparkConf()
    #conf.set('spark.master', 'spark://10.128.0.8:7077')
    conf.set('spark.app.name', 'Pyspark Testing')
    conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    conf.set('spark.jars.packages', 'com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5')
    conf.set('spark.driver.userClassPathFirst', 'true')
    conf.set('spark.executor.userClassPathFirst', 'true')

    session = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
    yield session
