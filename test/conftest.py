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
    #conf.set('spark.master', 'local[*]')
    conf.set('spark.app.name', 'Pyspark Testing')

    session = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
    yield session
