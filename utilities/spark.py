from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def initialise_spark() -> SparkSession:
    """Initialises and returns a Spark Session based on configuration"""
    spark_config = SparkConf()

    # update spark config
    spark_config.set('spark.debug.maxToStringFields', '100')

    # build session
    spark = SparkSession.builder.appName('Titanic Classifier').config(conf=spark_config).getOrCreate()
    return spark


def get_current_session():
    return SparkSession.builder.getOrCreate()
