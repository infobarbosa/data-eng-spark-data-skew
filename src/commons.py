from pyspark.sql import SparkSession
from pyspark import SparkConf

MASTER_ENDPOINT = "spark://spark-master:7077"
APP_NAME="data-eng-spark-data-skew"

def create_spark_session_with_aqe_disabled() -> SparkSession:
    conf = SparkConf() \
        .set("spark.driver.memory", "4G") \
        .set("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .set("spark.sql.shuffle.partitions", "200") \
        .set("spark.sql.adaptive.enabled", "false") \
        .set("spark.sql.files.maxPartitionBytes", "8M") \

    spark_session = SparkSession\
        .builder\
        .master(MASTER_ENDPOINT)\
        .config(conf=conf)\
        .appName(APP_NAME) \
        .getOrCreate()

    return spark_session


def create_spark_session_with_aqe_skew_join_enabled() -> SparkSession:
    conf = SparkConf() \
        .set("spark.driver.memory", "4G") \
        .set("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .set("spark.sql.shuffle.partitions", "201") \
        .set("spark.sql.adaptive.enabled", "true") \
        .set("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .set("spark.sql.adaptive.skewJoin.enabled", "true") \
        .set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3") \
        .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "12K")

    spark_session = SparkSession\
        .builder\
        .master(MASTER_ENDPOINT)\
        .config(conf=conf)\
        .appName(APP_NAME) \
        .getOrCreate()

    return spark_session


def create_spark_session_with_aqe_enabled() -> SparkSession:
    conf = SparkConf() \
        .set("spark.driver.memory", "4G") \
        .set("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .set("spark.sql.shuffle.partitions", "201") \
        .set("spark.sql.adaptive.enabled", "true") \
        .set("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .set("spark.sql.adaptive.skewJoin.enabled", "true") \
        .set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3") \
        .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "16M")

    spark_session = SparkSession\
        .builder\
        .master(MASTER_ENDPOINT)\
        .config(conf=conf)\
        .appName(APP_NAME) \
        .getOrCreate()

    return spark_session