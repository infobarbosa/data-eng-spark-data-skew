import logging
import time
from pyspark.sql import SparkSession
from commons import create_spark_session_with_aqe_disabled, create_spark_session_with_aqe_skew_join_enabled, create_spark_session_with_aqe_enabled

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger("data-eng-spark-data-skew")

if __name__ == "__main__":
    spark = create_spark_session_with_aqe_disabled()
    start_time = time.time()

    df = spark.read.csv('/opt/bitnami/spark/data/dados.csv', header=True, inferSchema=True)
    
    logger.info(f"numero inicial de particoes: {df.rdd.getNumPartitions()}")

#    df = df.repartition(50)

#    print(f"numero final de particoes: {df.rdd.getNumPartitions()}")

    agrupado = df.groupBy('chave').count()
    agrupado.show()
    logger.info(f"tempo total (em segundos) para processar o job: {time.time() - start_time}")

    input("Pressione Enter para finalizar...")

    spark.stop()
