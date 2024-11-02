from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("data-eng-spark-data-skew") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.shuffle.partitions", "4")  \
        .getOrCreate()

    df = spark.read.csv('/opt/bitnami/spark/data/dados.csv', header=True, inferSchema=True)
    df = df.repartition(4)  # Reduzir o número de partições iniciais

    agrupado = df.groupBy('chave').count()
    agrupado.show()

    input("Pressione Enter para finalizar...")

    spark.stop()
