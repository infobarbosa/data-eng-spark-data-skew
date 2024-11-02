from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ExemploDataSkew") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    df = spark.read.csv('/opt/bitnami/spark/data/dados.csv', header=True, inferSchema=True)
    agrupado = df.groupBy('chave').count()
    agrupado.show()

    input("Pressione Enter para finalizar...")

    spark.stop()
