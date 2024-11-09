docker exec -it spark-master mkdir -p /opt/bitnami/spark/app/data-eng-spark-data-skew
docker cp src/commons.py spark-master:/opt/bitnami/spark/app/data-eng-spark-data-skew/commons.py
docker cp src/job_spark.py spark-master:/opt/bitnami/spark/app/data-eng-spark-data-skew/job_spark.py
