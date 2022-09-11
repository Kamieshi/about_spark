from pyspark.sql import SparkSession

MASTER_STAND_ALONE = "spark://127.0.0.1:7077"
MASTER_LOCAL = "local[*]"
DRIVER_NME = "flask app"


def get_spark_session() -> SparkSession:
    session = (
        SparkSession
        .builder
        .master(MASTER_STAND_ALONE)
        .appName(DRIVER_NME)
        .config("spark.jars", "./jdbs_drivers/postgresql-42.5.0.jar")
        .getOrCreate()
    )
    return session
