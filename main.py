from pyspark.sql import SparkSession


def main() -> None:
    spark: SparkSession = (
        SparkSession
        .builder
        .master("local")
        .appName("Driver")
        .getOrCreate()
    )

    new_DS = spark.sparkContext.parallelize([1, 2, 3, 4])
    new_DS.saveAsTextFile("./from_local_1")
    spark.stop()

if __name__ == '__main__':
    main()
