from pyspark.sql import SparkSession

session = (
    SparkSession
    .builder
    .appName("Driver_")
    .config("spark.jars","./postgresql-42.5.0.jar")
    .getOrCreate()
)
#%%
df = session.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres")\
    .option("dbtable", "test") \
    .option("user", "user") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .load()
#%%
df.write.csv("./output")
session.stop()