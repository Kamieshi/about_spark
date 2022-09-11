from typing import List

from pyspark.sql.types import StructType
from pyspark.pandas import DataFrame
from pyspark.sql import functions as F
from pyspark import Row
from spark.get_spark_session import get_spark_session


class Logs:
    session = get_spark_session()

    @classmethod
    def __get_logs_df(cls) -> DataFrame:
        df = (cls.session.
              read.
              format("jdbc")
              .option("url", "jdbc:postgresql://172.27.0.2:5432/postgres")
              .option("user", "user")
              .option("password", "password").option("driver", "org.postgresql.Driver")
              .option("dbtable", "test")
              .load()
              ).repartition(3)
        return df

    @classmethod
    def schema(cls):
        schema: StructType = cls.__get_logs_df().schema
        return schema.json()

    @classmethod
    def get_logs(cls, level="") -> List[Row]:
        df = (cls.session.
              read.
              format("jdbc")
              .option("url", "jdbc:postgresql://172.27.0.2:5432/postgres")
              .option("user", "user")
              .option("password", "password").option("driver", "org.postgresql.Driver")
              .option("dbtable", "test")
              .option("numPartitions", "3")
              .option("partitionColumn", "id_pk")
              .option("upperBound", 3000)
              .option("lowerBound", 0)
              .load()
              )
        print([len(data) for data in df.rdd.glom().collect()])
        if level:
            df = df.filter(F.col("level") == level)
        return df.collect()

