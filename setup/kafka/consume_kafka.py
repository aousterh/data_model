from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "test").load()
df.printSchema()
df.writeStream.format("parquet").option("checkpointLocation", "checkpoint").option("path", "./parquet").start()

