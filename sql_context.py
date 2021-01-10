from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
sqlContext = SQLContext(sc)

# to read parquet file
df = sqlContext.read.parquet('/home/admin/zq-sample-data/outputs/part-00000-e200e06c-c753-43f3-9175-c6fc8201937a-c000.snappy.parquet')

df.printSchema()
df.show()

df.select("files").show()
