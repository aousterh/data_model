from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
sqlContext = SQLContext(sc)

# to read parquet file
df = sqlContext.read.parquet('/home/admin/zq-sample-data/outputs/merged.parquet')
df.printSchema()
df.groupBy("username").count().show(df.count(), False)
