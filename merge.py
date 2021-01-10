import math
from pyspark.sql import Row
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

def get_repartition_factor(dir_size):
    block_size = sc._jsc.hadoopConfiguration().get("dfs.blocksize")
    print(dir_size, block_size)
    return 1
#    return math.ceil(int(dir_size)/int(block_size)) # returns 2

sc = SparkContext('local')
spark = SparkSession(sc)
df=spark.read.option("mergeSchema", "true").parquet("/home/admin/zq-sample-data/parquet")
df.repartition(get_repartition_factor(217894092)).write.parquet("/home/admin/zq-sample-data/outputs")
