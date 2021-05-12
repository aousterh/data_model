from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pyspark
import os
from pyspark.sql.functions import desc


parquet_path = "../parquet"
merged_df_path = '../merged'

sc = SparkContext('local')
sqlContext = SQLContext(sc)
spark = SparkSession.builder.master("local[1]").getOrCreate()

# to read merged parquet file as a df
df = sqlContext.read.parquet(merged_df_path)

print("Analytics query")
print("count total number of records with each distinct source IP")
#df.groupBy("`id.orig_h`").count().show(df.count(), False)
#df.groupBy("_path").count().sort(desc("count")).show(df.count(),False)


print("Search query")
print("find all records with IP 10.128.19, sort by timestamp and return top 5")
df.createOrReplaceTempView("MERGED")
df2 = spark.sql("""SELECT * 
FROM MERGED
WHERE `id.orig_h` = '10.128.0.19'
ORDER BY ts
LIMIT 5""")
df3 = spark.sql("""SELECT *
        FROM MERGED
        WHERE _path = 'smb*' or _path = 'dce_rpc'
        """)
df4 = spark.sql("""SELECT _path, count(*)
        from MERGED
        GROUP BY _path
        ORDER BY COUNT(*) DESC""")
df5 = spark.sql("""SELECT query, count(*)
        from MERGED
        WHERE _path = 'dns'
        GROUP BY query
        ORDER BY COUNT(*) DESC""")
df6 = spark.sql("""SELECT ts, uid, id, method, uri, status_code
        from MERGED
        WHERE method = 'POST'
        """)
df7 = spark.sql("""SELECT _path, tx_hosts, rx_hosts, conn_uids, mime_type, filename, md5, sha1
        from MERGED
        WHERE filename IS NOT NULL
        """)
df7.show()

print("Data discovery query")
print("count the number of records with each different schema")
for root, dirs, files in os.walk(parquet_path, topdown = False):
    for name in files:
        print(name)
        df = spark.read.parquet(os.path.join(root, name))
        df.createOrReplaceTempView("Schema")
        df2 = spark.sql("""SELECT COUNT(*) FROM Schema""")
        df2.show()
