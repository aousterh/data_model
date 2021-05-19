import time
import copy
from resource import getrusage as resource_usage, RUSAGE_SELF
from time import time as timestamp
from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pyspark
import os
from collections import defaultdict
import statistics
from pyspark import SparkConf
import sys
import time
from statistics import mean

merged_df_path = "../merged"
parquet_path = "../parquet"

#conf = SparkConf().set("spark.executor.memory","2g").set("spark.driver.memory","2g").setMaster("local")
#sc = SparkContext("local")
#sc = SparkContext(conf=conf)
#sqlContext = SQLContext(sc)
#spark = SparkSession.builder.master("local[1]").config("spark.driver.memory","2g").config("spark.executor.memory","2g").getOrCreate()
spark = SparkSession.builder.master("local[1]").config("spark.ui.port","4050").getOrCreate()


def unix_time(function, *args, **kwargs):
    '''Return `real`, `sys` and `user` elapsed time, like UNIX's command `time`
    You can calculate the amount of used CPU-time used by your
    function/callable by summing `user` and `sys`. `real` is just like the wall
    clock.
    Note that `sys` and `user`'s resolutions are limited by the resolution on
    the operating system's software clock (check `man 7 time` for more
    details).
    '''
    start_time, start_resources = timestamp(), resource_usage(RUSAGE_SELF)
    r = function(*args, **kwargs)
    end_resources, end_time = resource_usage(RUSAGE_SELF), timestamp()
    return {'return': r,
            'real': end_time - start_time,
            'sys': end_resources.ru_stime - start_resources.ru_stime,
            'user': end_resources.ru_utime - start_resources.ru_utime}

def has_column(df, col):
    try:
        df[col]
        return True
    except pyspark.sql.utils.AnalysisException:
        return False

#load the merged parquet into a df
def load():
    df = spark.read.parquet(merged_df_path)
    return df

def analytics(df, iter_num):   
   # df.groupBy("`id.orig_h`").count().show(df.count(), False
   df.select("`id.orig_h`").count()
  
  #over each individual parquet instead of the merged file
  #for root, dirs, files in os.walk(parquet_path, topdown = False):
   #   for name in files:
    #      df = spark.read.parquet(os.path.join(root, name))
     #     if has_column(df, "`id.orig_h`"):
      #        df.select("`id.orig_h`").count()

def search(df, iter_num):
   origin = sys.stdout
   df2 = spark.sql("""SELECT *
    FROM MERGED
    WHERE `id.orig_h` = '10.128.0.19'
    ORDER BY ts
    LIMIT 5
    """)
   sys.stdout = open('/dev/null', 'w')
   df2.show()
   sys.stdout = origin

def discovery(df, iter_num):
    for root, dirs, files in os.walk(parquet_path, topdown = False):
        for name in files:
           # print(name)
            df = spark.read.parquet(os.path.join(root, name))
            df.createOrReplaceTempView("Schema")
            df2 = spark.sql("""SELECT COUNT(*) FROM Schema""")
            df2.count()
    
def path(df, iter_num):
    origin = sys.stdout
    df2 = spark.sql("""SELECT * FROM MERGED
     WHERE _path = 'smb*' or _path = 'dce_rpc' """)
    sys.stdout = open('/dev/null', 'w')
    df2.show()
    sys.stdout = origin

def post(df, iter_num):
    origin = sys.stdout
    df2 = spark.sql("""SELECT ts, uid, id, method, uri, status_code
                    from MERGED
                    WHERE method = 'POST'
                    """)
    sys.stdout = open('/dev/null', 'w')
    df2.show()
    sys.stdout = origin
 
def file_not_null(df, iter_num):
    origin = sys.stdout
    df2 = spark.sql("""SELECT _path, tx_hosts, rx_hosts, conn_uids, mime_type, filename, md5, sha1
                    from MERGED
                    WHERE filename IS NOT NULL
                    """)
    sys.stdout = open('/dev/null', 'w')
    df2.show()
    sys.stdout = origin
    
def count_path(df, iter_num):
    origin = sys.stdout
    df2 = spark.sql("""SELECT _path, count(*)
                    from MERGED
                    GROUP BY _path
                    ORDER BY COUNT(*) DESC""")
    sys.stdout = open('/dev/null', 'w')
    df2.show()
    sys.stdout = origin
    
def path_dns(df, iter_num):
    origin = sys.stdout
    df5 = spark.sql("""SELECT query, count(*)
                    from MERGED
                    WHERE _path = 'dns'
                    GROUP BY query
                    ORDER BY COUNT(*) DESC""")
    sys.stdout = open('/dev/null', 'w')
    df5.show()
    sys.stdout = origin

def http_reqs(df, iter_num):
    origin = sys.stdout
    df2 = spark.sql("""SELECT DISTINCT `id.orig_h`, `id.resp_h`, `id.resp_p`, method, host, uri
                    FROM MERGED
                    WHERE _path = 'http'
                    """)
    sys.stdout = open('/dev/null', 'w')
    df2.show()
    sys.stdout = origin

def path_conn(df, iter_num):
    origin = sys.stdout
    df2 = spark.sql("""SELECT DISTINCT `id.orig_h`, `id.resp_h`, `id.resp_h`
                   FROM MERGED
                   WHERE _path = 'conn'
                   """)
    sys.stdout = open('/dev/null', 'w')
    df2.show()
    sys.stdout = origin

def total_bytes(df, iter_num):
    origin = sys.stdout
    df2 = spark.sql("""SELECT orig_bytes + resp_bytes AS total_bytes, uid, orig_bytes, resp_bytes
                    FROM MERGED
                    WHERE _path = 'conn'
                    ORDER BY total_bytes DESC""")
    sys.stdout = open('/dev/null', 'w')
    df2.show()
    sys.stdout = origin

def benchmark(fn, num_iter=100):
    #global spark
    _real = list()
    for i in range(num_iter):
       # os.system('sudo sh flush_cache.sh')
        df = load()
        if fn != analytics:
            df = df.createOrReplaceTempView("MERGED") 
        t = unix_time(fn, df=df, iter_num=i)
        if i >= 10:
            _real.append(t["real"])
        #if fn == search and i == 0:
            #df = t["return"]
        if i == num_iter - 1:
            print("{}".format(fn.__name__))
            print("{}".format(_real))
            print("{}".format(mean(_real)))
        #spark.stop()
        #time.sleep(5)
        #os.system('ps aux | grep spark')
        #spark = SparkSession.builder.master("local[1]").getOrCreate()

def main():
    # TBD use sys.argv[0] for num_iter or dataset
   
    print("loading..")
    #df = unix_time(load)["return"]
    #df = load()
    print("name,real")
    print("------------------")

    print("Analytics query")
    print("count total number of records with each distinct source IP")
    #benchmark(analytics, num_iter=100)
    
    print("Data discovery query")
    print("count the number of records with each different schema")
    #benchmark(discovery, num_iter=100)

    print("Windows Networking Activity")
    #benchmark(path, num_iter=100)

    print("HTTP Post Requests")
    #benchmark(post, num_iter=100)

    print("Activity overview")
    #benchmark(count_path, num_iter=100)

    print("File Activity")
    #benchmark(file_not_null, num_iter=100)

    print("Unique DNS queries")
    #benchmark(path_dns, num_iter=100)

    print("HTTP Requests")
    #benchmark(http_reqs, num_iter=100)

    print("Unique Network Connections")
    #benchmark(path_conn, num_iter=100)

    print("Connection Received Data")
    #benchmark(total_bytes, num_iter=100)
 
    print("Search query")
    print("find all records with IP 10.128.19, sort by timestamp and return top 5")
    #df.createOrReplaceTempView("MERGED") 
    benchmark(search, num_iter=100)
    
if __name__ == '__main__':
        main()

