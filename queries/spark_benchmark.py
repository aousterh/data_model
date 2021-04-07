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

merged_df_path = '/home/admin/zq-sample-data/outputs/merged'
parquet_path = '/home/admin/zq-sample-data/parquet'

#conf = SparkConf().set("spark.executor.memory","2g").set("spark.driver.memory","2g").setMaster("local")
sc = SparkContext("local")
#sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
#spark = SparkSession.builder.master("local[1]").config("spark.driver.memory","2g").config("spark.executor.memory","2g").getOrCreate()
spark = SparkSession.builder.master("local[1]").getOrCreate()

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
    df = sqlContext.read.parquet(merged_df_path)
    return df

def analytics(df):   
  #df.groupBy("`id.orig_h`").count().show(df.count(), False)
  df.select("`id.orig_h`").count()
  
  #over each individual parquet instead of the merged file
  #for root, dirs, files in os.walk(parquet_path, topdown = False):
   #   for name in files:
    #      df = spark.read.parquet(os.path.join(root, name))
     #     if has_column(df, "`id.orig_h`"):
      #        df.select("`id.orig_h`").count()
          




def search(df):

    df2 = spark.sql("""SELECT * 
    FROM MERGED
    WHERE `id.orig_h` = '10.128.0.19'
    ORDER BY ts
    LIMIT 5
    """)
    df2.count()

def discovery(df):
    for root, dirs, files in os.walk(parquet_path, topdown = False):
        for name in files:
           # print(name)
            df = spark.read.parquet(os.path.join(root, name))
            df.createOrReplaceTempView("Schema")
            df2 = spark.sql("""SELECT COUNT(*) FROM Schema""")
            df2.count()

def benchmark1(fn, df, num_iter=10):
    #warm up run
    #fn(df)
    _real = list()
    #_real, _sys, _user = list(), list(), list()
    for _ in range(num_iter):
        #_df = copy.deepcopy(df)
        t = unix_time(fn, df=df)
        _real.append(t["real"])
        #_sys.append(t["sys"])
        #_user.append(t["user"])
        print("{}".format(fn.__name__))
        print("{}".format(_real))
       # print("{}".format(round(statistics.mean(_user), 5)))
       # print("{}".format(round(statistics.mean(_sys), 5)))


def main():
    # TBD use sys.argv[0] for num_iter or dataset
    print("loading..")
    #df = unix_time(load)["return"]
    df = load()
    print("name,real,user,sys")
    print("------------------")

    print("Analytics query")
    print("count total number of records with each distinct source IP")
    benchmark1(analytics, df, num_iter=10)
    print("Search query")
    print("find all records with IP 10.128.19, sort by timestamp and return top 5")
    df = load()
    df.createOrReplaceTempView("MERGED") 
    benchmark1(search, df, num_iter=10)
    print("Data discovery query")
    print("count the number of records with each different schema")
    benchmark1(discovery, df, num_iter=10)

if __name__ == '__main__':
        main()

