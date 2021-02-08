from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import os 
from pyspark.sql.functions import col,lit

def convert_columns_to_string(schema):
    print(schema)
    lst=[]
    
    for name in set(schema):
        lst.append("cast(`{col}` as string) as `{col}`".format(col=name))
        #if '\.' in name:
        #    lst.append("`{col}`".format(col=name))
        #else:
        #    lst.append(name)

    print(lst)
    return lst

def merge_schema(dir_path):
    idx = 0
    df = None
    cols1 = []
    cols2 = []
    required_cols = []
    print("\nProcessing files:")
    for dir in [d for d in os.listdir(dir_path)]:
        print("idx: " + str(idx) + " | path: " + dir_path + "/" + dir)
        # Get schema
        schema = spark.read.parquet(dir_path + "/" + dir).limit(0).schema

        # Read parquet file converting columns to string
        df_temp = spark.read.parquet(dir_path + "/" + dir)

        if idx == 0:
            df = df_temp
            cols1 = [c.name for c in schema]
            required_cols += cols1
        else:
            #find missing cols and union
            cols2 = [c.name for c in schema]
            required_cols += cols2
            print(set(required_cols))
            #df = df.selectExpr([col if col in cols1 else lit(None) for col in required_cols])
            df = df.selectExpr(convert_columns_to_string(required_cols)).union(df_temp.selectExpr(convert_columns_to_string(required_cols)))
            #cols1 = [c.name for c in df.schema]
        idx += 1
    return df

sc = SparkContext('local')
spark = SparkSession(sc)
data_path = "/home/admin/zq-sample-data/parquet"
#df = spark.read.parquet(merge_schema(data_path))
df = merge_schema(data_path)
df.printSchema()
