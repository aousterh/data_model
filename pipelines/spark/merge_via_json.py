import os
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
# running this script merges multiple parquet files into a single json dataframe, of which the schema is written to ../../merged in the parquet format

def merge_schemas(dir_path, spark):
    """
    Input: 
    - dir_path: Entity path containing partitions.
    - spark: Spark Session to use
   
    Output: JSON RDD
    Returns a JSON RDD containing the union of all partitions with columns converted to String.
    """
    
    idx = 0
    rdd_json = {}
    
    print("\nProcessing files:")
    
    # Read each directory and create a JSON RDD making a union of all directories
    for dir in [d for d in os.listdir(dir_path)]:
        print("idx: " + str(idx) + " | path: " + dir_path + "/" + dir)

        # Get schema
        schema = spark.read.parquet(dir_path + "/" + dir).limit(0).schema

        # Read parquet files
        df_temp = (spark.read
                       .parquet(dir_path + "/" + dir)
                  )

        # Convert to JSON to avoid error when union different schemas
        if idx == 0:
            rdd_json = df_temp.toJSON()
        else:
            rdd_json = rdd_json.union(df_temp.toJSON())

        idx = idx + 1

    return rdd_json

if __name__ == '__main__':
    sc = SparkContext('local')
    spark = SparkSession(sc)

    parquet_files_path = "../../parquet"
    df = spark.read.json(merge_schemas(parquet_files_path, spark))
    df.printSchema()

    df.write.parquet("../../merged")
