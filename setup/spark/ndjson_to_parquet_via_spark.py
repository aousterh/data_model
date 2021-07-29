import os
from pyspark.sql.session import SparkSession

# this script converts ndjson files from path ndjson_dir_name to parquet files stored in parquet_dir

ndjson_dir_name = "/zq-sample-data/ndjson-nested"
parquet_dir = 'parquet'

def ndjson_to_parquet():
    os.system("rm -fr " + parquet_dir)
    os.system("mkdir " + parquet_dir)

    for root, dirs, files in os.walk(ndjson_dir_name, topdown=False):
        for name in files:
            ndjson_filename = os.path.join(root, name)
            print("processing " + ndjson_filename)

            # read ndjson into a dataframe, write out as parquet into parquet_dir
            df = spark.read.json(ndjson_filename)
            df.write.format("parquet").mode("append").save(parquet_dir)

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[1]").getOrCreate()

    ndjson_to_parquet()
