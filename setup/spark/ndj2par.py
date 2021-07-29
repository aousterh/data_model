from json2parquet import convert_json
import os
import shutil
import gzip

# this script converts zipped ndjson files from path ndjson_dir_name to parquet files stored in '../../parquet

dir = os.path.dirname(__file__)
ndjson_dir_name = "../../../zq-sample-data/zeek-ndjson"
unzipped_dir = '../../unzipped_ndj'
parquet_dir = '../../parquet'

for root, dirs, files in os.walk(ndjson_dir_name, topdown=False):
    for name in files:
        src_filename = os.path.join(dir, unzipped_dir,  name.split('.')[0] + '.ndjson')
        dest_filename = os.path.join(dir, parquet_dir, name.split('.')[0] + '.parquet')
        os.makedirs(os.path.join(dir, unzipped_dir), exist_ok = True)
        os.makedirs(os.path.join(dir, parquet_dir), exist_ok = True)
        zipped_ndjson_file = os.path.join(root, name)
        print("processing " + os.path.join(root, name))

        with gzip.open(zipped_ndjson_file, 'rb') as f_in:
            with open(src_filename, 'wb') as f_out:
                f_out.write(f_in.read())
        try:
            convert_json(src_filename, dest_filename)
        except Exception as e:
            print("Failed to process the file: " + name)
            print(e)

