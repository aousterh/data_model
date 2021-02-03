from json2parquet import convert_json
import os
import shutil
import gzip

ndjson_dir_name = "/home/admin/zq-sample-data/zeek-ndjson"

for root, dirs, files in os.walk(ndjson_dir_name, topdown=False):
    for name in files:
        src_filename = os.path.join(root, '../tmp/', name.split('.')[0] + '.ndjson')
        dest_filename = os.path.join(root, '../parquet/', name.split('.')[0] + '.parquet')
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

