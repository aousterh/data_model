import os

zng_dir = "/zq-sample-data/z/zng"
ndjson_dir = 'ndjson'

def zng_to_ndjson():
    os.system("rm -fr " + ndjson_dir)
    os.system("mkdir " + ndjson_dir)

    for root, dirs, files in os.walk(zng_dir, topdown=False):
        for name in files:
            zng_filename = os.path.join(root, name)
            ndjson_filename = os.path.join(ndjson_dir, name.split('.')[0] + '.ndjson')
            print("processing " + zng_filename)

            # read in all records as zng and write out as ndjson
            cmd = "zq -f ndjson -o " + ndjson_filename + " '*' " + zng_filename
            os.system(cmd)

if __name__ == "__main__":
    zng_to_ndjson()
