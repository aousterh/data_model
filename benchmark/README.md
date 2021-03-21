Benchmark
==

The values are averages across 10 runs and in seconds.

Notes:
* For pandas, we do not acount for the time to load/dump data from/to disk. So the input format and output format represent the "in-memory format" only.
* zq and jq results are for a single run.
* "comp." = compressed

### zq-sample-data
|**<br>Tool**|**<br>Queries**|**Input<br>Format**|**Output<br>Format**|**<br>Real**|**<br>User**|**<br>Sys**|
|:----------:|:---------------:|:-----------------:|:------------------:|-----------:|-----------:|----------:|
| `pandas` | `analytics` | dataframe | dataframe | 0.4164 | 0.41069 | 0.00563 |
| `pandas` | `search + sort + top 5` | dataframe | rows | 1.15409 | 1.15203 | 0.00147 |
| `pandas` | `search w/concat + sort + top 5` | dataframe | dataframe | 4.55423 | 3.92687 | 0.62559 |
| `pandas` | `discovery` | dataframe | dataframe | 0.0015 | 0.00149 | 0.00001 |
| `spark` | `analytics` | parquet | dataframe | 6.24149 | 0.00267| 0.00133 |
| `spark` | `search + sort + top 5` | parquet | dataframe | 34.93111 | 0.004 | 0.00133 |
| `spark` | `discovery` | parquet | dataframe | 6.45012 | 0.052 | 0.004 |
| `zq` | `analytics` | comp. zng | zng | 2.27 | 3.74 | 0.09 |
| `zq` | `search` | comp. zng | zng | 2.03 | 3.39 | 0.10 |
| `zq` | `search + sort` | comp. zng | zng | 2.05 | 3.44 | 0.06 |
| `zq` | `search + sort + top 5` | comp. zng | zng | 2.04 | 3.37 | 0.13 |
| `zq` | `discovery` | comp. zng | zng | 2.23 | 3.67 | 0.10 |
| `jq` | `analytics` | comp. ndjson | ndjson | 35.28 | 36.50 | 2.72 |
| `jq` | `search` | comp. ndjson | ndjson | 26.27 | 27.72 | 2.67 |
| `jq` | `search + sort` | comp. ndjson | ndjson | 26.37 | 27.78 | 2.69 |
| `jq` | `search + sort + top 5` | comp. ndjson | ndjson | 26.09 | 27.48 | 2.61 |
| `jq` | `discovery` | comp. ndjson | ndjson | 32.59 | 33.76 | 2.80 |
