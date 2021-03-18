Benchmark
==

The values are averages across 10 runs and in seconds.

Notes:
* For pandas, we do not acount for the time to load/dump data from/to disk. So the input format and output format represent the "in-memory format" only.
* zq and jq results are for a single run.

### zq-sample-data
|**<br>Tool**|**<br>Queries**|**Input<br>Format**|**Output<br>Format**|**<br>Real**|**<br>User**|**<br>Sys**|
|:----------:|:---------------:|:-----------------:|:------------------:|-----------:|-----------:|----------:|
| `pandas` | `analytics` | dataframe | dataframe | 0.4164 | 0.41069 | 0.00563 |
| `pandas` | `search` | dataframe | rows | 1.15409 | 1.15203 | 0.00147 |
| `pandas` | `search w/ concat` | dataframe | dataframe | 4.55423 | 3.92687 | 0.62559 |
| `pandas` | `discovery` | dataframe | dataframe | 0.0015 | 0.00149 | 0.00001 |
| `spark` | `analytics` | parquet | dataframe | 6.24149 | 0.00267| 0.00133 |
| `spark` | `search` | parquet | dataframe | 34.93111 | 0.004 | 0.00133 |
| `spark` | `discovery` | parquet | dataframe | 6.45012 | 0.052 | 0.004 |
| `zq` | `analytics` | zng | zng | 2.27 | 3.74 | 0.09 |
| `zq` | `search` | zng | zng | 2.04 | 3.37 | 0.13 |
| `zq` | `discovery` | zng | zng | 2.23 | 3.67 | 0.10 |
| `jq` | `analytics` | ndjson | ndjson | 35.28 | 36.50 | 2.72 |
| `jq` | `search` | ndjson | ndjson | 26.09 | 27.48 | 2.61 |
| `jq` | `discovery` | ndjson | ndjson | 32.59 | 33.76 | 2.80 |
