Benchmark
==

The values are averages across 10 runs and in seconds.

Notes:
* For pandas, we do not acount for the time to load/dump data from/to disk. So the input format and output format represent the "in-memory format" only.
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
| `zq` | `analytics` | comp. zng | zng | 2.31 | 3.76 | 0.10 |
| `zq` | `search` | comp. zng | zng | 2.02 | 3.36 | 0.10 |
| `zq` | `search + sort` | comp. zng | zng | 2.05 | 3.37 | 0.12 |
| `zq` | `search + sort + top 5` | comp. zng | zng | 2.05 | 3.38 | 0.11 |
| `zq` | `discovery` | comp. zng | zng | 2.25 | 3.68 | 0.10 |
| `jq` | `analytics` | comp. ndjson | ndjson | 35.84 | 36.97 | 3.01 |
| `jq` | `search` | comp. ndjson | ndjson | 26.34 | 27.63 | 2.90 |
| `jq` | `search + sort` | comp. ndjson | ndjson | 26.47 | 27.72 | 2.90 |
| `jq` | `search + sort + top 5` | comp. ndjson | ndjson | 26.25 | 27.50 | 2.92 |
| `jq` | `discovery` | comp. ndjson | ndjson | 32.94 | 34.07 | 3.02 |
| `elastic search` | `search: find` | ndjson | elastic index | 12 | n/a | n/a |
| `elastic search` | `search: find and sort` | ndjson | elastic index | 34 | n/a | n/a |
| `elastic search` | `search: find, sort, return top n` | ndjson | elastic index | 7 | n/a | n/a |

