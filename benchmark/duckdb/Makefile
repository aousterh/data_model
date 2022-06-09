.PHONY: install
CONFIG := default.yaml

install:
	wget https://github.com/duckdb/duckdb/releases/download/v0.3.4/duckdb_cli-linux-amd64.zip
	unzip duckdb_cli-linux-amd64.zip

run:
	CONFIG=$(CONFIG) ./benchmark.py
