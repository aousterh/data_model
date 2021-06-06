.PHONY: pull

pull:
	cd ..; git clone https://github.com/brimsec/zq-sample-data
pd:
	python3 ./benchmark/pd.py
