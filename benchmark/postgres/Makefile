.PHONY: init stop start load run
DIR := .postgres
ifndef DB
override DB = zed
endif

CONFIG := default

# To prepare the database:
# make init
# make start
# make prep
# make load
# Then run the benchmark:
# make run

### Parallel load with Ray and skip decompression:
### 	NOZIP=true RAY=true make load
###

install:
	sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
	wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
	sudo apt-get update
	sudo apt -y install postgresql-13 postgresql-client-13 libpq-dev
init:
	rm -fr $(DIR)
	initdb $(DIR)
	sudo chmod a+w /var/run/postgresql || true
start: | stop
	# sudo systemctl stop postgresql@13-main
	pg_ctl -D $(DIR) --log=log.txt start
prep:
	psql postgres -c "CREATE ROLE zed WITH LOGIN PASSWORD 'zed'" || true
	psql postgres -c "CREATE DATABASE zed" || true
load:
	./load.py
run:
	CONFIG=$(CONFIG) ./benchmark.py | tee out.txt 2> err.txt
prep-nested:
	psql postgres -c "CREATE ROLE nested WITH LOGIN PASSWORD 'nested'" || true
	psql postgres -c "CREATE DATABASE nested " || true
load-nested:
	FORMAT=ndjson-nested DB=nested ./load.py
run-nested:
	DB=nested CONFIG=$(CONFIG) ./benchmark.py | tee out.txt 2> err.txt
stop:
	pg_ctl -D $(DIR) stop || true
