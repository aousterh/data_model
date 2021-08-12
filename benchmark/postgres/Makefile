.PHONY: init stop start load run
DB := .postgres
CONFIG := default

# To prepare the database:
# make init
# make start
# make prep
# make load
# Then run the benchmark:
# make run

install:
	sudo apt -y install postgresql-13 postgresql-client-13
init:
	initdb $(DB)
start:
	pg_ctl -D $(DB) start
prep:
	psql postgres -c "CREATE ROLE zed WITH LOGIN PASSWORD 'zed'" || true
	psql postgres -c "CREATE DATABASE zed" || true
load:
	./load.py
run:
	CONFIG=$(CONFIG) ./benchmark.py
stop:
	pg_ctl -D $(DB) stop