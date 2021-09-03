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
	sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
	wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
	sudo apt-get update
	sudo apt -y install postgresql-13 postgresql-client-13 libpq-dev
init:
	rm -fr $(DB)
	initdb $(DB)
	sudo chmod a+w /var/run/postgresql
start: | stop
	# sudo systemctl stop postgresql@13-main
	pg_ctl -D $(DB) --log=log.txt start
prep:
	psql postgres -c "CREATE ROLE zed WITH LOGIN PASSWORD 'zed'" || true
	psql postgres -c "CREATE DATABASE zed" || true
load:
	./load.py
run:
	CONFIG=$(CONFIG) ./benchmark.py | tee out.txt 2> err.txt
stop:
	pg_ctl -D $(DB) stop || true
