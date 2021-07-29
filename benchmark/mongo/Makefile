.PHONY: stop start load run
DB := zed
COL := zeek
CONFIG := default

# Steps:
# i. Run mongod: make start
# ii. Load dataset: make load
# iii. Benchmark: make run
# Do not run make load twice with the same DB

install:
	wget -qO - https://www.mongodb.org/static/pgp/server-4.2.asc | sudo apt-key add -
	echo "deb http://repo.mongodb.org/apt/debian stretch/mongodb-org/4.2 main" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.2.list
	sudo apt-get update
	sudo apt-get install -y mongodb-org
	#apt --fix-broken install
stop:
	pkill mongod || true
start:
	mkdir .mongo || true
	mongod --dbpath .mongo > /dev/null 2>&1 &
load:
	DB=$(DB) COL=$(COL) ./load.py
run:
	DB=$(DB) COL=$(COL) CONFIG=$(CONFIG) ./benchmark.py
