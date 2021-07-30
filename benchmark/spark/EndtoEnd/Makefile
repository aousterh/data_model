.PHONY: build run clean

build:
	sbt package

run:
	taskset -c 1 ~/spark/bin/spark-submit --num-executors 1 --executor-cores 1 --class "EndtoEnd" target/scala-2.12/end-to-end-query-time-benchmark_2.12-1.0.jar

clean:
	sbt clean
