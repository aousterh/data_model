TOPIC="zeek-logs"

.PHONY: write read
write:
	jq -rc . ./capture_loss.ndjson | kafka-console-producer.sh --broker-list localhost:9092 --topic $(TOPIC)
read:
	kafka-console-consumer.sh --topic $(TOPIC) --from-beginning --bootstrap-server localhost:9092

.PHONY: topic describe
topic:
	kafka-topics.sh --create --topic $(TOPIC) --bootstrap-server localhost:9092
describe:
	kafka-topics.sh --describe --topic $(TOPIC) --bootstrap-server localhost:9092
