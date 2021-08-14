.PHONY: init plot

init:
	pip install -r requirements.txt

plot:
	./plot_end_to_end.R \
	elastic_network_log_analytics_30_execution_times.csv elastic_network_log_search_30_execution_times.csv \
	mongo-analytics-range_sum_orig_bytes.csv mongo-search-random_orig_h.csv mongo-analytics-range_sum_orig_bytes-index.csv mongo-search-random_orig_h-index.csv \
	postgres-search-random_orig_h.csv postgres-analytics-range_sum_orig_bytes.csv postgres-search-random_orig_h-index.csv postgres-analytics-range_sum_orig_bytes-index.csv \
	end_to_end_zed_8_9_1_core.csv \
	spark_8_12_analytics.csv spark_8_12_search.csv
