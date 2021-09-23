.PHONY: init plot
DATA_DIR := results_sigmod_9_15

init:
	pip install -r requirements.txt

plot:
	./plot_end_to_end.R $(DATA_DIR)/mongo*.csv $(DATA_DIR)/spark*.csv $(DATA_DIR)/*zed*.csv $(DATA_DIR)/postgres*.csv

plot_type_contexts:
	./plot_type_contexts.R $(DATA_DIR)/type_context_results.csv $(DATA_DIR)/type_context_sizes.csv
