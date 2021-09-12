#!/usr/bin/env Rscript
# This script can be run using:
# ./plot_end_to_end.R <data 0> ... <data n-1>

# use the ggplot2 library
library(ggplot2)
library(plyr)

# read in data
args <- commandArgs(trailingOnly=TRUE)

tables <- lapply(args, read.csv, sep=",")
data <- do.call(rbind.fill, tables)


data$in_format <- factor(data$in_format, levels=c("index", "lake", "bson", "table", "zng", "zst", "parquet"))

head(data)
tail(data)

theme_set(theme_bw(base_size=12))


data$system_label = ifelse(data$system == "zed", "Zed",
	      ifelse(data$system == "postgres", "PostgreSQL",
	      ifelse(data$system == "mongo", "Mongo\nDB",
	      ifelse(data$system == "spark", "Spark",
	      ifelse(data$system == "Elastic", "Elastic\nSearch", "unknown")))))

# drop first 5 data points and summarize with mean
data_skip_warmup = data[data$index >= 5,]


ggplot(data, aes(x=index, y=real, color=interaction(system, in_format))) +
	     geom_point() +
	     geom_line() +
	     labs(x="Query Number", y="Query Time (s)") +
	     facet_grid(. ~ query) +
	     scale_color_discrete(name="System.Format")

ggsave("query_time_by_index.pdf", width=8, height=5)


ggplot(data, aes(x=index, y=real, color=interaction(system, in_format))) +
	     geom_point() +
	     geom_line() +
	     labs(x="Query Number", y="Query Time (s)") +
	     scale_color_discrete(name="System.Format") +
	     facet_grid(. ~ query) +
	     coord_cartesian(ylim=c(0, 2.2))

ggsave("query_time_by_index_zoom.pdf", width=8, height=5)


ggplot(data, aes(x=index, y=validation, color=interaction(system, in_format))) +
	     geom_point(alpha=0.5) +
	     labs(x="Query Number", y="Validation") +
	     scale_color_discrete(name="System.Format") +
	     facet_grid(. ~ query) +
	     coord_cartesian(ylim=c(0, 100000))

ggsave("validation.pdf", width=8, height=5)


# drop first 5 data points
data_skip_warmup = data[data$index >= 5,]

ggplot(data_skip_warmup, aes(x=interaction(system, in_format), y=real, color=system)) +
	     geom_boxplot() +
	     labs(x="System.Format", y="Query Time (s)") +
	     scale_color_discrete(name="System") +
	     coord_cartesian(ylim=c(0,12)) +
	     facet_grid(. ~ query) +
	     theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))

ggsave("query_time_boxplot.pdf", width=9, height=6)


ggplot(data_skip_warmup, aes(x=interaction(system, in_format), y=real, color=system)) +
	     geom_boxplot() +
	     labs(x="System.Format", y="Query Time (s)") +
	     scale_color_discrete(name="System") +
	     coord_cartesian(ylim=c(0,1.5)) +
	     facet_grid(. ~ query) +
	     theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))

ggsave("query_time_boxplot_zoom.pdf", width=9, height=6)


ggplot(data_skip_warmup, aes(real, color=interaction(system, in_format))) +
	stat_ecdf() +
	labs(x="Query Time (s)", y="CDF") +
	facet_grid(. ~ query) +
	scale_color_discrete(name="System.Format")

ggsave("query_time_cdf.pdf", width=8, height=5)


ggplot(data_skip_warmup, aes(x=validation, y=real, color=interaction(system, in_format))) +
			 geom_point() +
			 stat_smooth(method='lm', aes(color=interaction(system, in_format)), se=FALSE, size=0.5) +
			 labs(x="Validation (sum or number of matching records)", y="Query Time (s)") +
			 facet_grid(. ~ query) +
			 scale_color_discrete(name="System.Format")

ggsave("query_time_by_results.pdf", width=12, height=5)



data_sum_with_index <- data_skip_warmup[(data_skip_warmup$in_format == "index" | data_skip_warmup$in_format == "lake") &
data_skip_warmup$query == "analytics sum orig_bytes",]
ggplot(data_sum_with_index, aes(real, color=interaction(system, in_format))) +
			    stat_ecdf() +
			    labs(x="Query Time (s)", y="CDF") +
			    scale_color_discrete(name="System.Format")

ggsave("analytics_index_cdf.pdf")


search_skip_warmup = data_skip_warmup[data_skip_warmup$query == "search id.orig_h",]
ggplot(search_skip_warmup, aes(x=validation, y=real, color=interaction(system, in_format))) +
			 geom_point(alpha=0.5) +
			 stat_smooth(method='lm', aes(color=interaction(system, in_format)), se=FALSE, size=0.5) +
			 labs(x="Number of Returned Records", y="Query Time (s)") +
			 facet_grid(. ~ query) +
			 scale_color_discrete(name="System.Format")

ggsave("search_by_n_results.pdf", width=6, height=5)



summary <- ddply(data_skip_warmup, c("system", "in_format", "query"),
	summarise, avg_real = mean(real))


tapply(data$validation, data$query, summary)
tapply(data$real, interaction(data$system, data$in_format, data$query), summary)





# only show the best version of each system
search_subset <- search_skip_warmup[search_skip_warmup$in_format != "bson" &
	      search_skip_warmup$in_format != "zst" &
	      search_skip_warmup$in_format != "zng" &
	      search_skip_warmup$in_format != "table",]

sys_colors=c("#66c2a5", "#fc8d62", "#8da0cb", "#e78ac3", "#a6d854")
ggplot(search_subset, aes(x=system_label, y=real, color=system)) +
	     geom_boxplot() +
	     labs(x="System", y="Query Time (s)") +
	     scale_color_manual(values=sys_colors) +
	     coord_cartesian(ylim=c(0,11)) +
	     theme(legend.position="none")

ggsave("boxplot_search.pdf", width=4, height=4)



search_sort_skip_warmup = data_skip_warmup[data_skip_warmup$query == "search sort head id.orig_h",]
# only show the best version of each system
search_sort_subset <- search_sort_skip_warmup[search_sort_skip_warmup$in_format != "bson" &
	      search_sort_skip_warmup$in_format != "zst" &
	      search_sort_skip_warmup$in_format != "zng" &
	      search_sort_skip_warmup$in_format != "table",]
ggplot(search_sort_subset, aes(x=system_label, y=real, color=system)) +
	     geom_boxplot() +
	     labs(x="System", y="Query Time (s)") +
	     scale_color_manual(values=sys_colors) +
	     coord_cartesian(ylim=c(0,11)) +
	     theme(legend.position="none")

ggsave("boxplot_search_sort.pdf", width=4, height=4)



analytics_skip_warmup = data_skip_warmup[data_skip_warmup$query == "analytics avg field",]
analytics_subset <- analytics_skip_warmup[analytics_skip_warmup$in_format != "lake" &
		 analytics_skip_warmup$in_format != "zng" &
		 (analytics_skip_warmup$in_format != "index" | analytics_skip_warmup$system == "Elastic"),]

ggplot(analytics_subset, aes(x=system_label, y=real, color=system)) +
	     geom_boxplot() +
	     labs(x="System", y="Query Time (s)") +
	     scale_color_manual(values=sys_colors) +
	     coord_cartesian(ylim=c(0,11)) +
	     theme(legend.position="none")

ggsave("boxplot_analytics.pdf", width=4, height=4)