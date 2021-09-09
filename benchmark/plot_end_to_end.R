#!/usr/bin/env Rscript
# This script can be run using:
# ./plot_end_to_end.R <data 0> ... <data n-1>

# use the ggplot2 library
library(ggplot2)
library(plyr)

# read in data
args <- commandArgs(trailingOnly=TRUE)

tables <- lapply(args, read.csv, sep=",")
data <- do.call(rbind, tables)

head(data)

theme_set(theme_bw(base_size=12))


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
	     facet_grid(. ~ query)

ggsave("validation.pdf", width=8, height=5)


# drop first 5 data points
data_skip_warmup = data[data$index >= 5,]

ggplot(data_skip_warmup, aes(x=interaction(system, in_format), y=real, color=system)) +
	     geom_boxplot() +
	     labs(x="System.Format", y="Query Time (s)") +
	     scale_color_discrete(name="System") +
	     coord_cartesian(ylim=c(0,10)) +
	     facet_grid(. ~ query) +
	     theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))

ggsave("query_time_boxplot.pdf", width=9, height=6)


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

ggsave("query_time_by_results.pdf", width=8, height=5)


search_skip_warmup = data_skip_warmup[data_skip_warmup$query == "search id.orig_h",]
ggplot(search_skip_warmup, aes(x=validation, y=real, color=interaction(system, in_format))) +
			 geom_point(alpha=0.5) +
			 stat_smooth(method='lm', aes(color=interaction(system, in_format)), se=FALSE, size=0.5) +
			 labs(x="Number of Returned Records", y="Query Time (s)") +
			 facet_grid(. ~ query) +
			 scale_color_discrete(name="System.Format")

ggsave("search_by_n_results.pdf", width=6, height=5)


tapply(data$validation, data$query, summary)
tapply(data$real, interaction(data$system, data$in_format, data$query), summary)
