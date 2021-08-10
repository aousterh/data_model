#!/usr/bin/env Rscript
# This script can be run using:
# ./plot_end_to_end.R <data0> <data1> <data2> <data3> <data4>

# use the ggplot2 library
library(ggplot2)
library(plyr)

# read in data
args <- commandArgs(trailingOnly=TRUE)

data0 <- read.csv(args[1], sep=",")
data1 <- read.csv(args[2], sep=",")
data2 <- read.csv(args[3], sep=",")
data3 <- read.csv(args[4], sep=",")
data4 <- read.csv(args[5], sep=",")

data <- rbind.fill(data0, data1, data2, data3, data4)

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
	     coord_cartesian(ylim=c(0, 2.2))

ggsave("query_time_by_index_zoom.pdf", width=8, height=5)


data_skip_warmup = data[data$index >= 10,]
ggplot(data_skip_warmup, aes(real, color=interaction(system, in_format))) +
	stat_ecdf() +
	labs(x="Query Time (s)", y="CDF") +
	facet_grid(. ~ query) +
	scale_color_discrete(name="System.Format")

ggsave("query_time_cdf.pdf", width=8, height=5)


ggplot(data_skip_warmup, aes(x=validation, y=real, color=interaction(system, in_format))) +
			 geom_point() +
			 stat_smooth(method='lm', aes(color=interaction(system, in_format)), se=FALSE, size=0.5) +
			 labs(x="Number of Results (sum or number of matching records)", y="Query Time (s)") +
			 facet_grid(. ~ query) +
			 scale_color_discrete(name="System.Format")

ggsave("query_time_by_results.pdf", width=8, height=5)


tapply(data$validation, data$query, summary)
tapply(data$real, interaction(data$system, data$in_format, data$query), summary)
