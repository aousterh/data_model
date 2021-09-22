#!/usr/bin/env Rscript
# This script can be run using:
# ./plot_type_contexts.R <times> <sizes>

# use the ggplot2 library
library(ggplot2)
library(plyr)
library(ggpattern)

# read in data
args <- commandArgs(trailingOnly=TRUE)

times <- read.csv(args[1], sep=",")
sizes <- read.csv(args[2], sep=",")


head(times)
head(sizes)


theme_set(theme_bw(base_size=12))


times$label <- ifelse(times$organization == "fused" & times$format == "parquet", "Parquet w/uber schema",
	    ifelse(times$organization == "siloed" & times$format == "parquet", "Parquet siloed",
	    ifelse(times$organization == "default" & times$format == "zng", "ZNG flexible",
	    ifelse(times$organization == "siloed" & times$format == "zng", "ZNG siloed", "unknown"))))

sizes$label <- ifelse(sizes$organization == "fused" & sizes$format == "parquet", "Parquet w/uber schema",
	    ifelse(sizes$organization == "siloed" & sizes$format == "parquet", "Parquet siloed",
	    ifelse(sizes$organization == "default" & sizes$format == "zng", "ZNG flexible",
	    ifelse(sizes$organization == "siloed" & sizes$format == "zng", "ZNG siloed", "unknown"))))


#color_vals=c("#7b3294", "#c2a5cf", "#008837", "#a6dba0")
color_vals=c("#762a83", "#af8dc3", "#1b7837", "#7fbf7b")
linetype_vals=c("longdash", "twodash", "solid", "dashed")

times_sum <- times[times$query == "sum" & (times$format == "parquet" | times$organization != "fused"),]
ggplot(times_sum, aes(x=n_types, y=real, color=label, linetype=label)) +
	     geom_line(size=1) +
	     coord_cartesian(ylim=c(0,10)) +
	     labs(x="Number of Types", y="Query Time (s)") +
	     scale_color_manual(name="", values=color_vals) +
	     scale_linetype_manual(name="", values=linetype_vals) +
	     theme(legend.position=c(0.6,0.8), legend.background=element_blank(), legend.key=element_blank())

ggsave("type_contexts.pdf", width=3.2, height=3.2)


ggplot(sizes, aes(x=as.factor(n_types), y=size, fill=format)) +
	      geom_bar(stat="identity", position="dodge") +
	      facet_grid(. ~ organization)

ggsave("type_context_sizes_all.pdf", width=8, height=5)


sizes_subset <- sizes[sizes$format == "parquet" | (sizes$format == "zng" & sizes$organization != "fused"),]
sizes_subset_2 <- sizes_subset[sizes_subset$n_types == 1 | sizes_subset$n_types == 156 | sizes_subset$n_types == 1250 | sizes_subset$n_types == 10000,]
ggplot(sizes_subset_2, aes(x=as.factor(n_types), y=size, pattern_fill=label, pattern_angle=label)) +
	      geom_col_pattern(position="dodge", pattern="stripe", pattern_density=0.9, fill='white', color='black', pattern_spacing=0.1) +
		     scale_pattern_fill_manual(name="", values=color_vals) +
		     scale_pattern_angle_manual(name="", values=c(0, 40, 20, 60)) +
		     theme(legend.position=c(0.4, 0.8), legend.background=element_blank()) +
		     labs(y="Data Size (MiB)", x="Number of Types")

ggsave("type_context_sizes.pdf", width=3.2, height=3.2)