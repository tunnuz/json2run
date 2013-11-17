source("analysis.R")

# retrieve data
comp <- getExperiments(TRUE, "comp", c("main_instance"))
ours <- getExperiments(TRUE, "na_ns_validate")
comp_ours <- getExperiments(TRUE, "comp_ours", c("main_instance"))
levels(ours$configuration) <- "na/ns"

# merge data
all <- merge(ours, merge(comp, comp_ours, all = TRUE), all = TRUE)

# normalize data
all$cost <- as.numeric(all$cost)
all <- ddply(all, .(main_instance, configuration), summarize, cost = median(cost))
all$configuration <- as.factor(all$configuration)

# prune incomplete instances
experiments <- ddply(all, .(main_instance), .fun = function(x) { 
    x$count <- nrow(x)
    return(x)
})

incomplete_instances <- unique(experiments[experiments$count != max(experiments$count),]$main_instance)
all <- all[all$main_instance %ni% incomplete_instances, ]

# graph of costs
graph <- ggplot(all, aes(x = main_instance, y =cost, color = configuration)) + geom_point() + coord_flip() + opts(legend.position = "bottom") + scale_x_discrete("Instance") + scale_color_hue("Method") + scale_y_continuous("Cost")

# compute ranks
ranks <- ddply(all, .(main_instance), transform, costrank = rank(cost))
ranks <- ddply(ranks, .(configuration), summarize, costrank = sum(costrank)) 
ranks <- ranks[order(ranks$costrank),]
print(ranks)
print(graph)
