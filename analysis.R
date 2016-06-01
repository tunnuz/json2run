library(RMongo)
library(RJSONIO)
library(plyr)
library(ggplot2)
library(scales)

# mongodb connection
if (!exists("db_connection"))
{
    writeLines("Warning: you're not connected to any database, run connect(<hostname>) to connect.")
}

# (re)connect to database
connect <- function(host, database="j2r", user="j2r", pass="j2r")
{
    assign("db_connection", mongoDbConnect(database, host), envir = .GlobalEnv)
    # dbAuthenticate(mongo,"user","pass")
}

# "not-in" operator
`%ni%` = Negate(`%in%`)
Infinity <- Inf

# steal interleave from ggplot
interleave <- ggplot2:::interleave

# get all experiments
getExperiments <- function(batch, fromScratch = TRUE, instance_param = c("main_instance"))
{
    cacheFile = sprintf("%s.dat", batch)
    batch <- dbGetQueryForKeys(db_connection, "batches", sprintf('{ "name": "%s" }', batch), '{ "type": 1, "configurations": 1, "instance_parameter": 1 }')
    batch_id = batch$X_id

    if (is.na(batch$instance_parameter))
        batch$instance_parameter <- instance_param

    # instance parameters are known for races
    if (batch$type == "race")
        instance_param = c(batch$instance_parameter, "repetition")

    instance_param <- c(normalize_name(instance_param), "repetition")

    if (is.null(batch_id))
    {
        writeLines("Batch not found!")
        return(NULL)
    }
    else
        writeLines(sprintf("Found batch: %s", batch_id))

    confs <- extract_confs(batch)

    experiments <- data.frame()
    writeLines(sprintf("Getting experiments for batch %s", batch$name))

    # if cache file exists
    if (file.exists(cacheFile) & !fromScratch)
    {
        writeLines(sprintf("Loading cache file %s", cacheFile))
        load(cacheFile)
        return (experiments)

    }

    # if cache file doesn't exists or you wish to retrieve experiments from scratch
    else
    {
        writeLines("Dumping experiments from the database")

        # this is to avoid memory exceptions in RMongo, get chunks of 100000 results
        alreadyRetrieved <- 0
        writeLines("Retrieving chunks")
        repeat
        {

            # get chunk and attach it to experiments
            chunk <- dbGetQueryForKeys(
                db_connection,  # connection
                "experiments",  # collection
                sprintf('{ "batch": { "$oid": "%s" } }', batch_id),
                # selected fields
                "{
                    parameters: 1,
                    stats: 1
                }",
                limit = 100,
                skip = alreadyRetrieved
            )

            chunkSize <- nrow(chunk)
            alreadyRetrieved <- alreadyRetrieved + chunkSize
            writeLines(sprintf("Got %d records so far", alreadyRetrieved))
            experiments <- rbind.fill(experiments, chunk)

            # stop if last chunk has been retrieved
            if (chunkSize < 100)
                break
        }

        # remove _ids
        experiments$X_id <- NULL

        old_names <- names(experiments)
        experiments <- expand_JSON(experiments, "parameters")
        new_names <- names(experiments)

        parameters <- setdiff(new_names, old_names)

        instance_param <- intersect(instance_param, parameters)
        for (p in instance_param)
             experiments[,p] <- as.factor(sub(".*/", "", experiments[,p]))

        experiments$repetition <- NULL
        experiments <- expand_JSON(experiments, "stats")

        str(experiments)

        # generating configurations
        writeLines("Generating configurations ...")

        conf_params <- parameters[which(parameters %ni% instance_param)]
        experiments <- add_conf(experiments, conf_params)

        # pruned / winning
        if (batch$type == "race")
        {
            # confs, plus information about sum_of_ranks, last_iteration
            relevant_parameters <- get_relevant_params(experiments, conf_params)
            confs <- add_conf(confs, conf_params, relevant_parameters)

            winning <- confs[!is.na(confs$sum_of_ranks),]
            experiments <- merge(experiments, confs)
            experiments$pruned <- is.na(experiments$sum_of_ranks)
        }

        str(experiments)

        save(experiments, file=cacheFile)

        return (experiments)
    }
}

# get a set of parameters, and tries to detect whether they are needed in the configurations
get_relevant_params <- function(x, params)
{
    relevant_params <- c()
    for (param in params)
    {
        if (length(unique(x[,param])) > 1)
            relevant_params <- c(relevant_params, param)
    }
    return(relevant_params)
}

# expand content of columns (e.g. stats or parameters)
expand_JSON <- function(x, column)
{
    writeLines(sprintf("Expanding JSON %s ...", column))

    full <- lapply(x[,column], function(x)
    {
        y <- fromJSON(x, simplify=FALSE)
        return(y[unlist(lapply(y,function(x) { return(!is.list(x))}))])
    })

    # simplify=FALSE prevents fromJSON to arbitrarily return colums or rows depending on the length of the JSON string
    data <- do.call(rbind.fill, lapply(full, as.data.frame))
    names(data) <- normalize_name(names(data))
    x <- cbind(x,data)
    return(x[,which(names(x) != column)])
}

# generate configurations
add_conf <- function(x, params, relevant_params = c())
{
    if (length(relevant_params) == 0)
        relevant_params <- get_relevant_params(x, params)

    if (length(relevant_params) == 0)
    {
        x$configuration <- "unique"
        return(x)
    }

    # short_relevant_params <- unlist(lapply(relevant_params, shorthand_name))
    x$configuration <- as.factor(do.call(paste, interleave(relevant_params, x[relevant_params])))

    return(x)
}

# extract winning configurations (races only)
extract_confs <- function(batch)
{
    winning <- data.frame()

    # gather winning
    if (batch$type == "race")
    {
        racing = fromJSON(batch$configurations)
        for (i in 1:length(racing))
        {
            if (is.null(racing[[i]]$sum_of_ranks))
                racing[[i]]$sum_of_ranks <- NA
            row <- as.data.frame(racing[i])
            winning <- rbind.fill(winning, row)
        }
        names(winning) <- c(normalize_name(names(winning)))
    }
    return(winning)
}

# get rid of colons in names
normalize_name <- function(s)
{
    s <- tolower(gsub("::", "_", s, fixed = TRUE))
    return(gsub("..", "_", s, fixed = TRUE))
}

# shrink parameter names
shorthand_name <- function(s, l = 1)
{
    pcs <- strsplit(s, "_")[1]
    pcss <- c()
    for (i in pcs)
        pcss <- c(pcss, substr(i,0,l))
    pcss <- c(pcss, sep = "")
    return(do.call(paste, as.list(pcss)))
}

# configuration generation (probably not used)
linteraction <- function (factors, sep = ", ", lex.order = FALSE)
{
    args = list()
    if (is.data.frame(factor))
        for (i in 1:ncol(factors)) {
            args[i] <- factors[i]
        }
    else
        args = list(factors)

    narg <- length(args)
    if (narg == 1L && is.list(args[[1L]])) {
        args <- args[[1L]]
        narg <- length(args)
    }
    for (i in narg:1L) {
        f <- as.factor(args[[i]])[, drop = FALSE]
        l <- levels(f)
        if1 <- as.integer(f) - 1L
        if (i == narg) {
            ans <- if1
            lvs <- l
        }
        else {
            if (lex.order) {
                ll <- length(lvs)
                ans <- ans + ll * if1
                lvs <- paste(rep(l, each = ll), rep(lvs, length(l)),
                  sep = sep)
            }
            else {
                ans <- ans * length(l) + if1
                lvs <- paste(rep(l, length(lvs)), rep(lvs, each = length(l)),
                  sep = sep)
            }
            if (anyDuplicated(lvs)) {
                ulvs <- unique(lvs)
                while ((i <- anyDuplicated(flv <- match(lvs,
                  ulvs)))) {
                  lvs <- lvs[-i]
                  ans[ans + 1L == i] <- match(flv[i], flv[1:(i -
                    1)]) - 1L
                  ans[ans + 1L > i] <- ans[ans + 1L > i] - 1L
                }
                lvs <- ulvs
            }
        }
    }
    structure(as.integer(ans + 1L), levels = lvs, class = "factor")
}

# post-hoc of the Friedman rank sum test, as implemented in F-Race
friedman_posthoc <- function(x, alpha = 0.05, measure = "cost", optimization = "min"){

    # measure number of samples
    samples = min(ddply(x, .(configuration), .fun = function(x) { return(nrow(x)) })$V1)

    # configurations
    confs <- unique(x$configuration)
    opt_factor = 1
    if (optimization == "max")
        opt_factor = -1

    # rotate experiments to match F-Race's setup
    y <- ddply(x, .(configuration), .fun = function(x, samples) { return(rep(t(x[,"cost"] * opt_factor), samples)[1:samples])}, samples)

    y <- t(y[,2:ncol(y)])
    n <- nrow(y)
    I <- 1:ncol(y)

    # configurations on the columns
    k <- length(I)
    r <- t(apply(y[1:n, I], 1, rank))

    # code from F-Race starts here
    A <- sum(as.vector(r)^2)
    R <- apply(r, 2, sum)
    J <- I[order(R)]

    TIES <- tapply(r, row(r), table)
    STATISTIC <- ((12 * sum((R - n * (k + 1)/2)^2))/(n * k *
        (k + 1) - (sum(unlist(lapply(TIES, function(u) {
        u^3 - u
    })))/(k - 1))))
    PARAMETER <- k - 1
    PVAL <- pchisq(STATISTIC, PARAMETER, lower = FALSE)

    o <- order(R)

    if (!is.nan(PVAL) && (PVAL < alpha)) {

        t <- qt(1 - alpha/2, (n - 1) * (k - 1)) * (2 * (n * A - sum(R^2))/((n - 1) * (k - 1)))^(1/2)
        J <- I[o[1]]
        for (j in 2:k) if (abs(R[o[j]] - R[o[1]]) > t)
            break
        else J <- c(J, I[o[j]])
    }

    # return winning configurations and sum of ranks
    return(data.frame(configuration = confs[J], sum_of_ranks = R[J]))
}

# execute Friedman's post-hoc by subsets
friedman_posthoc_by_class <- function(x, class, one_winner = FALSE)
{
    x$pruned <- TRUE
    x$class <- as.factor(x[,class])
    x <- ddply(x, .(class), .fun = function(x, one_winner)
    {
        print(sprintf("Processing %s", x[1,"class"]))

        # do first race
        z <- friedman_posthoc(x)
        if (one_winner)
            z <- z[z$sum_of_ranks == max(z$sum_of_ranks),]
        print(z)
        x[x$configuration %in% z$configuration,]$pruned <- FALSE

        # check if we can race more on winners
        last = nrow(z)
        while (TRUE && last > 1)
        {
            z <- friedman_posthoc(x[x$pruned == FALSE,])
            if (one_winner)
                z <- z[z$sum_of_ranks == max(z$sum_of_ranks),]
            print(z)
            x$pruned <- TRUE
            x[x$configuration %in% z$configuration,]$pruned <- FALSE

            if (nrow(z) == last)
                break
            last = nrow(z)
        }
        return(x)
    }, one_winner)
    x$class <- NULL
    return(x)
}

regression_tree <- function(x, model) {

    # build regression tree to find good parameters wrt. performance
    fit <- rpart(model, method="anova")

    # summary(fit)
    printcp(fit)
    if (sum(fit$splits)> 0)
    {
    	# print textual version
    	print(fit)

        # plot tree
    	plot(fit, main = sprintf("regression tree"))
    	text(fit)

    	# save tree as postscript
    	post(fit, filename="regression_tree.ps")
    }

    # perform anova to detect relevant parameters
    print(anova(model))
    return(fit)
}
