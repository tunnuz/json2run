#!/usr/bin/env python2.7

import sys
import re
import json
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from json2run import *
from pymongo import *
from multiprocessing import *
from collections import namedtuple
import logging as log
from multiprocessing import cpu_count
import datetime
from math import floor, ceil

def main():
    # Add options parser
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    prepare_args(parser)
    args = parser.parse_args()

    # setup scm
    Persistent.scm = args.scm

    # logging setup
    log_level = log.INFO
    if args.log_level == "error":
        log_level = log.ERROR
    elif args.log_level == "info":
        log_level = log.INFO

    if args.log_file:
        log.basicConfig(filename=args.log_file, level=log_level, format='%(asctime)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    else:
        log.basicConfig(level=log_level,  format='%(asctime)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

    # database setup
    Persistent.connect(host=args.db_host, port=args.db_port, user=args.db_user, passw=args.db_pass, database=args.db_database)

    # Get srun arguments
    slurm_cmd = ("srun --quiet --job-name=j2rtask --quit-on-interrupt --time=%s --cpus-per-task=%i"
                    % (args.slurm_time, args.slurm_cpus))
    if args.slurm_partition != "":
        # If we have set a partition, add it to the cmd
        slurm_cmd += " --partition=%s" % (args.slurm_partition)
    if args.slurm_mem > 0:
        # If we have set memory, work out how much per cpu and add it to cmd
        mem_per_cpu = int(ceil(args.slurm_mem / args.slurm_cpus))
        slurm_cmd += " --mem-per-cpu=%i" % (mem_per_cpu)

    # Dict with slurm settings
    slurm = {"use": args.slurm,
             "cmd": slurm_cmd}

    # action dispatching
    if args.action == "print-cll" or args.action == "print-csv":

        if not args.input:
            log.error("You need to provide a source JSON file.")
            sys.exit(1)

        pex = from_file(args.input)

        if args.action == "print-cll":
            while pex.has_more():
                print(ParameterExpression.format(args.executable, pex.next(), args.separator, args.prefix))
        else:
            headers = pex.headers()
            print(",".join(headers))
            while pex.has_more():
                n = pex.next()
                l = []

                for h in headers:
                    p_value = None
                    for p in n:
                        if p.name == h:
                            p_value = p.value
                            break
                    if p_value:
                        l.append(str(p.value))
                    else:
                        l.append("")

                print(",".join(l))

    # run a batch
    elif args.action == "run-batch":

        if not args.batch_name:
            log.error("You need to provide a valid batch name.")
            sys.exit(1)

        batch = None
        samename = [b for b in Batch.get({ "name": args.batch_name })]
        unfinished = [b for b in samename if b["date_started"] == b["date_stopped"]]

        # resume
        if unfinished:

            log.info("There is an unfinished batch with this name, resuming.")
            b = unfinished.pop()
            batch = Batch(False)
            batch.load(b)
            batch.run(slurm, args.parallel_threads, args.greedy)

        # initialize
        elif not samename:

            if not args.executable:
                log.error("You need to provide a valid executable.")
                sys.exit(1)

            if not args.input:
                log.errro("You need to provide a source JSON file.")
                sys.exit(1)

            pex = from_file(args.input)
            batch = Batch(name = args.batch_name, generator = pex, executable = args.executable, repetitions = int(args.repetitions), prefix = args.prefix, separator = args.separator)
            batch.run(slurm, args.parallel_threads, args.greedy)

        else:
            log.error("A complete batch with the same name is already on the database, try another name.")
            sys.exit(1)

        batch.save()

    # run a race
    elif args.action == "run-race":

        if not args.batch_name:
            log.error("You need to provide a valid batch name.")
            sys.exit(1)

        batch = None
        samename = [b for b in Race.get({ "name": args.batch_name })]
        unfinished = [b for b in samename if b["date_started"] == b["date_stopped"]]

        # resume
        if unfinished:

            log.info("There is an unfinished batch with this name, resuming.")
            b = unfinished.pop()
            batch = Race(False)
            batch.load(b)

            if args.input:
                pex = from_file(args.input)
                batch.set_generator(pex)

            batch.run(slurm, args.parallel_threads, args.greedy, args.confidence)

        # initialize
        elif not samename:

            if not args.instance_param:
                log.error("You need to provide the name of the parameter representing the instance.")
                sys.exit(1)

            if not args.performance_param:
                log.error("You need to provide the name of the parameter representing the performance metric.")
                sys.exit(1)

            if not args.executable:
                log.error("You need to provide a valid executable.")
                sys.exit(1)

            if not args.input:
                log.error("You need to provide a source JSON file.")
                sys.exit(1)

            pex = from_file(args.input)

            batch = Race(name = args.batch_name, generator = pex, executable = args.executable, repetitions = int(args.repetitions), initial_block = int(args.initial_block), performance_parameter = args.performance_param, instance_parameter = args.instance_param, seed = args.seed, prefix = args.prefix, separator = args.separator)
            batch.run(slurm, args.parallel_threads, args.greedy, args.confidence)

        else:
            log.error("A complete batch with the same name is already on the database, try another name.")
            sys.exit(1)

        batch.save()

    # list batches
    elif args.action == "list-batches":

        fields = ["Name", "Completion", "Host", "User", "Type", "Started", "Finished"]

        batches = None
        if args.filter:
            batches = Batch.get({"name": { "$regex": args.filter  }}).sort("date_started", -1)
        else:
            batches = Batch.get({}).sort("date_started", -1)

        if args.limit != 0:
            batches.limit(args.limit)

        print("Batches matching criteria: ", batches.count())

        # get average experiment run time
        aggregate = False
        avg_duration = None
        last_experiment = None

        # get unfinished batches
        unfinished = [b["_id"] for b in batches if b["date_started"] == b["date_stopped"]]
        batches.rewind()

        try:

            # compute (on db) average experiment execution time and last experiment time
            ad = Persistent.database["experiments"].aggregate([{"$match":{"date_stopped":{"$exists":True}, "batch": { "$in": unfinished } }},{"$group":{"_id":"$batch","last_experiment":{"$max":"$date_stopped"}, "duration":{"$avg":{"$add":[{"$subtract":[{"$second":"$date_stopped"},{"$second":"$date_started"}]},{"$multiply":[60.0,{"$subtract":[{"$minute":"$date_stopped"},{"$minute":"$date_started"}]}]},{"$multiply":[3600.0,{"$subtract":[{"$hour":"$date_stopped"},{"$hour":"$date_started"}]}]},{"$multiply":[86400.0,{"$subtract":[{"$dayOfYear":"$date_stopped"},{"$dayOfYear":"$date_started"}]}]},{"$multiply":[977616000.0,{"$subtract":[{"$year":"$date_stopped"},{"$year":"$date_started"}]}]}]}}}}])
            avg_duration = {}
            for entry in ad["result"]:
                avg_duration[entry["_id"]] = entry["duration"]
            last_experiment = {}
            for entry in ad["result"]:
                last_experiment[entry["_id"]] = entry["last_experiment"]

            fields.extend(["ETA", "Active"])
            aggregate = True

        except Exception as e:
            log.info("The database doesn't support some of the features.")

        Row = namedtuple("Row", fields)
        table = []

        for b in batches:

            batch = None
            if (b["type"] == "race"):
                batch = Race(False)
            else:
                batch = Batch(False)

            batch.load(b)

            date_started = str(batch["date_started"].strftime("%d/%m/%y %H:%M"))
            date_stopped = "never" if batch["date_started"] == batch["date_stopped"] else str(batch["date_stopped"].strftime("%d/%m/%y %H:%M"))

            completion = batch.completion()

            if (b["type"] == "full"):
                completion = "%.2f " % batch.completion() + '%'

            if aggregate:
                ad = 0 if batch["_id"] not in avg_duration else avg_duration[batch["_id"]]
                le = datetime.datetime(year = 1970, month = 1, day = 1) if batch["_id"] not in last_experiment else last_experiment[batch["_id"]]

                # compute eta wrt. available cores
                threads = args.parallel_threads
                try:
                    threads = batch["threads"]
                except:
                    pass
                eta = (float(batch.missing()) * ad) / float(threads)

                # a batch is active if last experiment terminated not before twice the average duration ago
                now = datetime.datetime.utcnow()
                status = "*" if (now - datetime.timedelta(seconds = int(ad * 2.0)) < le) and batch["date_started"] == batch["date_stopped"] else " "

                # report eta if > than 1 minute
                sec_in_day = 86400.0
                sec_in_hour = 3600.0
                sec_in_minute = 60.0

                # compose eta string
                eta_str = ""
                if floor(eta / sec_in_day) > 0:
                    eta_str += "%dd " % floor(eta / sec_in_day)
                if floor((eta % sec_in_day) / sec_in_hour):
                    eta_str += "%dh " % floor((eta % sec_in_day) / sec_in_hour)
                if int((eta % sec_in_hour) / sec_in_minute):
                    eta_str += "%dm " % int((eta % sec_in_hour) / sec_in_minute)

                if eta_str:
                    eta_str += "(%d cores)" % threads

		# eta_str += " (avg. %f)" % ad

                r = Row(batch["name"], completion, batch["host"], batch["user"], batch.type(), date_started, date_stopped, eta_str or "--", status )
            else:
                r = Row(batch["name"], completion, batch["host"], batch["user"], batch.type(), date_started, date_stopped)

            table.append(r)

        print_table(table)

    # delete batch
    elif args.action == "delete-batch":

        if not args.batch_name:
            log.error("You need to provide a batch name to remove.")
            sys.exit(1)

        batch = None
        try:
            batch = Batch(False)
            batch.load(Batch.get({ "name": args.batch_name }).next())
        except:
            log.error("Error loading batch.")
            sys.exit(1)

        sys.stdout.write("Are you sure? ")
        sys.stdout.flush()
        if re.compile("Y|y|yes|YES").match(sys.stdin.readline()):
            Batch.remove({ "name": batch["name"] })
        else:
            sys.exit(0)

        sys.stdout.write("Remove related experiments? ")
        sys.stdout.flush()
        if re.compile("Y|y|yes|YES").match(sys.stdin.readline()):
            Experiment.remove({ "batch": batch["_id"] })
        else:
            sys.exit(0)

    # mark unfinished batch
    elif args.action == "mark-unfinished":

        if not args.batch_name:
            log.error("You need to provide a batch name to mark as unfinished.")
            sys.exit(1)

        batch = None
        try:
            batch = Batch(False)
            batch.load(Batch.get({ "name": args.batch_name }).next())
        except:
            log.error("Error loading batch.")
            sys.exit(1)

        batch["date_stopped"] = batch["date_started"]
        batch.save()

    # rename batch
    elif args.action == "rename-batch":

        if not args.batch_name:
            log.error("You need to provide a batch name to rename.")
            sys.exit(1)

        if not args.new_name:
            log.error("You need to provide a new name for this batch.")
            sys.exit(1)

        batch = None
        try:
            batch = Batch(False)
            batch.load(Batch.get({ "name": args.batch_name }).next())
        except:
            log.error("Error loading batch.")
            sys.exit(1)

        batch["name"] = args.new_name
        batch.save()

    # add repetitions to batch, set unfinished
    elif args.action == "set-repetitions":

        if not args.batch_name:
            log.error("You need to provide a batch name to rename.")
            sys.exit(1)

        if not args.repetitions:
            log.error("You need to provide a number of repetitions.")
            sys.exit(1)

        batch = None
        try:
            batch = Batch(False)
            batch.load(Batch.get({ "name": args.batch_name }).next())
        except:
            log.error("Error loading batch.")
            sys.exit(1)

        batch["repetitions"] = args.repetitions
        batch["date_stopped"] = batch["date_started"]

        batch.save()

    elif args.action == "set-generator":

        if not args.batch_name:
            log.error("You need to provide a batch name.")
            sys.exit(1)

        if not args.input:
            log.error("You need to provide a new JSON file.")
            sys.exit(1)

        batch = None

        try:
            batch = Batch(False)
            batch.load(Batch.get({ "name": args.batch_name }).next())
        except:
            log.error("Error loading batch.")
            sys.exit(1)

        pex = from_file(args.input)
        batch.update_generator(pex)

        # mark unfinished (in general)
        batch["date_stopped"] = batch["date_started"]

        batch.save()

    # batch info
    elif args.action == "batch-info":

        if not args.batch_name:
            log.error("You need to provide a batch name.")
            sys.exit(1)

        batch = None
        try:
            batch = Batch.get({ "name": args.batch_name }).next()
            del(batch["_id"])
            batch["date_started"] = str(batch["date_started"])
            batch["date_stopped"] = str(batch["date_stopped"])
            batch["generator"] = json.loads(batch["generator"])
            if batch["type"] == "race":
                batch["configurations"] = json.loads(batch["configurations"])
            print(json.dumps(batch, indent = 4))
        except Exception as e:
            log.error(e)
            log.error("Error loading batch.")
            sys.exit(1)

    # show non-pruned solutions
    elif args.action == "show-winning":

        if not args.batch_name:
            log.error("You need to provide a batch name.")
            sys.exit(1)

        batch = None
        try:
            batch = Batch.get({ "name": args.batch_name }).next()
            if batch["type"] != "race":
                log.error("This is not a race.")
                system.exit(1)

            winning = [j for j in json.loads(batch["configurations"]) if j["sum_of_ranks"]]
            print(json.dumps(winning, indent = 4))

        except Exception as e:
            log.error(e)
            log.error("Error loading batch.")
            sys.exit(1)

    # show non-pruned solutions
    elif args.action == "show-best":

        if not args.batch_name:
            log.error("You need to provide a batch name.")
            sys.exit(1)

        batch = None
        try:
            batch = Batch.get({ "name": args.batch_name }).next()
            if batch["type"] != "race":
                log.error("This is not a race.")
                system.exit(1)

            winning = [j for j in json.loads(batch["configurations"]) if j["sum_of_ranks"]]
            best = None
            for j in winning:
                if not best or best[0]["sum_of_ranks"] > j["sum_of_ranks"]:
                    best = [j]

            for j in winning:
                if j["sum_of_ranks"] == best[0] and j != best[0]:
                    best.append(j)

            print(json.dumps(best, indent = 4))

        except Exception as e:
            log.error(e)
            log.error("Error loading batch.")
            sys.exit(1)

    # dump csv of experiments
    elif args.action == "dump-experiments":

        if not args.batch_name:
            log.error("You need to provide a batch name to remove.")
            sys.exit(1)

        batch = None
        try:
            batch = Batch(False)
            batch.load(Batch.get({ "name": args.batch_name }).next())
        except:
            log.error("Error loading batch.")
            sys.exit(1)

        experiments = Experiment.get({ "batch": batch["_id"] })
        if not experiments.count():
            sys.exit(0)

        # stat headers
        e = experiments.next()
        stat_head = list(map(str, e["stats"].keys()) if "stats" in e else [])
        if args.stats:
            stat_head = args.stats

        batch.initialize_experiments()
        generator = batch.generator
        head = generator.headers()
        full_head = head[:]
        full_head.extend(stat_head)

        print(",".join(full_head))

        experiments = Experiment.get({ "batch": batch["_id"] })
        for e in experiments:
            l = [(str(e["parameters"][h]) if e["parameters"][h] != None else "true") if h in e["parameters"] else "" for h in head]
            if stat_head:
                l.extend((str(e["stats"][s]) if e["stats"][s] != None else "") if s in e["stats"] else "" for s in stat_head)

            print(",".join(l))

    Persistent.disconnect()

def prepare_args(parser):
    """Prepare the arguments for the program"""
    parser.add_argument("--input", "-i", required = False, type=str, help="the JSON input file")
    parser.add_argument("--executable", "-e", required = False, type=str, help="the executable to use")
    parser.add_argument("--action", "-a", required = False, type=str, default = "print-cll", choices=["batch-info", "show-best", "mark-unfinished", "rename-batch", "delete-batch", "dump-experiments", "show-winning", "run-batch", "run-race", "list-batches", "print-cll", "print-csv", "set-repetitions", "set-generator"], help="the action to execute")
    parser.add_argument("--repetitions", "-r", required = False, type=int, default = 1, help="number of repetitions of each experiment on a single instance")
    parser.add_argument("--instance-param", "-ip", required = False, type=str, help="name of the parameter representing the instance in a race")
    parser.add_argument("--performance-param", "-pp", required = False, type=str, help="name of the parameter representing the performance metric in a races")
    parser.add_argument("--initial-block", "-ib", required = False, type=int, default = 10, help="size of the initial block of experiments in a race")
    parser.add_argument("--confidence", required = False, type=float, default = 0.05, help="confidence for the hypotesis testing in a race")
    parser.add_argument("--batch-name", "-n", required = False, type = str, help = "name of the batch on the database")
    parser.add_argument("--parallel-threads", "-p", required = False, type = int, default = cpu_count(), help="number of parallel threads onto which to run the experiments, with slurm this is the max task concurrency")
    parser.add_argument("--greedy", "-g", required = False, type = bool, default = False, help="whether the experiment can be reused from every batch in the database (true) or just the current one (false)")
    parser.add_argument("--log-file", required = False, type = str, help="file where the whole log is written")
    parser.add_argument("--log-level", required = False, type = str, default="info", choices=["warning", "error", "info"] )
    parser.add_argument("--db-host", "-dh", required = False, type = str, default=Persistent.config["host"], help="the host where the database is installed")
    parser.add_argument("--db-port", "-dp", required = False, type = int, default=Persistent.config["port"], help="the port onto which the database is served")
    parser.add_argument("--db-database", "-dd", required = False, type = str, default=Persistent.config["database"], help="the database name")
    parser.add_argument("--db-user", "-du", required = False, type = str, default=Persistent.config["user"], help="the database username")
    parser.add_argument("--db-pass", "-dx", required = False, type = str, default=Persistent.config["pass"], help="the database password")
    parser.add_argument("--scm", required = False, type = str, default="", choices=["", "git", "mercurial"], help="kind of SCM used")
    parser.add_argument("--seed", "-s", required = False, type = int, default=0, help="seed to use, e.g. for race")
    parser.add_argument("--new-name", "-nn", required = False, type = str, help="new name for the batch")
    parser.add_argument("--prefix", "-pre", required = False, type = str, default=ParameterExpression.def_prefix, help="prefix character(s) for arguments")
    parser.add_argument("--filter", "-f", required = False, type = str, help="filter printouts")
    parser.add_argument("--separator", "-sep", required = False, type = str, default=ParameterExpression.def_separator, help="separator character(s) for arguments")
    parser.add_argument("--stats", "-st", required = False, type = str, nargs="+", help="list of stats to export in CSV")
    parser.add_argument("--limit", "-l", required = False, type = int, default=0, help="how many batches to show in the list")
    parser.add_argument("--slurm", "-sl", required = False, type = bool, default=False, help="run on a slurm cluster")
    parser.add_argument("--slurm-time", "-slt", required = False, type = str, default="02:00:00", help="time limit for each task in HH:MM:SS, note that priority is reduced for long tasks")
    parser.add_argument("--slurm-cpus", "-slc", required = False, type = int, default=1, help="cpus per task")
    parser.add_argument("--slurm-partition", "-slq", required = False, type = str, default="", help="the slurm partition(s) to submit to, can specify multiple comma separated partitions")
    parser.add_argument("--slurm-mem", "-slm", required = False, type = int, default=0, help="memory requested per task in MB (defaults to cluster default)")

    parser.add_help = True
    parser.prefix_chars = "-"
    parser.description = "Generates a number of parameter configurations from a JSON definition file, then uses them to either run experiments, tune parameter or just print out the parameter configurations."

def from_file(file):
    """Generates parameter expression from file name."""

    # open file
    try:
        input_file = open(file, "r")
        json_str = input_file.read()
    except Exception as e:
        log.error("Impossible to open file " + file + " for reading.")
        sys.exit(1)

    try:
        pex = ParameterExpression.from_string(json_str)
    except Exception as e:
        log.error("Impossible to generate ParameterExpression.")
        log.error(e)
        sys.exit(1)

    return pex

def print_table(rows):
    """Kindly donated by stackoverflow user"""

    if len(rows) > 0:
        headers = rows[0]._fields
        lens = []
        for i in range(len(rows[0])):
            lens.append(len(max([x[i] for x in rows] + [headers[i]],key=lambda x:len(str(x)))))
        formats = []
        hformats = []
        for i in range(len(rows[0])):
            if isinstance(rows[0][i], int):
                formats.append("%%%dd" % lens[i])
            else:
                formats.append("%%-%ds" % lens[i])
            hformats.append("%%-%ds" % lens[i])
        pattern = " | ".join(formats)
        hpattern = " | ".join(hformats)
        separator = "-+-".join(['-' * n for n in lens])
        print(hpattern % tuple(headers))
        print(separator)
        for line in rows:
            print(pattern % tuple(line))

# Run
if __name__ == "__main__":
    main()
