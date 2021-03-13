from __future__ import print_function
from . persistent import Persistent
import sys
from bson.objectid import ObjectId
from json2run import *
from . parameterexpression import *
from threading import *
from multiprocessing import cpu_count
from . experiment import *
from sys import version_info
if version_info[0] < 3:
    from Queue import Queue
else:
    from queue import Queue
import datetime, time
from time import sleep
from scipy.stats import rankdata, chi2, t as tstudent, wilcoxon
from math import *
import logging as log
import random

class Batch(Persistent):
    """Represent a set of experiments originated by the same parameter expression."""

    def __init__(self, new = True, **kwargs):
        """Initialize a full batch (no race) from a parameter expression."""
        
        super(Batch, self).__init__()

        self.start_lock = Lock()
        self.finish_lock = Lock()
 
        if new:
            # check if all needed parameters are in place
            missing = False
            for needed in ["generator", "name", "executable", "repetitions", "separator", "prefix"]:
                try:
                    self[needed] = kwargs[needed]
                except:
                    missing = True
                    log.error("'%s' is a mandatory parameter." % needed)

            if missing:
                raise ValueError
            
            # initialize generator, dates
            self.generator = self["generator"]
            self["generator"] = str(self.generator)
            self["type"] = self.type()            
            self["date_started"] = datetime.datetime.utcnow()
            self["date_stopped"] = datetime.datetime.utcnow()

        self.interrupted = False
        self.initialized = False
        self.running = []
        self.enqueued = []
 
    def update_generator(self, pex):

        self.generator = pex
        self["generator"] = str(self.generator)

    def initialize_experiments(self):
        """Initialize generator."""

        # add repetitions
        rep = Discrete({ "name": "repetition", "values": range(self["repetitions"]) })               
        self.generator = And({})
        self.generator.add_descendant(rep)
        self.generator.add_descendant(ParameterExpression.from_string(self["generator"]))
        
        self.initialized = True
    
    def completion(self, greedy = False):
        """Reports completion level of a batch."""
        return Experiment.get({ "batch": self["_id"] }).count() / (self.generator.count() * self["repetitions"]) * 100.0
    
    def missing(self):
        """Reports the number of missing experiments."""
        return(self.generator.count() * float(self["repetitions"]) - Experiment.get({ "batch": self["_id"] }).count())
    
    def load(self, obj):
        """Load database object and reconstitutes batch, to be used together with Batch(False)."""
        super(Batch, self).load(obj)
        
        # backwards compatibility
        if not "separator" in self:
            self["separator"] = ParameterExpression.def_separator

        if not "prefix" in self:
            self["prefix"] = ParameterExpression.def_prefix
            
        self.generator = ParameterExpression.from_string(self["generator"])
        
    def run(self, thread_n = cpu_count(), greedy = False):
        """Runs a whole batch of experiment, possibly skipping experiment which have been already run on this or other batches."""
        
        self.experiment_q = ExperimentQueue(thread_n)
        
        # initialize once
        if not self.initialized:
            self.initialize_experiments()
        
        # save current state
        log.info("Running batch with %d parallel threads and %s." % (thread_n, ("greedy" if greedy else "non greedy")))
        self["threads"] = thread_n
        self.save()

        # spawn thread_n-sized thread pool so that we start running straight away
        log.info("Initializing workers ...")
        for ti in range(thread_n):
            t = ExperimentRunner(self)
            t.setDaemon(True)
            t.start()
    
        try: 
            
            # populate experiment queue (skip existing)
            log.info("Generating experiments ...")
            generated_count = 0
            total_count = self.generator.count()

            # enqueue experiments as they are generated
            while self.generator.has_more() and not self.interrupted:

                generated_count += 1
                e = Experiment(self, self["executable"], self.generator.next())
                executable = e.executable
                parameters = ParameterExpression.format(None, e.parameters, self["separator"], self["prefix"])
           
                # if experiment is already on batch, just skip
                if e.on_batch():
                    log.info("Skipping (%d/%d) %s %s" % (generated_count, total_count, executable, parameters)) 
                    continue
                
                # consider experiments in whole database
                if greedy:
 
                    # look for similar experiments
                    similar = e.get_similar()
                    repetition = e["parameters"]["repetition"]
 
                    # handle repetitions
                    if repetition >= similar.count():

                        # run experiment anyway (don't have enough repetitions)
                        e.set_incremental(generated_count, total_count)
                        self.enqueued.append(e)
                        self.experiment_q.put(e)

                    else:
                        # skip the first repetition-1 experiments
                        for i in range(repetition):
                            similar.next()
                        
                        # copy (mark) the repetition-th experiment to this batch
                        e.load(similar.next())
                        n = e.copy_to(self, repetition)
                        n.save()
                        self.save()
                        log.info("Copying (%d/%d) %s %s" % (generated_count, total_count, executable, parameters))
                else:

                    # run experiment normally
                    e.set_incremental(generated_count, total_count)
                    self.enqueued.append(e)
                    self.experiment_q.put(e)
                    
            # wait for experiments to finish
            while self.running or self.experiment_q.qsize():
                try:
                    self.experiment_q.join_with_timeout(1)
                except NotFinished:
                    continue
            
            # final save
            if not self.interrupted:
                self["date_stopped"] = datetime.datetime.utcnow()

            self.save()
        
        except KeyboardInterrupt:
            
            log.info("\nStopping experiments ...")
            self.interrupted = True

            # kill all enqueued and running processes
            map(lambda x: x.kill(), self.enqueued)
            map(lambda x: x.kill(), self.running)

    def type(self):
        """Describes type of batch."""
        return "full"

    def experiment_started(self, experiment):
        """Add experiment to the list of running ones."""
        self.start_lock.acquire()
        self.enqueued.remove(experiment)
        self.running.append(experiment)
        self.start_lock.release()
        
    def experiment_finished(self, experiment):
        """Remove experiment from the list of running ones."""
        self.finish_lock.acquire()
        self.running.remove(experiment)
        self.finish_lock.release()
    
    @classmethod
    def collection(cls):
        return "batches"
    
class Race(Batch):
    
    def __init__(self, new = True, **kwargs):
        """Initialize race among configurations described in a file."""
        
        super(Race, self).__init__(new, **kwargs)
                
        if new:
            # check if all needed parameters are in place
            missing = False
            for needed in ["generator", "name", "executable", "repetitions", "initial_block", "instance_parameter", "performance_parameter", "seed", "separator", "prefix"]:
                try:
                    self[needed] = kwargs[needed]
                except:
                    missing = True
                    log.error("'%s' is a mandatory parameter." % needed)

            if missing:
                raise ValueError()
                
            # initialize generator, dates
            self.generator = self["generator"]
            self["generator"] = str(self.generator)
  
    def initialize_experiments(self):
        """Initialize generator."""

        # initialize generator
        self.generator = ParameterExpression.from_string(self["generator"])

        # intitialize set of instances and configurations
        instances = set()
        self.configurations = set()

        # separate instances from configurations 
        log.info("Generating all %d experiments" % self.generator.count())
        while self.generator.has_more():
            n = ParameterList(self.generator.next())
            
            # get value of instance parameter
            inst = n[self["instance_parameter"]]
            instances.add(inst.value)

            # rest of the experiment is a configuration
            n.remove(inst)
            self.configurations.add(n)

        # set -> list
        instances = list(instances)
        self.configurations = list(self.configurations)

        log.info("Race has %d configurations, %d instances (%d experiments for each configuration)." % (len(self.configurations), len(instances), len(instances) * int(self["repetitions"])))

        # seed, shuffle instances
        log.info("Shuffling instances with seed %d" % int(self["seed"]))
        random.seed(int(self["seed"]))
        random.shuffle(instances)
        
        # copy of instances for use in race
        self.instances = list(instances)
        
        # create instance generator
        inst = Discrete({ "name": self["instance_parameter"], "values": instances })
        
        # create instance generator
        rep = Discrete({ "name": "repetition", "values": range(int(self["repetitions"])) })

        # initialize instance and repetition generator (instance first)
        self.inst_generator = And({})
        self.inst_generator.add_descendant(rep)
        self.inst_generator.add_descendant(inst)
        
        self.initialized = True

    def completion(self, greedy = False):
        """Reports (estimated) completion level of a race."""
        
        (racing, total) = self.racing()

        if racing > 1:
            try:
	            p_value = " (p-value: %.2f)" % self["p_value"]
            except:
                p_value = ""
        else:
            p_value = ""
        return "%d / %d%s" % (racing, total, p_value)
        
    def racing(self):
        """Return number of racing, total configurations."""
        
        racing = 0
        total = 0
        for c in json.loads(self["configurations"]):
            if c["sum_of_ranks"] != None:
                racing += 1
            total += 1
        
        return (racing, total)
    
    def missing(self):
        """Compute estimated number of missing experiments."""
        
        (racing, total) = self.racing()        
        return (self.generator.count() * self["repetitions"] // total - self["iterations_completed"]) * racing

    def load(self, obj):
        """Load from database"""
        
        super(Batch, self).load(obj)
        
        # backwards compatibility        
        if not "separator" in self:
            self["separator"] = ParameterExpression.def_separator

        if not "prefix" in self:
            self["prefix"] = ParameterExpression.def_prefix
            
        # add repetitions to parameter expression
        self.generator = ParameterExpression.from_string(self["generator"])

    def set_generator(self, pex):
        
        self.update_generator(pex)
        self.initialize_experiments()

    def run(self, thread_n = cpu_count(), greedy = False, alpha = 0.05):
        """Runs a race of configurations, by constantly pruning inferior configurations."""

        log.info("Initializing experiments (this might take a while)")

        self.experiment_q = ExperimentQueue(thread_n)

        # initialize once
        if not self.initialized:
            self.initialize_experiments()

        log.info("Running with %d parallel threads, alpha = %f and %s." % (thread_n, alpha, ("greedy" if greedy else "non greedy")))
        
        self.greedy = greedy
        self.alpha = alpha
        
        # initialize data structures for configurations
        self.racing = range(0,len(self.configurations))
        
        # initialize configurations dictionary (for logging)
        self.configurations_dict = [ { p.name: p.value for p in conf } for conf in self.configurations ]
        
        for i in range(len(self.configurations_dict)):
            self.configurations_dict[i]["iterations_completed"] = 0
            self.configurations_dict[i]["sum_of_ranks"] = float("inf")
        
        log.info("Racing configurations: %s" % self.racing)
        
        # instantiate empty arrays for each configuration
        self.experiments = { conf_index: [] for conf_index in range(0, len(self.configurations)) }

        # batch info (for logging)
        self.iterations_completed = 0
        self["iterations_completed"] = self.iterations_completed
        self["threads"] = thread_n
        self["configurations"] = json.dumps(self.configurations_dict)
                        
        self.save()

        # spawn thread_n-sized thread pool
        for ti in range(thread_n):
            t = ExperimentRunner(self)
            t.setDaemon(True)
            t.start()
                        
        # keep track of what we have already run, for each iteration
        self.executed = []
        self.iteration = []
        
        # experiments started for each configuration (for killing)
        self.started = { ParameterList(c): [] for c in self.configurations }

        # start generating experiments
        try:
            
            current_inst = None
            started_iteration = -1

            # enqueued experiments, experiments on database, missing experiments
            enqueued = []
            on_db = []
            missing = []
                
            # if there are more racing configurations and instances
            while (len(self.racing) > 1 and self.inst_generator.has_more()):
                                         
                # if there are no experiments to run
                if not missing:
                                        
                    # increment iteration
                    current_inst = ParameterList(self.inst_generator.next())
                    started_iteration += 1
                                        
                    # initialize executed experiments for this instance
                    self.iteration.append(current_inst)
                    self.executed.append([])
                    
                    # enqueued experiments, experiments on db
                    enqueued = []
                    on_db = []
                    
                    # print current instance
                    log.info("Iteration: %s" % current_inst)
                    
                
                # sort racing by sum of ranks (low sum of ranks are run first)
                racing = list(self.racing)
                racing.sort(key = lambda c: self.configurations_dict[c]["sum_of_ranks"])
                
                # recompute missing (with new race information, sorted by sum of ranks)
                missing = filter(lambda x: x not in enqueued+on_db, [ParameterList(self.configurations[c]) for c in self.racing])
                
                # if there are no missing experiments for this iteration, move on
                if not missing:
                    continue
                
                # otherwise get first missing experiment
                configuration = missing.pop(0)
                    
                # instantiate experiment object
                parameters = list(configuration)
                parameters.extend(current_inst)                
                executable = self["executable"]
                e = Experiment(self, executable, parameters, started_iteration)
                
                # generate parameter expression (for logging purposes)
                parameters = ParameterExpression.format(None, e.parameters, self["separator"], self["prefix"])

                # if experiment already on batch, skip it
                if e.on_batch():
                    log.info("Skipping %s %s" % (executable, parameters))
                    self.enqueued.append(e)
                    self.experiment_started(e)
                    on_db.append(configuration)
                    self.experiment_finished(e)
                    continue

                # if greedy, try to get similar experiments from db
                if greedy:
                    similar = e.get_similar()
                    repetition = e["parameters"]["repetition"]

                    # if search is negative, execute current experiment
                    if repetition >= similar.count():                        
                        self.started[configuration].append(e)
                        self.enqueued.append(e)
                        enqueued.append(configuration)
                        self.experiment_q.put(e)
                        
                    # otherwise copy it from db
                    else:
                        for i in range(0, repetition):
                            similar.next()
                        e.load(similar.next())
                        n = e.copy_to(self, repetition)
                        n.save()
                        log.info("Copying %s %s" % (executable, parameters))
                        self.enqueued.append(e)
                        self.experiment_started(e)
                        on_db.append(configuration)
                        self.experiment_finished(e)

                # if not greedy (and experiment not in batch, execute it)
                else:                    
                    self.started[configuration].append(e)
                    enqueued.append(configuration)
                    self.enqueued.append(e)
                    self.experiment_q.put(e)
                        
            # wait for experiments to finish
            while self.running or self.experiment_q.qsize():
                try:
                    self.experiment_q.join_with_timeout(1)
                except NotFinished:
                    continue
            
            # final save
            if not self.interrupted:
                self["date_stopped"] = datetime.datetime.utcnow()
            
            self.save()
            
            
        except KeyboardInterrupt:
            
            log.info("\nStopping experiments ...")
            self.interrupted = True
            
            # kill all running processes
            map(lambda x: x.kill(), self.enqueued)
            map(lambda x: x.kill(), self.running)
                
    def experiment_started(self, experiment):
        super(Race, self).experiment_started(experiment)
        
    def experiment_finished(self, experiment):
        """Handle experiment end, trigger pruning of inferior if needed."""

        self.finish_lock.acquire()
        self.running.remove(experiment)

        # if we have interrupted the execution, exit
        if self.interrupted or experiment.interrupted:
            if not self.interrupted:
                log.error("Experiment %s from iteration %d exited with status %d!" % (experiment.parameters, experiment.iteration, experiment.status))
            self.finish_lock.release()
            return
        
        # get the experiment's configuration
        configuration = [p for p in experiment.parameters if p.name != self["instance_parameter"]]
        
        # add terminated experiment to list of executed experiments
        self.executed[experiment.iteration].append(ParameterList(configuration))

        for e in range(len(self.executed)):
           if e >= self.iterations_completed and len(self.executed[e]):
               print("Iteration %d has %d experiments to go." % (e, len(self.racing) - len(self.executed[e])))

        # only process current iteration (to be compliant with race)
        if self.iterations_completed != experiment.iteration:
            self.finish_lock.release()
            return
            
        # process completed iterations in order
        while True:
            
            # gather needed experiments to perform next pruning
            needed = [ParameterList(self.configurations[c]) for c in self.racing]
            needed_in_place = set(needed).issubset(set(self.executed[self.iterations_completed]))
            
            # if all needed experiments are terminated
            if needed_in_place:
            
                # also check if we still need to race
                if len(self.racing) > 1:
                                         
                    self.prune_inferiors()
                    self.iterations_completed += 1
                
                    # update survival data
                    for i in range(len(self.configurations_dict)):
                        self.configurations_dict[i]["sum_of_ranks"] = None

                    for i in range(len(self.racing)):
                        c = self.racing[i]
                        self.configurations_dict[c]["iterations_completed"] = self.iterations_completed
                        self.configurations_dict[c]["sum_of_ranks"] = self.last_sor[i]

                    # update counter, just for completion info
                    self["iterations_completed"] = self.iterations_completed
                    self["configurations"] = json.dumps(self.configurations_dict)
                
                    # save temporary status of race
                    self.save()
                
                    # if we're still racing
                    if len(self.racing) > 1:
                        
                        pass 

                        # kill all experiments related to pruned configurations
                        # pruned = [c for c in self.started if c not in map(lambda x: self.configurations[x], self.racing)]
                        # for p in pruned:
                        #    while self.started[p]:
                        #        e = self.started[p].pop(0)
                        #        e.kill()
                                
                    # if we have already a winner
                    else:
                        
                        # kill all enqueued experiments
                        for p in self.started:
                            while self.started[p]:
                                e = self.started[p].pop(0)
                                e.kill()
                                
                    # avoid trying to prune with an empty iteration      
                    if len(self.executed) <= self.iterations_completed:
                        break
                else:
                    
                    # kill all enqueued experiments, then break (no more pruning to do)
                    for p in self.started:
                        while self.started[p]:
                            e = self.started[p].pop(0)
                            e.kill()
                    break                    
            else:
                break

        self.finish_lock.release()

    def prune_inferiors(self):
        """Prune inferior configurations according to Friedman and Wilcoxon tests."""
        
        # get all experiments of the current configurations
        for c in self.racing:
            
            self.instance = self.iteration[self.iterations_completed]
            
            parameters = { "parameters."+p.name: p.value for p in self.configurations[c]+self.instance }
            parameters = dict(parameters.items() + { "batch": self.get_id() }.items())

            exp = Experiment.get(parameters, { "stats."+self["performance_parameter"]: 1 })            
            
            e = None
            if exp.count():
                e = exp.next()
            else:
                log.error("Failed fetching %s" % parameters)
                sys.exit(1)
           
            self.experiments[c].append(e["stats"][self["performance_parameter"]])
      
        # compute ranks
        conf_rank, inst_rank, sum_of_ranks, ties = self.compute_ranks([self.experiments[c] for c in self.racing])

        # print debug information about sum of ranks
        for c in self.racing:
            log.info("%s: %s %s, (sum: %s)" % (c, self.experiments[c], conf_rank[self.racing.index(c)], sum_of_ranks[self.racing.index(c)]))
        

        # if it's time to prune
        if self.iterations_completed+1 >= self["initial_block"]:
                        
            n = float(self.iterations_completed+1)
            k = float(len(self.racing))

            log.info("Checking null hypothesis against %d configurations on %d samples ..." % (k, n))

            if k > 2:
                # use friedman
                log.info("Running Friedman rank sum test")
                
                # sum of squares of ( sumOfRanks[treatment] - n * ( k + 1 ) / 2.0 ), which is used in various statistics
                r = sum([ pow(s - n * (k + 1) / 2, 2) for s in sum_of_ranks ])
            
                # ties correction
                ties_correction = sum([sum(map(lambda x: x**3 - x, t)) / (k - 1) for t in ties])
            
                # default statistic
                if (n * k * (k+1)) - ties_correction != 0:
                    statistic = 12 * r / ((n * k * (k+1)) - ties_correction)
                else:
                    statistic = float('inf')
                        
                # sum of squared ranks 
                a = sum([sum([pow(j,2) for j in i]) for i in inst_rank])
            
                log.info("Statistic: %f" % statistic)
            
                p_value = 1 - chi2.cdf(statistic, k-1)
                
                self["p_value"] = p_value
                
                log.info("P-value: %f" % p_value)
            
                if p_value <= self.alpha:
                     
                    t_student = tstudent.ppf(1 - self.alpha / 2, (n-1) * (k-1)) * sqrt(2 * (n * a - sum([ pow(rk,2) for rk in sum_of_ranks ])) / ((n-1) * (k-1)))
                    best_sum = min(sum_of_ranks)
                
                    log.info("t: %f" % (t_student))
                           
                    for c in self.racing:
                        log.info("%d: (%f - %f = %f) vs. %f" % (c, sum_of_ranks[self.racing.index(c)], best_sum, abs(sum_of_ranks[self.racing.index(c)] - best_sum), t_student))

                    self.racing = filter(lambda i: abs(sum_of_ranks[self.racing.index(i)] - best_sum) <= t_student, self.racing)
            
            else:
                
                # wilcoxon
                log.info("Running Wilcoxon signed-rank test")
                
                if n < 10:
                    log.info("Sample size too small for Wilcoxon signed-rank test")
                else:
                    p_value = wilcoxon(*[self.experiments[c] for c in self.racing])[1]
               
                    self["p_value"] = p_value
               
                    log.info("P-value: %s" % p_value)
                
                    if p_value <= self.alpha:
                
                        log.info("Old racing: %s" % (self.racing))
                    
                        if sum_of_ranks[0] <  sum_of_ranks[1]:
                            self.racing = [self.racing[0]]
                        else:
                            self.racing = [self.racing[1]]

        log.info("New racing: %s" % (self.racing))
        self.last_sor = [sum_of_ranks[self.racing.index(rc)] for rc in self.racing]
        log.info("Sum of ranks: %s" % ([sum_of_ranks[self.racing.index(rc)] for rc in self.racing]))
    
    def compute_ranks(self, experiments):
        """Compute per-configuration ranks."""
        
        # experiments:
        # conf1: [x,y,z,...]
        # conf2: [i,j,k,...]

        inst_first = map(list, zip(*experiments))
        inst_rank = [list(rankdata(l)) for l in inst_first]
        
        # results
        conf_rank = zip(*inst_rank)
        
        # compute ties
        ties = [[len(filter(lambda x: x == t,a)) for t in set(a)] for a in inst_rank]
        
        # calculate sum of ranks
        sum_of_ranks = map(sum, conf_rank)        
        
        return conf_rank, inst_rank, sum_of_ranks, ties
            
    def type(self):
        return "race"
        
    @classmethod
    def collection(cls):
        return "batches"

        
class ExperimentQueue(Queue):
    """Specialization of Queue to have timed join"""
    
    def join_with_timeout(self, timeout):
        self.all_tasks_done.acquire()
        try:
            endtime = time.time() + timeout
            while self.unfinished_tasks:
                remaining = endtime - time.time()
                if remaining <= 0.0:
                    raise NotFinished()
                self.all_tasks_done.wait(remaining)
        finally:
            self.all_tasks_done.release()

class NotFinished(Exception):
    pass
