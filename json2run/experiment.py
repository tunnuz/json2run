import json
from threading import *
from .persistent import *
from .parameterexpression import *
from time import sleep
import random
from datetime import datetime
import logging as log


class ExperimentRunner(Thread):
    """Experiment consumer, handles experiment launching and interruption."""

    def __init__(self, batch):
        """Saves reference to batch, for interrupt detection."""

        super(ExperimentRunner, self).__init__()
        self.batch = batch

    def run(self):
        """Consume experiments until the queue has more to process."""

        # until the queue is empty
        while True:

            # get experiment from queue
            self.current = self.batch.experiment_q.get()
            self.batch.experiment_started(self.current)

            # be nice with enqueuing thread
            sleep(0.1)

            # get prefix and separator information from batch (if possible)
            prefix = self.batch["prefix"] if "prefix" in self.batch else None
            separator = self.batch["separator"] if "separator" in self.batch else None

            # generate command line
            parameters = ParameterExpression.format(None, self.current.parameters, separator, prefix)

            # check if batch has been interrupted in the meanwhile
            if self.current.interrupted:

                # remove experiment from queue without executing it
                self.batch.experiment_finished(self.current)
                self.batch.experiment_q.task_done()

            else:

                # run experiment, record time, save output
                self.current["date_started"] = datetime.utcnow()

                # print 
                if self.current.total and not self.current.interrupted:
                    log.info("Running (%d/%d) %s %s" % (
                    self.current.incremental, self.current.total, self.current.executable, parameters))
                else:
                    log.info("Running %s %s" % (self.current.executable, parameters))

                # open subprocess and wait for it to finish
                try:

                    self.current.process = Persistent.run("%s %s" % (self.current.executable, parameters))
                    self.current.status = self.current.process.wait()

                except Exception as e:

                    if not self.current.interrupted:
                        print("Failed running experiment: ", e)

                self.current["date_stopped"] = datetime.utcnow()

                # process output, save experiment (if valid)
                self.terminate()

                # cleanup process information
                self.current.clean()

                # notify batch that experiment is over
                self.batch.experiment_finished(self.current)

                # task is done
                self.batch.experiment_q.task_done()

    def terminate(self):
        """Get experiment result, save it, or kill experiment."""
        try:

            # read from stdout
            output = "".join(self.current.process.stdout)
            errs = "".join(self.current.process.stderr)

            # parse the JSON output, save results
            if self.current.status != 0:
                log.error("Output: **%s**, status: %s, errs: %s" % (output, self.current.status, errs))
                self.current.interrupted = True

            # if interrupted or wrong 
            if self.current.interrupted:
                return

            # parse stats, save result
            json_output = json.loads(output)

            # if output includes an array of solutions, store it on the database
            if "solutions" in json_output:
                self.current["solutions"] = json_output["solutions"]
                del (json_output["solutions"])

            # consider the rest of information as stats
            self.current["stats"].update(json_output)
            self.current.save()

        except Exception as e:

            print("Failed reading experiment results: ", e)

            # output wasn't valid JSON, ignore result (don't save it)
            self.current.interrupted = True


class Experiment(Persistent):
    """A single experiment."""

    def __init__(self, batch, executable, params, iteration=None):
        """Initializes an experiment."""

        super(Experiment, self).__init__()

        self.process = None
        self.status = 1
        self.incremental = 0
        self.total = 0
        self.iteration = iteration
        self.interrupted = False
        self.batch = batch
        self.parameters = [p for p in params if p.name != "repetition"]
        self.executable = executable
        self["batch"] = batch.get_id()
        self["executable"] = executable
        self["parameters"] = {p.name: p.value for p in params}
        self["copy"] = False
        self["stats"] = {}
        self["solutions"] = []

    def load(self, obj):
        """Load database object and reconstitutes experiment."""
        super(Experiment, self).__init__(obj)

    def kill(self):
        """Kill experiment."""
        self.lock()
        self.interrupted = True
        if self.process:
            try:
                self.process.kill()
                self.process.wait()
            except:
                self.process = None
        self.unlock()

    def clean(self):
        """Cleanup process execution."""
        # if self.process:
        # self.process.stdout.close()
        # self.process.stderr.close()
        self.process = None

    def set_incremental(self, incremental, total):
        """Incremental and total indices (for logging)."""

        self.incremental = incremental
        self.total = total

    def on_db(self):
        """Checks whether this experiment is already on the db, if greedy = false
        only checks if the experiment is in the current batch."""

        query = {}
        for p in self["parameters"]:
            query["parameters.%s" % p] = self["parameters"][p]
        del (query["parameters.repetition"])
        query["copy"] = False  # we're not interested in copies

        return Experiment.exists(query)

    def on_batch(self):
        """Checks whether this experiment is already on the db, if greedy = false
        only checks if the experiment is in the current batch."""

        query = {}
        query["parameters"] = self["parameters"]
        query["batch"] = self["batch"]
        return Experiment.exists(query)

    def get_similar(self):
        """Get list of similar (i.e. with same parameters and executable) experiments out of this batch."""

        query = {}
        for p in self["parameters"]:
            query["parameters.%s" % p] = self["parameters"][p]
        del (query["parameters.repetition"])
        query["copy"] = False  # we're not interested in copies
        query["executable"] = self["executable"]  # same executable
        query["batch"] = {"$nin": [self["batch"]]}

        return Experiment.get(query)

    @classmethod
    def collection(cls):
        return "experiments"

    def copy_to(self, batch, repetition):
        """Create copy of this experiment."""

        new = Experiment(batch, self.executable, self.parameters)
        new["parameters"]["repetition"] = repetition
        new["stats"] = self["stats"]
        new["solutions"] = self["solutions"]
        new["date_started"] = self["date_started"]
        new["date_stopped"] = self["date_stopped"]
        new["copy"] = True

        return new
