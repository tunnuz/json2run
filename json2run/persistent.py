from pymongo.database import Database
from pymongo.mongo_client import MongoClient
from threading import Lock
from bson.objectid import ObjectId
import sys
import os
from subprocess import *
import logging as log


def synchronized(fun):
    """Synchronized decorator, synchronizes a piece of code using the self.sync lock."""

    def _synchronized_fun(self, *args, **kwargs):
        self.lock()
        fun(self, *args, **kwargs)
        self.unlock()

    return _synchronized_fun


class Persistent(object):
    """An object which can be saved on the database."""

    connection = None
    """Current connection to database daemon."""

    database = None
    """Current database."""

    scm = None
    """SCM used for the codebase."""

    config = {
        "host": "localhost",
        "port": 27017,
        "user": "j2r",
        "pass": "j2r",
        "database": "j2r"
    }
    """Default values for connection."""

    def __init__(self, inner=None):
        """Initialize with empty object."""

        self.sync = Lock()

        if inner != None:
            self.inner = inner
        else:
            self.inner = {}

    def lock(self):
        """Acquire lock."""
        self.sync.acquire()

    def unlock(self):
        """Release lock."""
        self.sync.release()

    def __getitem__(self, field):
        """Subscript getter."""
        try:
            # try getting by key
            return self.inner[field]
        except KeyError:
            # if key not found, could be an index
            try:
                return list(self.inner.items())[int(field)][1]
            except ValueError:
                # not an index
                raise KeyError
            except IndexError:
                # no more elements 
                raise StopIteration

    def __setitem__(self, field, value):
        """Subscript setter."""
        self.inner[field] = value

    def __contains__(self, field):
        """Checks whether container has item."""
        return field in self.inner

    def save(self):
        """Upsert inner object."""

        database = Persistent.database
        try:
            if "_id" not in self:
                self["user"] = Persistent.user()
                self["host"] = Persistent.host()
                self["system"] = Persistent.platform()

            self["_id"] = database[self.collection()].save(self.inner)
        except Exception as e:
            print("Failed saving on database: ", e)

    @classmethod
    def collection(cls):
        """Get name of the collection where this kind of persistent is saved."""
        pass

    @classmethod
    def get(cls, query, fields=None):
        """Get matching persistents from database."""

        database = Persistent.database
        return database[cls.collection()].find(query, fields)

    @classmethod
    def remove(cls, query):
        """Remove matching persistents from database."""

        database = Persistent.database
        database[cls.collection()].remove(query)

    @staticmethod
    def connect(**kwargs):
        """Connect to the database with the specified data."""

        config = kwargs
        config["pass"] = kwargs["passw"]
        del (config["passw"])
        Persistent.config = config

        try:
            Persistent.connection = MongoClient(config["host"], config["port"])
            Persistent.database = Persistent.connection[config["database"]]
            Persistent.database.authenticate(config["user"], config["pass"])
        except Exception as e:
            print(e)
            sys.exit(1)

    @staticmethod
    def disconnect():
        """Disconnect from database."""

        Persistent.connection.close()
        Persistent.database = None

    def load(self, obj):
        """Load object literal in object."""
        self.inner = obj

    @classmethod
    def exists(cls, obj):
        """Checks if a document with the specified fields exists in the database."""

        database = Persistent.database
        res = database[cls.collection()].find_one(obj)
        return res != None

    def get_id(self):
        """Get object id of persistent object."""

        if "_id" not in self.inner:
            self.save()

        return self["_id"]

    @staticmethod
    def run_and_report(cmd):

        """Run command on the shell, report stdout, stderr"""
        proc = Persistent.run(cmd)
        proc.wait()
        output = "".join([x.rstrip() for x in proc.stdout])
        return output

    @staticmethod
    def run(cmd):
        """Run """
        return Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE,
                     close_fds=True)  # close_fds doesn't seem to work properly

    @staticmethod
    def revision():
        """Get scm revision."""

        if Persistent.scm == "git":
            return Persistent.run_and_report("git log -n 1 | grep commit | sed -e \"s/commit //\"", None)
        elif Persistent.scm == "mercurial":
            return Persistent.run_and_report("hg log -r tip | grep changeset | sed -e \"s/.*:*://\"", None)
        return ""

    @staticmethod
    def host():
        """Get current machine hostname."""

        return Persistent.run_and_report("hostname")

    @staticmethod
    def user():
        """Get current user."""

        return Persistent.run_and_report("whoami")

    @staticmethod
    def platform():
        """Get current platform."""

        return Persistent.run_and_report("uname -sr")
