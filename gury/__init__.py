#!/usr/bin/python
# -*- coding: utf-8 -*-
import datetime
import decimal
import grp
import math
import os
import pwd
import random
import re
import subprocess
import sys
import time
from bson.binary import Binary
from bson.code import Code
from bson.objectid import ObjectId

try:
    from bson.regex import Regex
except ImportError:
    try:
        import bson.re

        Regex = bson.re.compile
    except ImportError:
        Regex = re.compile
from bson.son import SON
from bson.timestamp import Timestamp
from collections import Iterable
from dateutil.parser import parse

try:
    from etc.comparisons import symmetric_recursive_comparison_typestrict, symmetric_recursive_comparison
except ImportError:
    my_full_path = os.path.realpath(__file__)
    my_full_dir = os.path.dirname(my_full_path)
    pos = my_full_dir.find('QANG')
    qa_root = my_full_dir[:pos]
    dir_to_add = qa_root + 'QANG/utils'
    sys.path.append(dir_to_add)
    from etc.comparisons import symmetric_recursive_comparison_typestrict, symmetric_recursive_comparison

from etc.excluded_generated_tests import excluded_generated_tests
from etc.simple_data_creator import collection_generator
from etc.sorting_key import sorting_key
from etc.timed import timed_abort, TimeExpired
from etc.combine_decorators import combine
from etc.auth_decorator import *
from etc.version_decorator import *
from etc.errors_decorator import *
from etc.redhat_decorator import ubuntu_only, redhat_only
from nose.plugins.attrib import attr
from nose.tools import raises
from unittest import TestCase
from pymongo import MongoClient, errors, version

pymongo_version = version.split('.')
import platform

if int(pymongo_version[0]) > 2:
    from bson.int64 import Int64

    NumberLong = Int64
else:
    NumberLong = long
false = False
true = True
null = None
inf = float('inf')
ISODate = parse
NumberInt = int
NaN = float('nan')
Infinity = float('inf')
NegInfinity = float('-inf')

try:
    USER_HOME = os.environ['HOME']
except:
    sys.exit("Please set the HOME environment variable to point to your /home/USER")

log = [USER_HOME, "/memory_fingerprints.log"]

LOG_FILE = "".join(log)
LOG_DIR = log[0]
USER = USER_HOME.split('/')[-1]

SONARTESTS_SONARD = {'server_config': 'simple'}

# try to create the memory log file
try:
    uid = pwd.getpwnam(USER).pw_uid
    gid = grp.getgrnam(USER).gr_gid

    m = open(LOG_FILE, 'a')
    if os.access(LOG_FILE, os.W_OK) or os.access(LOG_DIR, os.W_OK):
        os.chown(LOG_FILE, uid, gid)
    m.close
except:
    pass


class TestCase(TestCase):
    REDHAT_PLATFORM = 'redhat' in platform.platform().lower()

    @classmethod
    def setUpClass(self):
        reset_error_message(TestCase)
        TestCase.sonar_pid = get_sonar_pid()
        try:
            self.SONAR_PORT = int(os.environ.get('SONAR_PORT'))
        except TypeError:
            # default to sonarw
            self.SONAR_PORT = 27117
        self.dbconnection = MongoClient('127.0.0.1', self.SONAR_PORT)
        try:
            self.dbconnection.admin.authenticate('cale', 'jj')
        except:
            print
            'authentication of cale failed'
            sys.exit(1)
        self.sonar_version = get_version(self)
        self.maxDiff = 40960
        self.longMessage = True

    def setUp(self):
        self.curr_time = time.time()

    # assumes each class has at least one test case
    @classmethod
    def tearDownClass(self):
        self.dbconnection.close()

    def tearDown(self):
        logline = ""
        try:
            prev_time = self.curr_time
        except AttributeError:
            logline = "Please override the setUp() method for {0}{1}".format(self.id(), '\n')
        else:
            curr_time = time.time()
            logline = "{0:.2f}s..{1}..{2}MB..{3}{4}".format(curr_time - prev_time, datetime.datetime.now(),
                                                            self.get_vm_size(), self.id(), '\n')
        finally:
            reset_error_message(TestCase)
            if os.access(LOG_FILE, os.W_OK):
                with open(LOG_FILE, 'a') as m:
                    m.write(logline)

    def fail(self, *args):
        error_message = '\n'.join([self.get_error_message()] + map(str, args))
        super(TestCase, self).fail(error_message)

    def run_query_with_abort(self, query, time_limit, query_type, allow_exception=True):
        @timed_abort(time_limit)
        def sonar_call():
            if query_type == 'update':
                result = getattr(self.db, query_type)(*query)
            else:
                result = getattr(self.db, query_type)(self.query)
            if isinstance(result, Iterable):
                [doc for doc in result]

        try:
            sonar_call()
        except TimeExpired as e:
            self.fail(e)
        except AttributeError:
            self.fail(AttributeError('db attribute not initialized..'))
        except Exception as e:
            if allow_exception:
                pass
            else:
                self.fail(e)

    # In newer versions of pymongo aggregations and queries return cursors and command cursors that can be looped
    # through. In older versions of pymongo aggregations and queries would return dictionaries with the
    # results stored in the value of the "result" key.
    def get_results(res):
        if type(res) is dict:
            results = res['result']
        else:
            results = [doc for doc in res]
        return results

    def get_error_message(self):
        if self.db_name and self.coll_name and self.query:
            err_msg = "{3}Database: {0}{3}Collection: {1}{3}Query: {2}".format(self.db_name, self.coll_name, self.query,
                                                                               '\n')
        else:
            err_msg = "\nThis script needs to set self.db_name, self.coll_name and self.query variables."
        return err_msg

    @classmethod
    def get_vm_size(self):
        try:
            vmsize = 0
            if TestCase.sonar_pid:
                proc_file = "/proc/" + TestCase.sonar_pid + "/status"
                if os.path.isfile(proc_file) and os.access(proc_file, os.R_OK):
                    with open(proc_file, 'r') as s:
                        lines = s.readlines()
                    thirteenth_line = lines[12]
                    no_kb = thirteenth_line.split(' ')[-2]
                    vmsize = no_kb.split('\t')[-1]
            return int(vmsize) / 1024
        except IndexError:
            return 0

    def assertItemsEqualTypestrict(self, results, expected):
        msg = "Results: {0}{2}Expected Results: {1}{2}".format(expected, results, "\n")
        self.assertEqual(len(results), len(expected), msg)
        results = sorted(results, cmp=sorting_key)
        expected = sorted(results, cmp=sorting_key)
        for x in xrange(len(results)):
            self.assertEqual({}, symmetric_recursive_comparison_typestrict(results[x], expected[x])[0], msg)

    def get_sonar_home(self):
        try:
            return os.environ["SONAR_HOME"]
        except:
            self.fail("The SONAR_HOME environment variable must be set.")


def get_sonar_pid():
    try:
        sonar_home = os.environ["SONAR_HOME"]
    except:
        sys.exit("The SONAR_HOME environment variable must be set.")
    pid_file = sonar_home + "/sonard.pid"
    if os.access(pid_file, os.R_OK):
        with open(pid_file, 'r') as p:
            return p.read().splitlines()[0]


def reset_error_message(self):
    self.db_name = self.coll_name = self.query = None