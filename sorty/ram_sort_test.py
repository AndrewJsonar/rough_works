#!/usr/bin/python
# -*- coding: utf-8 -*-
# Brian Laird Nov 18 2015
from __init__ import *
import time
import pprint
import pymongo
from common import clear_system_cache
from collections import OrderedDict

showQuery = 0
showResult = 0
exitReceived = 0
cmdDebug = 0
debug = 0
gFailures = 0
import signal
import sys
from subprocess import call
from math import sqrt
import os.path
import argparse
import md5
import re
import _sre
from bson import binary
import base64
import uuid
import json
import inspect
from debug_print import *

# import json
# import simplejson as json
# from json_tricks import dumps
dist_error_re = re.compile(
    "Error preparing distributed aggregation|No replica set members match selector|No primary available for writes|.*Connection refused|Not replica set master|Error executing first step of distributed aggregation|connection closed|Cannot create database")

result_durations = {}


def iterationNumberToString(iterations):
    if (iterations == 10000):
        return "10k";
    if (iterations == 100000):
        return "100k";
    if (iterations == 1000000):
        return "1m";
    if (iterations == 10000000):
        return "10m";
    if (iterations == 50000000):
        return "50m";
    if (iterations == 100000000):
        return "100m";
    if (iterations == 500000000):
        return "500m";
    return str(iterations);


def parseIterations(iterationString):
    if iterationString == "10k":
        return 10000;
    if iterationString == "100k":
        return 100000;
    if iterationString == "1m":
        return 1000000;
    if iterationString == "10m":
        return 10000000;
    if iterationString == "50m":
        return 50000000;
    if iterationString == "100m":
        return 100000000;
    if iterationString == "500m":
        return 500000000;
    return int(iterationString);


parser = argparse.ArgumentParser()
parser.add_argument('iterations', type=str)
parser.add_argument('-start', type=int, default=0)
parser.add_argument('-end', type=int, default=999999999)
parser.add_argument('-port', type=int, default=27117)
parser.add_argument('-test', type=str, default="")
parser.add_argument('-queries', type=int, default=0)
parser.add_argument('-fulldiff', type=int, default=0)
parser.add_argument('-loops', type=int, default=1)
parser.add_argument('-wait', type=int, default=0)
parser.add_argument('-debug', type=int, default=0)
parser.add_argument('-print_debug_level', type=int, default=0)
parser.add_argument('-log_debug_filename', type=str, default="/tmp/ram_sort_test_log.txt")

parser.add_argument('-clearCache', type=int, default=0)

parser.add_argument('-output', type=str, default="/local/raid0/sonar")
parser.add_argument('-virtualMemoryPrintout', type=int, default=0)
parser.add_argument('-baselineDirectory', type=str, default="/local/raid0/baselines")
parser.add_argument('-updateCalibrationData', type=int, default=0)
parser.add_argument('-reCalibrate', type=int, default=0)
parser.add_argument('--user', '-u', dest="user", type=str, default='kojima')
parser.add_argument('--pass', dest="password", type=str, default='hideo')
parser.add_argument('--unauth', type=int, default=0)
parser.add_argument('--removeFiles', type=int, default=1)
parser.add_argument('--removeSuccessfulResults', type=int, default=1)
parser.add_argument('--distributed', type=int, default=0)
parser.add_argument('--skipNonDist', type=int, default=0)
parser.add_argument('--createFile', type=int, default=0)
parser.add_argument('--expectedWorkers', type=int, default=3)
parser.add_argument('-host1', type=str, default="")
parser.add_argument('-host2', type=str, default="")
parser.add_argument('-host3', type=str, default="")
parser.add_argument('-replicaSet', type=str, default="rs0")
parser.add_argument('-sonarhome', type=str, default="/var/lib/sonarw")
args = parser.parse_args()
debug = args.debug
configure_debug(args)
tests = args.test.split(',')


def get_results(res):
    if type(res) is dict:
        results = res['result']
    else:
        results = [doc for doc in res]
    return results


def make_comment(testNo):
    test_instance_id = random.randint(1, 2 ** 63)
    comment_label = "track_dist_%d_%s" % (testNo, str(test_instance_id))
    return comment_label


def updateFailures():
    global gFailures
    gFailures += 1


global_iterations = parseIterations(args.iterations);
global_iterations_str = iterationNumberToString(global_iterations)
if args.updateCalibrationData:
    print
    "Updating Calibration data in ", args.baselineDirectory


def signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    exitReceived = 1
    sys.exit(0)


recalibrationSamples = 20
if global_iterations >= 10000000:
    recalibrationSamples = 2
deb("iterations: %d" % global_iterations);
signal.signal(signal.SIGINT, signal_handler)


def get_time_from_line(line):
    time_index = line.index(' ms')
    line = line[:time_index][::-1]
    time_index = line.index(' ')
    line = line[:time_index][::-1]
    return int(line)


def get_rate_from_line(line):
    temp = re.split("[ ]+", line)
    return float(temp[4])


def mean(X):
    return float(sum(X)) / len(X)


def var(X):
    return mean(map(lambda x: x ** 2, X)) - mean(X) ** 2


def stdev(X):
    return sqrt(var(X))


def getTimes(f):
    times = map(get_rate_from_line, f.readlines())
    return times


def enabled(t):
    filename = "%s/%d/calibration/history.%d.waive" % (args.baselineDirectory, global_iterations, t)
    return not os.path.isfile(filename)


def stats(t):
    filename = "%s/%d/calibration/history.%d.stat" % (args.baselineDirectory, global_iterations, t)
    with open(filename) as calibration_file:
        times = getTimes(calibration_file)
        if len(times):
            avg = mean(times)
            standard_dev = stdev(times)
            tolerance = 3 * standard_dev
            hilimit = avg + tolerance
            lolimit = avg - tolerance
            percentTolerance = (tolerance * 100) / avg
            if percentTolerance < 5:
                tolerance = avg / 20
                hilimit = avg + tolerance
                lolimit = avg - tolerance
                percentTolerance = 5
            return (1, avg, hilimit, lolimit, tolerance, percentTolerance)
        return (0, 0, 0, 0, 0, 0)


def rates(t):
    filename = "%s/%d/calibration/history.%d.stat" % (args.baselineDirectory, global_iterations, t)
    with open(filename) as calibration_file:
        times = getTimes(calibration_file)
        output_str = ""
        for x in times:
            output_str = "%s %8.2f" % (output_str, x)
        return output_str


def check_performance_results(test, new_time, rate, res):
    passed = True
    filename = "%s/%d/calibration/history.%d.stat" % (args.baselineDirectory, global_iterations, test)
    if 0 == args.clearCache:
        output_str = res
    elif not enabled(test):
        output_str = "WAIVE"
    elif os.path.isfile(filename):
        (ok, avg, hilimit, lolimit, tolerance, percentTolerance) = stats(test)
        if ok:
            if rate < lolimit:
                status = 'FAILED2'
                passed = False
            elif rate > hilimit:
                deb("Recalibrate: rate %8.2f hilimit: %8.2f lowlimit: %8.2f" % (rate, hilimit, lolimit))
                status = 'RECALIBRATE'
                passed = True
            else:
                status = res
            percent = (rate * 100) / avg
            output_str = "%3.0f%% %8.2f +-%3.0f%%  %s " % (percent, avg, percentTolerance, status)
            if passed:
                output_str = "%s %s" % (output_str, rates(test))
    else:
        output_str = "OK (no perf)"
    return (output_str, passed)


def baselineExistsHelper(testNumber, suffix):
    filename = "Out.%d.%s" % (testNumber, suffix)
    fileTemplate = "%s/%d/%s" % (args.baselineDirectory, global_iterations, filename)
    md5FileTemplate = "%s.md5" % fileTemplate
    if not os.path.exists(os.path.expanduser(fileTemplate)) and not os.path.exists(os.path.expanduser(md5FileTemplate)):
        return false;
    return true;


def baselineExists(testNumber):
    ret = baselineExistsHelper(testNumber, "json") or baselineExistsHelper(testNumber, "csv");
    deb("baselineExists: %d" % ret)
    return ret


def logResult(now, testNumber, millis, rate, distMillis):
    if args.updateCalibrationData or args.reCalibrate:
        deb("%s %s %d %d %8.12f" % ("logResult", now, testNumber, millis, rate))
        filename = "history.%d.stat" % (testNumber)
        fileTemplate = "%s/%d/calibration/%s" % (args.baselineDirectory, global_iterations, filename)
        with open(fileTemplate, "a") as myfile:
            myfile.write("%s %d ms %f\n" % (now, millis, rate))


def cmd(ln):
    deb(ln)
    with os.popen(ln) as f:
        output = ""
        for line in f.readlines():
            output = output + line
        if cmdDebug:
            print
            ln
            for line in f.readlines():
                print
                line
        return output


def convertToLong(val):
    return '{"$numberLong":"%d"}' % val


def convertToRegex(val):
    return '{"$regex":"%s","$options":""}' % val


def convertToBinary(val):
    return '{"$binary":"%s","$type":%d}' % (base64.b64encode(val), val.subtype)


def convertToBool(val):
    if (val):
        return "true"
    return "false"


# xxx
class ObjectEncoder(json.JSONEncoder):
    def default(self, obj):
        print("ObjectEncoder: %s" % obj.dir())
        if (isinstance(property, binary.Binary)):
            deb("GOT BINARY")
        if hasattr(obj, "to_json"):
            return self.default(obj.to_json())
        return self.default(d)


def convertArrayToString(obj):
    deb2("convertArrayToString:" + str(obj))
    output = "[";
    first = true
    binary_re = re.compile('Binary."(.*)", *([0-9]*)')
    uuid_re = re.compile('UUID\(')
    for property in obj:
        if not first:
            output += ","
        first = false
        if (isinstance(property, re._pattern_type)):
            repat = property.pattern
            repat = convertToRegex(repat)
            output += repat
        elif (isinstance(property, list)):
            output += convertArrayToString(property)
        elif (isinstance(property, dict)):
            output += convertToString(property)
        elif (isinstance(property, long)):
            output += convertToLong(obj[property])
        elif (isinstance(property, bool)):
            output += convertToBool(obj[property])
        else:
            try:
                property_as_str = str(property)
            except UnicodeEncodeError:
                property_as_str = property.encode("utf8")
            temp = '"' + property_as_str + '"'
            temp = temp.replace("'", '"')
            index = temp.find("ObjectId(")
            if -1 != index:
                temp = '{"$oid":"%s"}' % str(obj[property])
                match = re.search(binary_re, temp)
                if not match:
                    match = re.search(uuid_re, temp)
                    bin_type = 3
                    if match:
                        val64 = base64.b64encode(obj[prop].bytes)
                else:
                    bin_type = match.group(2)
                    val64 = base64.b64encode(obj[prop])
                if match:
                    deb2("convertToString  b64: %s" % (val64))
                    temp = '{"$binary":"%s","$type":%s}' % (val64, bin_type)
            output += temp
    output += "]"
    output = output.replace('u"', '"')
    return output


def convertToString(obj):
    output = "{";
    first = true
    binary_re = re.compile('Binary."(.*)", *([0-9]*)')
    uuid_re = re.compile('UUID\(')
    for prop in obj:
        if not first:
            output += ",";
        first = false
        if (isinstance(obj[prop], re._pattern_type)):
            repat = obj[prop].pattern
            repat = convertToRegex(repat)
            output += '"' + prop + '" :' + repat
        elif (isinstance(obj[prop], list)):
            output += '"' + prop + '":'
            temp = convertArrayToString(obj[prop])
            temp = temp.decode("utf8")
            output += temp
        elif (isinstance(prop, dict)):
            output += '"' + prop + '"' + convertToString(obj[prop])
        elif (isinstance(obj[prop], long)):
            output += '"' + prop + '":' + convertToLong(obj[prop])
        elif (isinstance(obj[prop], bool)):
            output += '"' + prop + '":' + convertToBool(obj[prop])
        else:
            deb2("convertToString %s %s" % (type(prop), prop))
            temp = str({prop: obj[prop]})
            temp = temp.replace("'", '"')
            deb2(temp)
            index = temp.find("ObjectId(")
            if -1 != index:
                temp = '"' + prop + '":' + '{"$oid":"%s"}' % str(obj[prop])
                deb2(temp)
                output += temp
            else:
                match = re.search(binary_re, temp)
                if not match:
                    match = re.search(uuid_re, temp)
                    bin_type = 3
                    if match:
                        val64 = base64.b64encode(obj[prop].bytes)
                else:
                    bin_type = match.group(2)
                    val64 = base64.b64encode(obj[prop])
                if match:
                    deb2("convertToString  b64: %s" % (val64))
                    temp = '"' + prop + '":' + '{"$binary":"%s","$type":%s}' % (val64, bin_type)
                    output += temp
                else:
                    output += temp[1:-1]
        deb2("convertToString: %s %s" % (type(prop), output))
    output += "}"
    output = output.replace('u"', '"')
    return output


def iterationString(num):
    if (num == 10000):
        return "10K";
    if (num == 100000):
        return "100K";
    if (num == 1000000):
        return "1M";
    return "";


if (len(tests) == 1):
    if tests[0] == '':
        tests = []
deb("tests: %s" % tests)
diffstr = "/home/qa/git/qa/QANG/sort_load_test/dwc %s %s"
if args.fulldiff:
    diffstr = "diff %s %s"

if args.distributed:
    allHosts = "%s" % (args.host1)
    if len(args.host2):
        allHosts = "%s,%s" % (allHosts, args.host2)
    if len(args.host3):
        allHosts = "%s,%s" % (allHosts, args.host3)
    uri = 'mongodb://' + args.user + ':' + args.password + "@%s/admin?replicaSet=%s" % (allHosts, args.replicaSet)
else:
    uri = 'mongodb://' + args.user + ':' + args.password + '@localhost' + ':' + str(args.port) + '/?authSource=admin'

if (args.unauth):
    uri = 'mongodb://' + 'localhost' + ':' + str(args.port) + '/?authSource=admin'
deb("Connecting to: %s" % uri)

if args.removeFiles:
    cmd("rm -f %s/agg_out/*" % args.sonarhome)

# def clear_sonar_cache

iterations = global_iterations
upperlimit = iterations

loops = args.loops
projection = {'$project': {"Product.ShortDescription": 1, "Product.Country": 1, "Product.MarketSectorDescription": 1,
                           "Product.IsEquityLinked": 1, "Product.SecurityTypeLevel1": 1, "Product.Description": 1,
                           "cloudTimestamp": 1, "_id": 1}}
hiddenCitiProjection0 = {'$project': {"Product": 1, "IncomeInformation": 1, "cloudTimestamp": 1, "_id": 1}}
hiddenCitiProjection = {'$project': {"Product.MarketSectorDescription": 1, "Product.Country": 1,
                                     "smcp_counter": {'$sum': ["$IncomeInformation.CouponDividendFrequency", 100]},
                                     "cloudTimestamp": 1, "_id": 1}}
hiddenCitiProjection2 = {'$project': {"Product.ShortDescription": 1, "Product.Country": 1,
                                      "smcp_counter": {'$sum': ["$IncomeInformation.CouponDividendFrequency", 100]},
                                      "IncomeInformation.CouponDividendFrequency": 1, "cloudTimestamp": 1, "_id": 1}}
projectCiti1A = {'$project': {"Product.MarketSectorDescription": 1}}
projectCiti4 = {'$project': {"cloudTimestamp": 1, "_id": 1, "Product.Country": 1, "Product.MarketSectorDescription": 1,
                             "Product.IsEquityLinked": 1}}
projectCiti3 = {'$project': {"Product.ShortDescription": 1, "Product.Country": 1, "cloudTimestamp": 1}}
projectCiti1 = {'$project': {"cloudTimestamp": 1}}
projectCiti5 = {'$project': {"cloudTimestamp": 1, "_id": 0}}
projectCiti2 = {'$project': {"Product.ShortDescription": 1, "cloudTimestamp": 1}}
projectCiti3 = {'$project': {"Product.MarketSectorDescription": 1, "cloudTimestamp": 1, "_id": 1}}
project_all = {'$project': {'*': 1}}
project_a = {'$project': {'_id': 0, 'a': 1}}
project_i_a = {'$project': {'_id': 0, 'i': 1, 'a': 1}}
project_a_b = {'$project': {'_id': 0, 'a': 1, 'b': 1}}
project_g_d = {'$project': {'_id': 0, 'g': 1, 'd': 1}}
project_a_b_c = {'$project': {'_id': 0, 'a': 1, 'b': 1, 'c': 1}}
project_c = {'$project': {'_id': 0, 'c': 1}}
project_d = {'$project': {'_id': 0, 'd': 1}}
project_g = {'$project': {'_id': 0, 'g': 1}}
proj_full = {'$project': {'_id': 0, 'Full Sql': 1}}
proj_full_id = {'$project': {'_id': 1, 'Full Sql': 1}}
proj_sid = {'$project': {'_id': 0, 'Session Id': 1}}
project_id_i = {'$project': {'_id': 1, 'i': 1}}
sortOrder43 = OrderedDict([('a', 1), ('g', -1), ('h', -1), ('_id', 1)])
sortOrder55 = OrderedDict([('g', 1), ('h', 1), ('d', 1), ('_id', 1)])
sortOrder60 = OrderedDict([('$maxFieldSize', 2048), ('b', 1), ('a', 1)])
sortOrder61 = OrderedDict([('$maxFieldSize', 2048), ('a', 1), ('g', -1)])
sortOrder62 = OrderedDict([('$maxFieldSize', 2048), ('b', 1), ('i', -1)])
sortOrder63 = OrderedDict([('g', 1), ('h', 1)])
sortOrder70 = OrderedDict([('g', 1), ('d', 1)])
sortOrder71 = OrderedDict([('g', 1), ('d', -1), ('_id', 1)])
sortOrder72 = OrderedDict([('g', 1), ('d', -1), ('h', -1), ('_id', 1)])
sortOrder73 = OrderedDict([('$maxFieldSize', 16), ('g', 1), ('b', -1), ('c', -1), ('d', 1), ('i', 1), ('a', 1)])
sortOrder80 = OrderedDict([('a', 1), ('g', -1), ('_id', 1)])
sortOrder160 = {'$sort': OrderedDict([('a', 1), ('i', 1)])}
sortOrder161 = {'$sort': OrderedDict([('a', -1), ('i', -1)])}
sortOrder200 = OrderedDict([('Product.MarketSectorDescription', 1), ('cloudTimestamp', 1), ('_id', 1)])
sortOrder210 = OrderedDict([('Product.MarketSectorDescription', 1), ('cloudTimestamp', 1), ('_id', 1)])
sortOrderCiti4 = OrderedDict(
    [('Product.MarketSectorDescription', 1), ('cloudTimestamp', 1), ('Product.ShortDescription', 1), ('_id', 1)])
sortOrderCiti5 = OrderedDict(
    [('Product.MarketSectorDescription', 1), ('cloudTimestamp', 1), ('Product.IssueStatus', 1), ('Product.Country', 1),
     ('_id', 1)])
sortOrderCiti6 = OrderedDict(
    [('Product.MarketSectorDescription', 1), ('cloudTimestamp', 1), ('Product.SecurityTypeLevel1', 1),
     ('Product.CFICode', 1), ('ExtXref.JJKO', 1), ('_id', 1)])
sortOrder300 = OrderedDict([('Full Sql', 1)])
sortOrder310 = OrderedDict([('Response Time', 1), ('Timestamp', -1), ('_id', 1)])
sortOrder320 = OrderedDict(
    [('Response Time', 1), ('Timestamp', -1), ('Instance Id', 1), ("Records Affected (Desc)", -1),
     ("Statement Type", 1), ("Access Rule Description", 1), ("_id", 1)])
sortOrder320A = OrderedDict(
    [("_id", 1), ('Response Time', 1), ('Timestamp', -1), ('Instance Id', 1), ("Records Affected (Desc)", -1),
     ("Statement Type", 1), ("Access Rule Description", 1)])
sortOrder330 = OrderedDict([('Session Id', 1)])
sortOrderSessIdId = OrderedDict([('Session Id', 1), ('_id', 1)])
match340 = {
    "$match": {
        "$or": [
            {
                "Response Time": 48
            },
            {
                "Response Time": 20
            }
        ]
    }
}
sortOrder340 = {
    "$sort": {
        "Session Id": 1
    }
}
sortOrder340A = OrderedDict([('Session Id', 1), ('_id', 1)])

project_lots = {'$project': {'IssueUnderwriter': 1, "FixedIncome": 1, "LegacySupport": 1, "XrefProperties": 1,
                             "IncomeInformation": 1, "Xref": 1, "CovenantDefaultInfo": 1, "ExtXref": 1, "id": 1,
                             "TraceHistory": 1, "Product": 1, "cloudTimestamp": 1, "SecureInformation": 1,
                             "SecureFiInformation": 1, "IndustryClass": 1, "IssueClassification": 1, "Redemption": 1,
                             "Redemption": 1, "ResetPreviousDateHistory": 1, "PriceXref": 1, "MsdInformation": 1,
                             "ExchangeListingInfo": 1, "MsdFiInformation": 1, "LegacyClearing": 1, "Settlement": 1,
                             "ConversionBasic": 1}}
matchCiti = {'$match': {"cloudTimestamp": {"$gte": datetime.datetime(2014, 01, 01, 14, 57, 32, 0),
                                           "$lte": datetime.datetime(2014, 02, 14, 20, 57, 10, 0)}}}
matchCiti2 = {'$match': {"cloudTimestamp": {"$gte": datetime.datetime(2014, 9, 04, 14, 57, 32, 0),
                                            "$lte": datetime.datetime(2015, 01, 14, 20, 57, 10, 0)},
                         'Product.MarketSectorDescription': {'$gte': 'O'}}}

col1 = {'locale': 'en_US', 'numericOrdering': true}
col2 = {'locale': 'fr_CA', 'numericOrdering': false}
col3 = {'locale': 'fa_AF', 'strength': 1}
col4 = {'locale': 'en_US', 'strength': 2}
col5 = {'locale': 'en_US', 'strength': 3, 'caseLevel': true}
col6 = {'locale': 'en_US', 'strength': 3}
col7 = {'locale': 'en_US', 'strength': 4}
col8 = {'locale': 'en_US', 'strength': 5}
col9 = {'locale': 'en_US', 'caseLevel': true}
col10 = {'locale': 'en_US', 'caseLevel': false}
col11 = {'locale': 'en_US', 'backwards': true}
col12 = {'locale': 'en_US', 'strength': 2, 'alternate': 'shifted'}
col12 = {'locale': 'en_US', 'strength': 2, 'maxVariable': 'space'}

arrayReduce = {
    '$project': {
        'items': {
            '$arrayReduce': {
                'input': "$b",
                'as': "item",
                'into': "s",
                'cond': {'$sum': ["$$s", "$$item"]},
                'init': {'$literal': 0},
                'finish': {'$mul': ["$$s", '$i']},
            }
        }
    }
}
ONLY12 = 1.2
ONLY14 = 1.4
ONLY20 = 2.0
NOD = 99  # no distributed
data = [

    [10, 1, 'DISTR-A Straight $out (NBL)       ', 'samDist', 0, [{'$limit': upperlimit}, {'$out': 'osamDist'}]],
    [11, 1, 'DISTR-A $project $out (NBL)       ', 'samDist', 0,
     [{'$limit': upperlimit}, {'$project': {'*': 1}}, {'$out': 'osamDist'}]],
    [12, 1, 'Sorting single col, proj 2 col(NBL)', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, project_a_b,
      {'$out': {"fstype": "local", "format": "json", "filename": "TempOut.12.json"}}]],
    [13, 1, 'Sorting single col, proj 1 col(NBL)', 'samstr', 0, [{'$limit': upperlimit}, {'$sort': {'a': 1}}, project_a,
                                                                 {'$out': {"fstype": "local", "format": "json",
                                                                           "filename": "TempOut.13.json"}}]],
    [14, 1, 'Sorting single col, proj 3 col(NBL)', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, project_a_b_c,
      {'$out': {"fstype": "local", "format": "json", "filename": "TempOut.14.json"}}]],

    [20, 1, 'Sorting Strings limit 1k          ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, {'$limit': 1000}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.20.json"}}]],
    [21, 1, 'Sorting Strings limit 10k         ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, {'$limit': 10000}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.21.json"}}]],
    [22, 1, 'Sorting Strings limit 100k        ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, {'$limit': 100000}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.22.json"}}]],
    [23, 1, 'Sorting Strings limit 1m          ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, {'$limit': 1000000}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.23.json"}}]],
    [24, 1, 'Sorting Strings limit 10m         ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, {'$limit': 10000000}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.24.json"}}]],
    [25, 1, 'Sorting Strings limit 10m project ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$project': {'a': 1}}, {'$sort': {'a': 1}}, {'$limit': 10000000}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.25.json"}}]],

    [30, 1, 'Sorting Numbers limit 1           ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': {'c': 1}}, {'$limit': 1}, project_c,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.30.json"}}]],
    [31, 1, 'Sorting Numbers limit 10          ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': {'c': 1}}, {'$limit': 10}, project_c,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.31.json"}}]],
    [32, 1, 'Sorting Numbers limit 100         ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': {'c': 1}}, {'$limit': 100}, project_c,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.32.json"}}]],
    [33, 1, 'Sorting Numbers limit 1k          ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': {'c': 1}}, {'$limit': 1000}, project_c,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.33.json"}}]],
    [34, 1, 'Sorting Numbers limit 10k         ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': {'c': 1}}, {'$limit': 10000}, project_c,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.34.json"}}]],
    [35, 1, 'Sorting Numbers limit 100k        ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': {'c': 1}}, {'$limit': 100000}, project_c,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.35.json"}}]],
    [36, 1, 'Sorting Numbers limit 1m          ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': {'c': 1}}, {'$limit': 1000000}, project_c,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.36.json"}}]],
    [37, 1, 'Sorting Numbers limit 10m         ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': {'i': 1}}, {'$limit': 10000000}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.37.json"}}]],
    [38, 1, 'Sorting Integers small range      ', 'samstr', 0, [{'$limit': upperlimit}, {'$sort': {'g': 1}}, project_g,
                                                                {'$out': {"fstype": "local", "format": "json",
                                                                          "filename": "Out.38.json"}}]],
    [39, 1, 'Sorting Integers small range      ', 'samstr', 0, [{'$limit': upperlimit}, {'$sort': {'g': 1}}, project_g,
                                                                {'$out': {"fstype": "local", "format": "json",
                                                                          "filename": "Out.39.json"}}]],

    [40, 1, 'Sorting Strings                   ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, project_a, {'$group': {'_id': '1', 'count': {'$sum': 1}}}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.40.json"}}]],
    [41, 1, 'Sorting Strings with Undef        ', 'samu', 0, [{'$limit': upperlimit}, {'$sort': {'a': 1}}, project_a, {
        '$out': {"fstype": "local", "format": "json", "filename": "Out.41.json"}}]],
    [42, 1, 'Sorting Strings with null         ', 'samn', 0, [{'$limit': upperlimit}, {'$sort': {'a': 1}}, project_a, {
        '$out': {"fstype": "local", "format": "json", "filename": "Out.42.json"}}]],
    [43, 1, 'Sorting Strings groupXXX          ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder43}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.43.json"}}]],
    [44, 1, 'Sorting Large Strings             ', 'samstr', 0, [{'$limit': upperlimit}, {'$sort': {'d': 1}}, project_d,
                                                                {'$out': {"fstype": "local", "format": "json",
                                                                          "filename": "Out.44.json"}}]],
    [45, 1, 'Sorting Large Strings desc        ', 'samstr', 0, [{'$limit': upperlimit}, {'$sort': {'d': -1}}, project_d,
                                                                {'$out': {"fstype": "local", "format": "json",
                                                                          "filename": "Out.45.json"}}]],
    [46, 1, 'Sorting Strings $out json         ', 'samstr', 0, [{'$limit': upperlimit}, {'$sort': {'a': 1}}, project_a,
                                                                {'$out': {"fstype": "local", "format": "json",
                                                                          "filename": "Out.46.json"}}]],
    [47, 1, 'Sorting Strings $out csv          ', 'samstr', 0, [{'$limit': upperlimit}, {'$sort': {'a': 1}}, project_a,
                                                                {'$out': {"fstype": "local", "format": "csv",
                                                                          "filename": "Out.47.csv"}}]],
    [48, 1, 'Sorting Strings selected none     ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$match': {'b': {'$lt': -5}}}, {'$sort': {'a': 1}}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.48.json"}}]],
    [49, 1, 'Sorting Strings selected few      ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$match': {'b': {'$lt': 5}}}, {'$sort': {'a': 1}}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.49.json"}}]],
    [50, 1, 'Sorting Strings selected many     ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$match': {'b': {'$gt': 5}}}, {'$sort': {'a': 1}}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.50.json"}}]],
    [51, 1, 'Sorting Strings selected all(NBL) ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$match': {'b': {'$gte': 0}}}, {'$sort': {'a': 1}}, project_a,
      {'$group': {'_id': '1', 'count': {'$sum': 1}}}]],
    [52, 1, 'Sorting Strings selected half     ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$match': {'g': {'$gt': 50}}}, {'$sort': {'a': 1}}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.52.json"}}]],
    [53, 1, 'Sorting Strings XL                ', 'samSZ', 0,
     [{'$limit': upperlimit}, {'$sort': {'$maxFieldSize': 2048, 'a': 1}}, project_a,
      {'$out': {"fstype": "local", "format": "csv", "filename": "Out.53.csv"}}]],
    [54, 1, 'Sorting Strings XL  desc          ', 'samSZ', 0,
     [{'$limit': upperlimit}, {'$sort': {'$maxFieldSize': 2048, 'a': -1}}, project_a,
      {'$out': {"fstype": "local", "format": "csv", "filename": "Out.54.csv"}}]],
    [55, 1, 'Sorting Strings dist              ', 'samDist', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder55}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.55.json"}}]],

    [60, 1, 'XXXX StringsXL 2 columns project  ', 'samSZ', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder60}, {'$project': {'_id': 0, 'b': 1, 'a': 1}},
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.60.json"}}]],
    [61, 1, 'XXXX Strings 2 columns project    ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder61}, {'$project': {'_id': 0, 'a': 1, 'g': 1}},
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.61.json"}}]],
    [62, 1, 'XXXX StringsXL 2 columns          ', 'samSZ', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder62}, {'$project': {'_id': 0, 'b': 1, 'i': 1}},
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.62.json"}}]],
    [63, 1, 'XXXX Strings 2 columns            ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder63}, {'$project': {'_id': 0, 'g': 1, 'h': 1}},
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.63.json"}}]],

    [70, 1, 'Sorting 2 columns                 ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder70}, project_g_d,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.70.json"}}]],
    [71, 1, 'Sorting 3 columns                 ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder71}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.71.json"}}]],
    [72, 1, 'Sorting 4 columns                 ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder72}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.72.json"}}]],
    [73, 1, 'Sorting 6 columns                 ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder73}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.73.json"}}]],

    [90, 1, 'Sorting bool                      ', 'samb', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, {'$unwind': '$a'}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.90.json"}}]],
    [91, 1, 'Sorting bool  desc                ', 'samb', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': -1}}, {'$unwind': '$a'}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.91.json"}}]],
    [92, 1, 'Sorting Objects                   ', 'samo', ONLY14,
     [{'$limit': upperlimit}, {'$sort': {'e': 1}}, {'$project': {'_id': 0, 'e': 1}}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.92.json"}}]],
    [93, 1, 'Sorting Mixed Types               ', 'samm', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, {'$project': {'_id': 0, 'a': 1}}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.93.json"}}]],
    [94, 1, 'Sorting Integers                  ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': {'b': 1}}, {'$project': {'_id': 0, 'b': 1}}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.94.json"}}]],
    [95, 1, 'Sorting Integers desc             ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': {'b': -1}}, {'$project': {'_id': 0, 'b': 1}}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.95.json"}}]],
    [96, 1, 'Sorting Integers small group      ', 'samstr', 0,
     [{'$limit': upperlimit}, {'$sort': {'g': 1}}, {'$project': {'_id': 0, 'g': 1}}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.96.json"}}]],
    [97, 1, 'Sorting Regex                     ', 'samrex', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, {'$project': {'_id': 0, 'a': 1}}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.97.json"}}]],
    [98, 1, 'Sorting BinData                   ', 'sambin', 0, [{'$limit': upperlimit}, {'$sort': {'a': 1}}, project_a,
                                                                {'$out': {"fstype": "local", "format": "json",
                                                                          "filename": "Out.98.json"}}]],
    [99, 1, 'Sorting Timestamps                ', 'samtz', 0, [{'$limit': upperlimit}, {'$sort': {'a': 1}}, project_a, {
        '$out': {"fstype": "local", "format": "json", "filename": "Out.99.json"}}]],

    # cores 1.2
    [110, 1, 'Sorting Integers/unwind           ', 'samasz', ONLY14,
     [{"$optimizer": 0}, {'$limit': upperlimit}, {'$sort': {'b': 1, '_id': 1}}, {'$unwind': '$a'}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.110.json"}}]],
    # cores 1.2
    [111, 1, 'Sorting Strings/unwind            ', 'samasz', ONLY14,
     [{'$limit': upperlimit}, {'$sort': {'d': 1}}, {'$unwind': '$a'}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.111.json"}}]],
    [112, 1, 'Sorting [String]                  ', 'samasz', 0, [{'$limit': upperlimit}, {'$sort': {'a': 1}}, project_a,
                                                                 {'$out': {"fstype": "local", "format": "json",
                                                                           "filename": "Out.112.json"}}]],
    [113, 1, 'Sorting [String] desc             ', 'samasz', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': -1}}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.113.json"}}]],
    [114, 1, 'Sorting [Regex]                   ', 'samarex', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.114.json"}}]],
    [115, 1, 'Sorting [Regex]  desc             ', 'samarex', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': -1}}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.115.json"}}]],
    [116, 1, 'Sorting [Timestamp]               ', 'samatz', 0, [{'$limit': upperlimit}, {'$sort': {'a': 1}}, project_a,
                                                                 {'$out': {"fstype": "local", "format": "json",
                                                                           "filename": "Out.116.json"}}]],
    [117, 1, 'Sorting [Object]                  ', 'samaob', 0, [{'$limit': upperlimit}, {'$sort': {'a': 1}}, project_a,
                                                                 {'$out': {"fstype": "local", "format": "json",
                                                                           "filename": "Out.117.json"}}]],
    [118, 1, 'Sorting [Object]  desc            ', 'samaob', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': -1}}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.118.json"}}]],
    [119, 1, 'Sorting Strings array of mix      ', 'samamix', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1, 'i': 1}}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.119.json"}}]],

    [120, 1, 'Sorting All limit 1               ', 'samAll', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, {'$limit': 1}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.120.json"}}]],
    [121, 1, 'Sorting All limit 10              ', 'samAll', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, {'$limit': 10}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.121.json"}}]],
    [122, 1, 'Sorting All limit 100             ', 'samAll', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, {'$limit': 100}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.122.json"}}]],
    [123, 1, 'Sorting All limit 1k              ', 'samAll', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, {'$limit': 1000}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.123.json"}}]],
    [124, 1, 'Sorting All limit 10k             ', 'samAll', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, {'$limit': 10000}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.124.json"}}]],
    [125, 1, 'Sorting All limit 100k            ', 'samAll', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, {'$limit': 100000}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.125.json"}}]],
    [126, 1, 'Sorting All limit 1M              ', 'samAll', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, {'$limit': 1000000}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.126.json"}}]],
    [127, 1, 'Sorting All limit 10M             ', 'samAll', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, {'$limit': 10000000}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.127.json"}}]],
    # Not deterministic[128, 1, 'unwind/Sort All                   ', 'samAll',  0,  [{'$limit': upperlimit},{'$unwind': '$a'},{'$sort': {'a': 1}},project_a,{'$out':{"fstype": "local", "format": "json", "filename": "Out.128.json"}}]],

    [130, 1, 'Temp whole bson unwind            ', 'samasz', ONLY12,
     [{'$limit': upperlimit}, {'$unwind': '$b'}, {'$sort': {'a': 1, 'b': 1}}, project_a_b,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.130.json"}}]],
    [131, 1, 'Temp whole bson group             ', 'samDist', 0,
     [{'$limit': upperlimit}, {'$group': {'i': {'$sum': "$c"}, '_id': {'$mod': ['$c', 10000000]}}},
      {'$sort': {'_id': 1, 'i': 1}}, project_id_i,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.131.json"}}]],
    [132, 1, 'Calculated project and *          ', 'samstr', ONLY14,
     [{'$limit': upperlimit}, {'$sort': {'_id': 1}}, {'$project': {'x': {'$mul': ['$g', 4]}, '*': 1}},
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.132.json"}}]],
    [133, 1, 'Trivial sort                      ', 'samstr', NOD,
     [{'$limit': upperlimit}, {'$sort': {'zzz': 1}}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.133.json"}}]],

    # Test columns not sorted
    # [140, 1, 'Sorting All limit 1               ', 'samstr',  0,  [{'$limit': upperlimit},{'$sort': {'i': 1}},{'$limit': 1},project_i_a,{'$out':{"fstype": "local", "format": "json", "filename": "Out.140.json"}}]],
    # [141, 1, 'Sorting All limit 10              ', 'samstr',  0,  [{'$limit': upperlimit},{'$sort': {'i': 1}},{'$limit': 10},project_i_a,{'$out':{"fstype": "local", "format": "json", "filename": "Out.141.json"}}]],
    # [142, 1, 'Sorting All limit 100             ', 'samstr',  0,  [{'$limit': upperlimit},{'$sort': {'i': 1}},{'$limit': 100},project_i_a,{'$out':{"fstype": "local", "format": "json", "filename": "Out.142.json"}}]],
    # [143, 1, 'Sorting All limit 1k              ', 'samstr',  0,  [{'$limit': upperlimit},{'$sort': {'i': 1}},{'$limit': 1000},project_i_a,{'$out':{"fstype": "local", "format": "json", "filename": "Out.143.json"}}]],
    # [144, 1, 'Sorting All limit 10k             ', 'samstr',  0,  [{'$limit': upperlimit},{'$sort': {'i': 1}},{'$limit': 10000},project_i_a,{'$out':{"fstype": "local", "format": "json", "filename": "Out.144.json"}}]],
    # [145, 1, 'Sorting All limit 100k            ', 'samstr',  0,  [{'$limit': upperlimit},{'$sort': {'i': 1}},{'$limit': 100000},project_i_a,{'$out':{"fstype": "local", "format": "json", "filename": "Out.145.json"}}]],
    # [146, 1, 'Sorting All limit 1M              ', 'samstr',  0,  [{'$limit': upperlimit},{'$sort': {'i': 1}},{'$limit': 1000000},project_i_a,{'$out':{"fstype": "local", "format": "json", "filename": "Out.146.json"}}]],
    # [147, 1, 'Sorting All limit 10M             ', 'samstr',  0,  [{'$limit': upperlimit},{'$sort': {'i': 1}},{'$limit': 10000000},project_i_a,{'$out':{"fstype": "local", "format": "json", "filename": "Out.147.json"}}]],

    # [150, 1, 'Sorting All limit 1               ', 'samstr',  0,  [{'$limit': upperlimit},{'$sort': {'i': 1,'a': 1}},{'$limit': 1},project_i_a,{'$out':{"fstype": "local", "format": "json", "filename": "Out.150.json"}}]],
    # [151, 1, 'Sorting All limit 10              ', 'samstr',  0,  [{'$limit': upperlimit},{'$sort': {'i': 1,'a': 1}},{'$limit': 10},project_i_a,{'$out':{"fstype": "local", "format": "json", "filename": "Out.151.json"}}]],
    # [152, 1, 'Sorting All limit 100             ', 'samstr',  0,  [{'$limit': upperlimit},{'$sort': {'i': 1,'a': 1}},{'$limit': 100},project_i_a,{'$out':{"fstype": "local", "format": "json", "filename": "Out.152.json"}}]],
    # [153, 1, 'Sorting All limit 1k              ', 'samstr',  0,  [{'$limit': upperlimit},{'$sort': {'i': 1,'a': 1}},{'$limit': 1000},project_i_a,{'$out':{"fstype": "local", "format": "json", "filename": "Out.153.json"}}]],
    # [154, 1, 'Sorting All limit 10k             ', 'samstr',  0,  [{'$limit': upperlimit},{'$sort': {'i': 1,'a': 1}},{'$limit': 10000},project_i_a,{'$out':{"fstype": "local", "format": "json", "filename": "Out.154.json"}}]],
    # [155, 1, 'Sorting All limit 100k            ', 'samstr',  0,  [{'$limit': upperlimit},{'$sort': {'i': 1,'a': 1}},{'$limit': 100000},project_i_a,{'$out':{"fstype": "local", "format": "json", "filename": "Out.155.json"}}]],
    # [156, 1, 'Sorting All limit 1M              ', 'samstr',  0,  [{'$limit': upperlimit},{'$sort': {'i': 1,'a': 1}},{'$limit': 1000000},project_i_a,{'$out':{"fstype": "local", "format": "json", "filename": "Out.156.json"}}]],
    # [157, 1, 'Sorting All limit 10M             ', 'samstr',  0,  [{'$limit': upperlimit},{'$sort': {'i': 1,'a': 1}},{'$limit': 10000000},project_i_a,{'$out':{"fstype": "local", "format": "json", "filename": "Out.157.json"}}]],

    [160, 1, 'Sorting Collation  1               ', 'samsz_collation', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.160.json"}}], col1],
    [161, 1, 'Sorting Collation  2               ', 'samsz_collation', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.161.json"}}], col2],
    [162, 1, 'Sorting Collation  3               ', 'samsz_collation', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1}}, project_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.162.json"}}], col3],
    [163, 1, 'Sorting Collation  4               ', 'samsz_collation', 0,
     [{'$limit': upperlimit}, sortOrder160, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.163.json"}}], col4],
    [164, 1, 'Sorting Collation  5               ', 'samsz_collation', 0,
     [{'$limit': upperlimit}, sortOrder161, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.164.json"}}], col5],
    [165, 1, 'Sorting Collation  6               ', 'samsz_collation', 0,
     [{'$limit': upperlimit}, sortOrder160, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.165.json"}}], col6],
    [166, 1, 'Sorting Collation  7               ', 'samsz_collation', 0,
     [{'$limit': upperlimit}, sortOrder161, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.166.json"}}], col7],
    [167, 1, 'Sorting Collation  8               ', 'samsz_collation', 0,
     [{'$limit': upperlimit}, sortOrder160, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.167.json"}}], col8],
    [168, 1, 'Sorting Collation  9               ', 'samsz_collation', 0,
     [{'$limit': upperlimit}, sortOrder161, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.168.json"}}], col9],
    [169, 1, 'Sorting Collation  10              ', 'samsz_collation', 0,
     [{'$limit': upperlimit}, sortOrder160, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.169.json"}}], col10],
    [170, 1, 'Sorting Collation  11              ', 'samsz_collation', 0,
     [{'$limit': upperlimit}, sortOrder161, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.170.json"}}], col11],
    [171, 1, 'Sorting Collation  12              ', 'samsz_collation', 0,
     [{'$limit': upperlimit}, sortOrder160, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.171.json"}}], col12],

    # Deprecated test
    # [180, 1, 'Sorting Calculation               ', 'samasz',  0,  [{'$limit': upperlimit},{'$sort': [({'$mod':['$i', 10]},-1),('_id',1)]},project_all,{'$out':{"fstype": "local", "format": "json", "filename": "Out.180.json" }}]],
    [181, 1, 'Sorting output of unwind          ', 'samaob', 0,
     [{'$limit': upperlimit}, {'$unwind': '$a'}, {'$sort': {'a.x': 1, 'a.y': 1}}, {'$project': {'_id': 0, 'a.x': 1}},
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.181.json"}}]],
    # [182, 1, 'Sorting large calculation         ', 'samaob',  0,  [{'$limit': upperlimit},{'$sort': {'resMod':{'$mod':['$b', 16]},'resDiv':{'$div':['$a', 16]}}},{'$project': {'_id':0, 'a':1}},{'$out':{"fstype": "local", "format": "json", "filename": "Out.182.json" }}]],

    [183, 1, 'Sorting output of project         ', 'samsz', 0,
     [{'$limit': upperlimit}, {'$project': {'a': 1, '_id': 0, 'b': 1}}, {'$sort': {'a': 1, 'b': 1}},
      {'$project': {'_id': 0, 'a': 1, 'b': 1}},
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.183.json"}}]],

    [184, 1, 'SNRD-4271                         ', 'samsz', 0,
     [{'$limit': upperlimit}, {'$match': {'a': {'$gte': 'a'}}}, {'$group': {'_id': '$b', 'count': {'$sum': 1}}},
      {'$match': {'count': {'$gte': 5}}}, {'$sort': {'count': 1, '_id': 1}}, {'$project': {'*': 1}},
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.184.json"}}], col11],
    [188, 1, 'Sorting Collation Array  1        ', 'samasz_collation', 0,
     [{'$limit': upperlimit}, sortOrder161, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.188.json"}}], col11],
    [189, 1, 'Sorting Collation Array  2        ', 'samasz_collation', 0,
     [{'$limit': upperlimit}, sortOrder160, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.189.json"}}], col12],
    [190, 1, 'Sorting Collation Array  3        ', 'samasz_collation', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1, '_id': 1}}, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.190.json"}}], col1],
    [191, 1, 'Sorting Collation Array  4        ', 'samasz_collation', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1, '_id': 1}}, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.191.json"}}], col2],
    [192, 1, 'Sorting Collation Array  5        ', 'samasz_collation', 0,
     [{'$limit': upperlimit}, {'$sort': {'a': 1, '_id': 1}}, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.192.json"}}], col3],
    [193, 1, 'Sorting Collation Array  6        ', 'samasz_collation', 0,
     [{'$limit': upperlimit}, sortOrder160, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.193.json"}}], col4],
    [194, 1, 'Sorting Collation Array  7        ', 'samasz_collation', 0,
     [{'$limit': upperlimit}, sortOrder161, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.194.json"}}], col5],
    [195, 1, 'Sorting Collation Array  8        ', 'samasz_collation', 0,
     [{'$limit': upperlimit}, sortOrder160, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.195.json"}}], col6],
    [196, 1, 'Sorting Collation Array  9        ', 'samasz_collation', 0,
     [{'$limit': upperlimit}, sortOrder161, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.196.json"}}], col7],
    [197, 1, 'Sorting Collation Array  10       ', 'samasz_collation', 0,
     [{'$limit': upperlimit}, sortOrder160, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.197.json"}}], col8],
    [198, 1, 'Sorting Collation Array  11       ', 'samasz_collation', 0,
     [{'$limit': upperlimit}, sortOrder161, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.198.json"}}], col9],
    [199, 1, 'Sorting Collation Array  12       ', 'samasz_collation', 0,
     [{'$limit': upperlimit}, sortOrder160, project_i_a,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.199.json"}}], col10],

    [200, 2, 'Citi 3 columns                    ', 'products', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder200}, projection,
      {'$out': {"fstype": "local", "format": "csv", "filename": "Out.200.csv"}}]],
    # [200, 2, 'Citi 3 columns                    ', 'products',0,  [{'$limit': upperlimit}, {'$sort': sortOrder200},projection,{'$out':{"fstype": "local", "format": "json", "filename": "Out.200.csv" }}]],
    [201, 2, 'Citi SNRD-2374  (NBL)             ', 'products', 0,
     [{'$limit': upperlimit}, {'$sort': {"Product.MarketSectorDescription": 1}}, projection,
      {'$out': {"fstype": "local", "format": "csv", "filename": "TempOut.201.csv"}}]],
    # takes 4 hours
    # [202, 2, 'Citi 1 large column               ', 'products',0,  [{'$limit': upperlimit}, {'$sort': {"Product.Description":1}},{'$project':{'_id':0,'Product.Description':1}},{'$out':{"fstype": "local", "format": "csv", "filename": "Out.202.csv" }}]],
    # SNRD-4199 [203, 2, 'Citi match 3 col                  ', 'products',0,  [matchCiti,{'$limit': upperlimit},{'$sort': sortOrder200},project_all,{'$out':{"fstype": "local", "format": "json", "filename": "Out.203.json"}}]],
    [204, 2, 'Citi match 1 col (NBL)            ', 'products', 0,
     [matchCiti, {'$limit': upperlimit}, {'$sort': {"Product.MarketSectorDescription": 1}}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "TempOut.204.json"}}]],
    [205, 2, 'Citi 3 col no limit               ', 'products', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder200}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.205.json"}}]],
    [206, 2, 'Citi match 3 col #2               ', 'products', NOD,
     [matchCiti2, {'$limit': upperlimit}, {'$sort': sortOrder200}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.206.json"}}]],
    # [206, 2, 'Citi match 3 col #2               ', 'products',0,  [ matchCiti2,{'$limit': upperlimit},{'$sort': sortOrder200},projectCiti3,{'$out':{"fstype": "local", "format": "json", "filename": "Out.206.json"}}]],

    [210, 2, 'Citi 3 columns limit 1            ', 'products', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder210}, {'$limit': 1}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.210.json"}}]],
    [211, 2, 'Citi 3 columns limit 10           ', 'products', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder210}, {'$limit': 10}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.211.json"}}]],
    [212, 2, 'Citi 3 columns limit 100          ', 'products', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder210}, {'$limit': 100}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.212.json"}}]],
    [213, 2, 'Citi 3 columns limit 1k           ', 'products', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder210}, {'$limit': 1000}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.213.json"}}]],
    [214, 2, 'Citi 3 columns limit 10k          ', 'products', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder210}, {'$limit': 10000}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.214.json"}}]],
    [215, 2, 'Citi 3 columns limit 100k         ', 'products', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder210}, {'$limit': 100000}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.215.json"}}]],
    [216, 2, 'Citi 3 columns limit 1m           ', 'products', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder210}, {'$limit': 1000000}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.216.json"}}]],
    [217, 2, 'Citi 3 columns limit 10m          ', 'products', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder210}, {'$limit': 10000000}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.217.json"}}]],

    [230, 2, 'Citi match 4 col                  ', 'products', NOD,
     [matchCiti2, {'$limit': upperlimit}, {'$sort': sortOrderCiti4}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.230.json"}}]],
    [231, 2, 'Citi match 5 col                  ', 'products', NOD,
     [matchCiti2, {'$limit': upperlimit}, {'$sort': sortOrderCiti5}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.231.json"}}]],
    [232, 2, 'Citi match 6 col                  ', 'products', NOD,
     [matchCiti2, {'$limit': upperlimit}, {'$sort': sortOrderCiti6}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.232.json"}}]],
    [233, 2, 'Citi SNRD-2685      (NBL)         ', 'products', 0,
     [{'$limit': upperlimit}, {'$sort': {"Product.MarketSectorDescription": 1}}, projection,
      {'$out': {"fstype": "local", "format": "json", "filename": "TempOut.233.json"}}]],
    [234, 2, 'Citi SNRD-2685                    ', 'products', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder210}, hiddenCitiProjection,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.234.json"}}]],
    [235, 2, 'Citi SNRD-2680                    ', 'products', ONLY14,
     [{'$limit': upperlimit}, hiddenCitiProjection0, {'$sort': sortOrder210}, hiddenCitiProjection,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.235.json"}}]],
    [236, 2, 'Citi SNRD-2680                    ', 'products', ONLY14,
     [{'$limit': upperlimit}, hiddenCitiProjection0, matchCiti, {'$sort': sortOrder210}, hiddenCitiProjection,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.236.json"}}]],

    [237, 2, 'Citi SNRD-2685                    ', 'products', 0,
     [{'$limit': upperlimit}, hiddenCitiProjection, {'$sort': sortOrder210}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.237.json"}}]],

    [240, 2, 'Citi sort 1 col, proj 1 col       ', 'products', ONLY14,
     [{'$limit': upperlimit}, {'$sort': {"cloudTimestamp": 1}}, projectCiti5,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.240.json"}}]],
    [241, 2, 'Citi sort 1 col, proj 2 col(NBL)  ', 'products', 0,
     [{'$limit': upperlimit}, {'$sort': {"cloudTimestamp": 1}}, projectCiti2,
      {'$out': {"fstype": "local", "format": "json", "filename": "Tout.241.json"}}]],
    [242, 2, 'Citi sort 1 col, proj 3 col(NBL)  ', 'products', 0,
     [{'$limit': upperlimit}, {'$sort': {"cloudTimestamp": 1}}, projectCiti3,
      {'$out': {"fstype": "local", "format": "json", "filename": "Tout.242.json"}}]],
    [243, 2, 'Citi sort 1 col, proj 4 col(NBL)  ', 'products', 0,
     [{'$limit': upperlimit}, {'$sort': {"cloudTimestamp": 1}}, projectCiti4,
      {'$out': {"fstype": "local", "format": "json", "filename": "Tout.243.json"}}]],
    [244, 2, 'Citi match sort 1 col, proj 1 col ', 'products', NOD,
     [matchCiti2, {'$limit': upperlimit}, {'$sort': {"cloudTimestamp": 1}}, projectCiti5,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.244.json"}}]],
    [245, 2, 'Citi match sort 1 col, proj3(NBL) ', 'products', 0,
     [matchCiti2, {'$limit': upperlimit}, {'$sort': {"cloudTimestamp": 1}}, projectCiti3,
      {'$out': {"fstype": "local", "format": "json", "filename": "Tout.245.json"}}]],
    [246, 2, 'Citi match sort 1 col, proj1(NBL) ', 'products', 0,
     [matchCiti2, {'$limit': upperlimit}, {'$sort': {"Product.MarketSectorDescription": 1}}, projectCiti1A,
      {'$out': {"fstype": "local", "format": "json", "filename": "Tout.246.json"}}]],
    [247, 2, 'Citi SNRD-2374(NBL)               ', 'products', 0,
     [matchCiti2, {'$limit': upperlimit}, {'$sort': {"Product.MarketSectorDescription": 1}}, projectCiti1A]],

    [250, 2, 'Citi skip sort 1 col, proj 1 col   ', 'products', ONLY14,
     [{'$skip': upperlimit}, {'$limit': upperlimit}, {'$sort': {"cloudTimestamp": 1}}, projectCiti5,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.250.json"}}]],
    [251, 2, 'Citi skip sort 1 col, proj 2(NBL)  ', 'products', 0,
     [{'$skip': upperlimit}, {'$limit': upperlimit}, {'$sort': {"cloudTimestamp": 1}}, projectCiti2,
      {'$out': {"fstype": "local", "format": "json", "filename": "Tout.251.json"}}]],
    [252, 2, 'Citi skip sort 1 col, proj3(NBL)   ', 'products', 0,
     [{'$skip': upperlimit}, {'$limit': upperlimit}, {'$sort': {"cloudTimestamp": 1}}, projectCiti3,
      {'$out': {"fstype": "local", "format": "json", "filename": "Tout.252.json"}}]],
    # [253, 2, 'Citi skip sort 3 col, proj *       ', 'products',0, [{'$skip': upperlimit},{'$limit': upperlimit}, {'$sort': sortOrder210},project_all,{'$out':{"fstype": "local", "format": "json", "filename": "Out.253.json"}}]],
    [254, 2, 'Citi skip match sort 1 col, proj 1 ', 'products', NOD,
     [{'$skip': upperlimit}, matchCiti2, {'$limit': upperlimit}, {'$sort': {"cloudTimestamp": 1}}, projectCiti1,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.254.json"}}]],
    [255, 2, 'Citi skip match sort 1 col, p3(NBL)', 'products', 0,
     [{'$skip': upperlimit}, matchCiti2, {'$limit': upperlimit}, {'$sort': {"cloudTimestamp": 1}}, projectCiti3,
      {'$out': {"fstype": "local", "format": "json", "filename": "Tout.255.json"}}]],
    [256, 2, 'Citi skip match sort 1 col, p1(NBL)', 'products', 0,
     [{'$skip': upperlimit}, matchCiti2, {'$limit': upperlimit}, {'$sort': {"Product.MarketSectorDescription": 1}},
      projectCiti1A, {'$out': {"fstype": "local", "format": "json", "filename": "Tout.256.json"}}]],
    [257, 2, 'Citi SNRD-2374 (NBL)               ', 'products', 0,
     [{'$skip': upperlimit}, matchCiti2, {'$limit': upperlimit}, {'$sort': {"Product.MarketSectorDescription": 1}},
      projectCiti1A]],

    [300, 3, 'GDM  limit 1                      ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder300}, {'$limit': 1}, proj_full,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.300.json"}}]],
    [301, 3, 'GDM  limit 10                     ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder300}, {'$limit': 10}, proj_full,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.301.json"}}]],
    [302, 3, 'GDM  limit 100                    ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder300}, {'$limit': 100}, proj_full,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.302.json"}}]],
    [303, 3, 'GDM  limit 1k                     ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder300}, {'$limit': 1000}, proj_full,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.303.json"}}]],
    [304, 3, 'GDM  limit 10k                    ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder300}, {'$limit': 10000}, proj_full,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.304.json"}}]],
    [305, 3, 'GDM  limit 100k                   ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder300}, {'$limit': 100000}, proj_full,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.305.json"}}]],

    [310, 4, 'GDM  limit 1   3 column           ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder310}, {'$limit': 1}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.310.json"}}]],
    [311, 4, 'GDM  limit 10   3 column          ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder310}, {'$limit': 10}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.311.json"}}]],
    [312, 4, 'GDM  limit 100  3 column          ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder310}, {'$limit': 100}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.312.json"}}]],
    [313, 4, 'GDM  limit 1k   3 column          ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder310}, {'$limit': 1000}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.313.json"}}]],
    [314, 4, 'GDM  limit 10k  3 column          ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder310}, {'$limit': 10000}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.314.json"}}]],
    [315, 4, 'GDM  limit 100k 3 column          ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder310}, {'$limit': 100000}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.315.json"}}]],

    [320, 4, 'GDM  limit 1   7 column           ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder320}, {'$limit': 1}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.320.json"}}]],
    [321, 4, 'GDM  limit 10   7 column          ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder320}, {'$limit': 10}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.321.json"}}]],
    [322, 4, 'GDM  limit 100  7 column          ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder320}, {'$limit': 100}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.322.json"}}]],
    [323, 4, 'GDM  limit 1k   7 column          ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder320}, {'$limit': 1000}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.323.json"}}]],
    [324, 4, 'GDM  limit 10k  7 column          ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder320}, {'$limit': 10000}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.324.json"}}]],
    [325, 4, 'GDM  limit 100k 7 column          ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder320}, {'$limit': 100000}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.325.json"}}]],
    [326, 4, 'GDM  limit 10k  7 column          ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder320A}, {'$limit': 10000}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.326.json"}}]],

    [330, 4, 'GDM  limit 1   session id         ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder330}, {'$limit': 1}, proj_sid,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.330.json"}}]],
    [331, 4, 'GDM  limit 10   session id        ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder330}, {'$limit': 10}, proj_sid,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.331.json"}}]],
    [332, 4, 'GDM  limit 100  session id        ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder330}, {'$limit': 100}, proj_sid,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.332.json"}}]],
    [333, 4, 'GDM  limit 1k   session id        ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder330}, {'$limit': 1000}, proj_sid,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.333.json"}}]],
    [334, 4, 'GDM  limit 10k  session id        ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder330}, {'$limit': 10000}, proj_sid,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.334.json"}}]],
    [335, 4, 'GDM  limit 100k session id        ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': sortOrder330}, {'$limit': 100000}, proj_sid,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.335.json"}}]],

    [340, 4, 'GDM  limit 1    SNRD-2231         ', 'fullSQL', 0,
     [{'$limit': upperlimit}, match340, sortOrder340, {'$limit': 1}, proj_sid,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.340.json"}}]],
    [341, 4, 'GDM  limit 10   SNRD-2231         ', 'fullSQL', 0,
     [{'$limit': upperlimit}, match340, sortOrder340, {'$limit': 10}, proj_sid,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.341.json"}}]],
    [342, 4, 'GDM  limit 100  SNRD-2231         ', 'fullSQL', 0,
     [{'$limit': upperlimit}, match340, sortOrder340, {'$limit': 100}, proj_sid,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.342.json"}}]],
    [343, 4, 'GDM  limit 1k   SNRD-2231         ', 'fullSQL', 0,
     [{'$limit': upperlimit}, match340, sortOrder340, {'$limit': 1000}, proj_sid,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.343.json"}}]],
    [344, 4, 'GDM  limit 10k  SNRD-2231         ', 'fullSQL', 0,
     [{'$limit': upperlimit}, match340, sortOrder340, {'$limit': 10000}, proj_sid,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.344.json"}}]],
    [345, 4, 'GDM  limit 100k SNRD-2231         ', 'fullSQL', 0,
     [{'$limit': upperlimit}, match340, sortOrder340, {'$limit': 100000}, proj_sid,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.345.json"}}]],

    [350, 4, 'GDM  SNRD-2466                    ', 'fullSQL', 0,
     [{'$limit': upperlimit}, {'$sort': {'Full Sql': 1}}, {'$limit': 2}, project_all,
      {'$out': {"fstype": "local", "format": "json", "filename": "Out.350.json"}}]],
    [351, 4, 'GDM  SNRD-2480                    ', 'session', 0,
     [{'$limit': upperlimit}, {'$group': {'_id': '$Session Start', 'count': {'$sum': 1}}}, {'$sort': {'_id': -1}},
      project_all, {'$out': {"fstype": "local", "format": "json", "filename": "Out.351.json"}}]],
    [352, 4, 'Temp whole bson join              ', 'fullSQL', 0, [{'$limit': upperlimit}, {
        '$join': {'$joined': {"s": "session"}, '$match': [{"Session Id": "$s.$Session Id"}],
                  '$project': {"full_sql_id": "$_id", "OS_USER": "$s.$OS User", "Session Id": "$s.$Session Id",
                               "Full Sql": 1, "Succeeded": 1}, '$having': {"Session Id": {'$lt': 500000}}}},
                                                                  {'$sort': sortOrderSessIdId},
                                                                  {'$project': {'Session Id': 1, '_id': 1}}, {
                                                                      '$out': {"fstype": "local", "format": "json",
                                                                               "filename": "Out.352.json"}}]],

]
'''
[56, 1, '43prime                           ', 'samstr',  0,   [{'$limit': upperlimit},{'$sort': sortOrder43},project_a,{'$out':{"fstype": "local", "format": "json", "filename": "Out.56.json"}}]],
[93, 1, 'Sorting Mixed Types               ', 'samm',  0,   [{'$optimizer':0},{'$limit': upperlimit},{'$sort': {'a': 1}},{'$project': {'_id': 0, 'a': 1}},project_all,{'$out':{"fstype": "local", "format": "json", "filename": "Out.93.json"}}]],
db.products.aggregate({$limit: 1000000}, {$sort: {"Product.MarketSectorDescription":1,"cloudTimestamp":1,"_id":1}})
['DISTR-A just sort                 ', 'samDist', 0,   [{'$limit': upperlimit},{'$sort': {'a': 1}}]],
[183, 1, 'Sorting output of group           ', 'samsz', 0,     [{'$limit': upperlimit},{$group: {_id: {$mod: ['$i',5000000]},count:{$sum: 1}}},{'$sort': {'count': 1, '_id': 1}},{'$project': {_'_id':0, 'count':1}},{'$out':{"fstype": "local", "format": "json", "filename": "Out.183.json" }}]],
'''


class SORT_TEST(object):

    @classmethod
    def setUpClass(self):
        self.connect(0)
        self.db_name = 'sam'
        self.lastVirtualMemory = 0

    @classmethod
    def clearCache(self, db):
        if args.clearCache:
            clear_system_cache()
            self.clear_sonar_cache(db)

    @classmethod
    def connect(self, level):
        if level > 600:
            print
            "timeouts exceeded"
            return
        try:
            # print "port: ", args.port
            # self.dbconnection = pymongo.MongoClient(host ='localhost',port = args.port)
            self.dbconnection = pymongo.MongoClient(uri)
        except Exception as e:
            print
            'couldnt connect. sleeping for 5 seconds then retrying'
            time.sleep(5)
            print
            e
            self.connect(level + 1)

    @classmethod
    def stop(self):
        self.dbconnection.close()

    @classmethod
    def vm(self):
        if 0 == args.virtualMemoryPrintout:
            return ""
        try:
            self.vmdoc = self.dbconnection['sonar_log']['sonar_state']
            cursor = self.vmdoc.aggregate([{'$sort': {'_id': -1}}, {'$limit': 1}, {
                '$project': {'_id': 0, 'vm': {'$div': ['$sonarVirtualMemoryKb', 1048576]}}}])
            docs = [doc for doc in cursor]
        except Exception as e:
            printLn("Lost DB Connection 1: %s" % e.message)
            return ""
        vm = docs[0]["vm"]
        ret = ""
        if vm > self.lastVirtualMemory:
            if 0 == self.lastVirtualMemory:
                ret = "%8.6f" % vm
            else:
                ret = "+%8.6f = %8.6f" % (vm - self.lastVirtualMemory, vm)
            self.lastVirtualMemory = vm
        return ret

    @classmethod
    def clear_sonar_cache(self, db):
        try:
            self.dbconnection[db].command('cacheReset', 1)
        except Exception as e:
            printLn("Lost DB Connection 2: %s" % e.message)

    @classmethod
    def sonar_version(self, db):
        try:
            ver = self.dbconnection[db].command('buildInfo', 1)
        except Exception as e:
            printLn("Lost DB Connection 3: %s" % e.message)
            return 2.0
        if (args.unauth):
            verfloat = 1.4
        else:
            verfloat = float(ver['sonarVersion'][1:4])
        print("SonarW Version: %s" % verfloat)
        return verfloat

    @classmethod
    def set_block_allocation_size_percentage(self, value):
        try:
            self.dbconnection.admin.command("setParameter", block_allocation_size_percentage=value)
        except Exception as e:
            printLn("Lost DB Connection 4: %s" % e.message)
            pass

    @classmethod
    def make_query_distributed(self, query, comment):
        res = []
        res.append({'$comment': comment})
        for clause in query:
            res.append(clause)
        return res

    @classmethod
    def make_query_non_distributed(self, query):
        res = [{'$dist': False}]
        for clause in query:
            res.append(clause)
        return res

    @classmethod
    def checkResult(self, testNumber, duration, rate):
        (res, passed) = self.checkResultHelper(testNumber, "json", duration, rate)
        deb("checkResult1 passed: %d" % passed)
        if res == "OK":
            (res, passed) = self.checkResultHelper(testNumber, "csv", duration, rate)
            deb("checkResult2 passed: %d" % passed)
        return (res, passed)

    @classmethod
    def checkResultHelper(self, testNumber, suffix, duration, rate):
        deb("%s %d %s" % ("checkResultHelper", testNumber, suffix))
        filename = "Out.%d.%s" % (testNumber, suffix)
        self.result_file_name = "%s/agg_out/%s" % (args.sonarhome, filename)
        fileTemplate = "%s/%d/%s" % (args.baselineDirectory, global_iterations, filename)
        md5FileTemplate = "%s.md5" % fileTemplate
        calibration_file = "%s/%d/calibration/history.%d.stat" % (args.baselineDirectory, global_iterations, testNumber)
        result = "OK"
        passed = False
        if os.path.exists(os.path.expanduser(fileTemplate)) or os.path.exists(os.path.expanduser(md5FileTemplate)):
            res = cmd(diffstr % (self.result_file_name, fileTemplate))
            result = "PASSED"
            passed = True
            if len(res):
                updateFailures()
                return ("FAILED1: \n    diff %s %s" % (
                os.path.expandvars(self.result_file_name), os.path.expanduser(fileTemplate)), False)
        if (-1 != duration):
            return check_performance_results(testNumber, duration, rate, result)
        return (result, passed)

    @classmethod
    def run_query(self, distributed):
        current_milli_time = lambda: int(round(time.time() * 1000))
        self.clearCache(self.database)
        start_time = current_milli_time()
        query_succeeded = False
        result = "PASSED"
        for i in range(1, 400):
            try:
                if distributed:
                    self.comment_label = make_comment(self.t)
                    self.actual_query = self.make_query_distributed(self.query, self.comment_label)
                else:
                    self.actual_query = self.make_query_non_distributed(self.query)
                deb("%s db.%s.aggregate(%s)" % (self.class_to_db[self.c], self.col, self.query))
                if self.collation:
                    results = self.db.aggregate(self.actual_query, collation=self.collation)
                else:
                    results = self.db.aggregate(self.actual_query)
                query_succeeded = True
                if i > 1:
                    print("%d Retries" % i)
            except Exception as e:
                self.skippedColumns = self.skippedColumns + " " + self.col
                deb("Caught Exception in run_query: [%s]" % e.message)
                result = "FAILED_QUERY %s" % e.message
                match = re.match(dist_error_re, e.message)
                pass
                if match:
                    time.sleep(8)
                    continue
                else:
                    print("Failed: %s" % e.message)
            break
        if i >= 400:
            deb("Exceeded retries")
        end_time = current_milli_time()
        now = datetime.datetime.now()
        duration = (end_time - start_time) * 1.0
        if duration == 0:
            duration = 1
        return (query_succeeded, result, duration)

    @classmethod
    def run_distributed_test(self):
        passedWorkersTest = False;
        if args.reCalibrate:
            return self.recalibrate();
        self.db = self.dbconnection[self.class_to_db[self.c]][self.col]
        if 0 == args.skipNonDist:
            (passed, result, duration) = self.run_query(False)
            rate = (iterations * loops) / duration
            if passed:
                (result, passed) = self.checkResult(self.t, duration, rate)
            deb("Non-dist result: %s passed: %s duration: %8.2f rate: %s" % (result, passed, duration, rate))
            if not passed:
                return (duration, -1, rate, result, "NOT_RUN", passed, -1)
        else:
            duration = -1
            rate = -1
            result = -1
        (passedDist, resultDist, durationDist) = self.run_query(True)
        rateDist = (iterations * loops) / durationDist
        if passed:
            (resultDist, passedDist) = self.checkResult(self.t, durationDist, rateDist)
        deb("resultDist: %s passedDist: %d" % (resultDist, passedDist))
        if not passedDist:
            return (duration, durationDist, rate, result, resultDist, passedDist, passedWorkersTest)
        passedWorkersTest = self.verify_all_workers_did_work()
        return (duration, durationDist, rate, result, resultDist, passedDist, passedWorkersTest)

    @classmethod
    def run_individual_test(self):
        # cmd("rm -f $SONAR_HOME/agg_out/*")
        if args.reCalibrate:
            return self.recalibrate();
        current_milli_time = lambda: int(round(time.time() * 1000))
        self.db = self.dbconnection[self.class_to_db[self.c]][self.col]
        self.clearCache(self.database)
        start_time = current_milli_time()
        (passed, result, duration) = self.run_query(False)
        end_time = current_milli_time()
        now = datetime.datetime.now()
        duration = (end_time - start_time) * 1.0
        if duration == 0:
            duration = 1
        rate = (iterations * loops) / duration
        if passed:
            (result, passed) = self.checkResult(self.t, duration, rate)
        return (duration, rate, result, passed)

    @classmethod
    def recalibrate(self):
        # remove calibration file
        calibration_file = "%s/%d/calibration/history.%d.stat" % (args.baselineDirectory, global_iterations, self.t)
        try:
            os.remove(calibration_file)
        except:
            deb("Could not remove: %s" % calibration_file)
        duration = 0;
        rate = 0
        for i in range(0, recalibrationSamples):
            current_milli_time = lambda: int(round(time.time() * 1000))
            self.db = self.dbconnection[self.database][self.col]
            self.clearCache(self.database)
            start_time = current_milli_time()
            try:
                deb("%s db.%s.aggregate(%s)" % (self.class_to_db[self.c], self.col, self.query))
                results = self.db.aggregate(self.query)
            except:
                self.skippedColumns = self.skippedColumns + " " + self.col
            end_time = current_milli_time()
            now = datetime.datetime.now()
            duration = (end_time - start_time) * 1.0
            if duration == 0:
                duration = 1
            rate = (iterations * loops) / duration
            logResult(now, self.t, duration, rate, -1)
        (result, passed) = self.checkResult(self.t, duration, rate)
        result = "%s (recalibrated) %s" % (result, rates(self.t))
        return (duration, rate, result, passed)

    @classmethod
    def get_data(self, i, d):
        self.t = d[i][0]
        self.ts = str(self.t)
        self.c = d[i][1]
        self.desc = d[i][2]
        self.col = d[i][3]
        self.starting_version = float(d[i][4])
        self.query = d[i][5]
        self.distributed = False
        if len(d[i]) > 6:
            self.collation = d[i][6]
        else:
            self.collation = None

    @classmethod
    def set_distributed(self):
        self.distributed = True

    @classmethod
    def selected(self, index):
        ts = str(index)
        if (index >= args.start and index <= args.end) and (len(tests) == 0 or ts in tests):
            return true;
        return false;

    @classmethod
    def verify_all_workers_did_work(self):
        deb("verify_all_workers_did_work label: %s" % self.comment_label)
        try:
            dist_info = [d for d in
                         self.dbconnection['admin']['system.dist_stats'].find(
                             {"label": self.comment_label})]
        except Exception as e:
            printLn("Lost DB Connection 5: %s" % e.message)
            return False
        if 0 == len(dist_info):
            # print("No distributed results found for label: %s"%self.comment_label)
            return True
        deb("*** Dist Info: " + pprint.pformat(dist_info[0]))

        if len(dist_info[0]['workers']) != args.expectedWorkers and len(dist_info[0]['workers']) == 1:
            return True

        if len(dist_info[0]['workers']) != args.expectedWorkers:
            print("Not all workers did some work -only %d out of %d" % (
                len(dist_info[0]['workers']), args.expectedWorkers))
            return False
            raise Exception(
                "Not all workers did some work - only %d out of %d" % (
                    len(dist_info[0]['workers']), args.expectedWorkers))

        for worker in dist_info[0]['workers']:
            if worker['errors'] != 0:
                print("Worker errors: %d" % worker['errors'])
                return False
            if worker['calls'] != worker['replies']:
                print("Worker calls != worker replies")
                return False
        return True

    @classmethod
    def consume_find_cursor(self, description, index, cursor):
        deb2("consume_find_cursor: trying test: %s start: %d end: %d index: %d" % (
        description, args.start, args.end, index));
        if true:
            comment = make_comment(index)
            deb("consume_find_cursor: starting test: %s" % description);
            current_milli_time = lambda: int(round(time.time() * 1000))
            start_time = current_milli_time()
            i = 0;
            fileTemplate = "Out.%d.json" % (index)
            md5FileTemplate = "Out.%d.md5" % (index)
            baselineFileTemplate = "%s/%s/Out.%d.json" % (args.baselineDirectory, global_iterations, index)
            baselineMd5FileTemplate = "%s/%s/Out.%d.md5" % (args.baselineDirectory, global_iterations, index)
            deb("consume_find_cursor: baseline md5: %s" % baselineMd5FileTemplate)
            writingToFile = false

            if not os.path.exists(os.path.expanduser(baselineFileTemplate)) and not os.path.exists(
                    os.path.expanduser(baselineMd5FileTemplate)):
                writingToFile = true;

            if args.createFile:
                writingToFile = true
                deb("createFile")
                fileTemplate = "Out.%d.json" % index

            if writingToFile:
                deb("writing to file: %s" % fileTemplate)
                f = open(fileTemplate, 'w')

            m = md5.new()
            first = true;
            status = "PASSED"
            now = datetime.datetime.now()
            try:
                for doc in cursor:
                    if first:
                        deb("consume_find_cursor: first item available");
                        first = false;
                    docstr = convertToString(doc) + "\n";
                    if writingToFile:
                        f.write(docstr.encode("utf8"));
                    m.update(docstr.encode("utf8"));
                    i = i + 1;
            except Exception as e:
                print
                "Exception %s consuming cursor" % e.message
                status = "FAILED %s" % e.message
                printLn("%4d %-35s %8.0f %s" % (index, description, 0, status))
                try:
                    self.record_results.insert(
                        {'date': now, 'testDate': self.testDate, 'iterations': global_iterations, 'testNumber': index,
                         'testName': description, 'duration': 0, 'status': status});
                except Exception as e:
                    printLn("Lost DB Connection 6: %s" % e.message)
                    pass
                return
            end_time = current_milli_time();
            duration = (end_time - start_time) * 1.0
            if duration == 0:
                duration = 1
            result_durations[index] = duration
            if writingToFile:
                f.close()
            if os.path.exists(os.path.expanduser(baselineMd5FileTemplate)) and not args.createFile:
                md5f = open(baselineMd5FileTemplate, 'r')
                baseMd5 = md5f.read()
                if (baseMd5 != m.hexdigest()):
                    printLn("%s %4d %-35s %8.0f FAILED MD5 check: baseline: [%s] current: [%s]" % (
                    global_iterations_str, index, description, duration, baseMd5, m.hexdigest()))
                    if args.createFile:
                        printLn("diff %s %s" % (fileTemplate, baselineFileTemplate))
                    self.record_results.insert(
                        {'date': now, 'testDate': self.testDate, 'iterations': global_iterations, 'testNumber': index,
                         'testName': description, 'duration': duration, 'status': "FAILED"});
                    return
            else:
                deb("md5: %s" % md5FileTemplate)
                md5f = open(md5FileTemplate, 'w')
                md5f.write(m.hexdigest())
                md5f.close()
                status = "CREATED BASELINE"
            if index >= 2000 and index < 3000:
                last = 0
                try:
                    last = result_durations[index - 1000]
                except Exception as e:
                    printLn("Lost DB Connection 7: %s" % e.message)
                    pass
                if last:
                    printLn("%s %4d %-35s %8.0fms %3.0f%% %s" % (
                    global_iterations_str, index, description, duration, (duration * 100 / last), status))
                else:
                    printLn("%s %4d %-35s %8.0fms %s" % (global_iterations_str, index, description, duration, status))
            else:
                printLn("%s %4d %-35s %8.0fms %s" % (global_iterations_str, index, description, duration, status))
            deb("insert results")
            try:
                self.record_results.insert(
                    {'testDate': self.testDate, 'iterations': global_iterations, 'date': now, 'testNumber': index,
                     'testName': description, 'duration': duration, 'status': status});
            except Exception as e:
                printLn("Lost DB Connection 8: %s" % e.message)
                pass
            deb("close the cursor")
            cursor.close()
            deb("exit function")

    @classmethod
    def getColl(self, collection):
        return self.dbconnection[self.database][collection + iterationString(global_iterations)]

    @classmethod
    def execute_find_query(self, dist):
        if global_iterations > 1000000:
            return
        self.database = "sam"
        coll = self.getColl("samDist")
        # if self.selected(1000) and not dist: self.consume_find_cursor('Empty find no sort       ',  1000, coll.find({'$dist':false}).sort('field_not_exists':1).limit(upperlimit));
        if self.selected(1001): self.consume_find_cursor('Empty find, sort LONG    ', 1001,
                                                         coll.find({'$dist': false}).sort('i', 1).limit(upperlimit));
        if self.selected(1002): self.consume_find_cursor('Empty find, sort LONG    ', 1002,
                                                         coll.find({'$dist': false}).sort('i', -1).limit(upperlimit));
        if self.selected(1003): self.consume_find_cursor('Project LONG, sort LONG  ', 1003,
                                                         coll.find({'$dist': false}, {'i': 1}).sort('i', 1).limit(
                                                             upperlimit));
        if self.selected(1004): self.consume_find_cursor('Project LONG, sort LONG  ', 1004,
                                                         coll.find({'$dist': false}, {'i': 1}).sort('i', -1).limit(
                                                             upperlimit));

        if self.selected(1010): self.consume_find_cursor('Project 2, sort on 2     ', 1010,
                                                         coll.find({'$dist': false}, {'a': 1, 'b': 1}).sort(
                                                             [('a', 1), ('_id', 1)]).limit(upperlimit));
        if self.selected(1011): self.consume_find_cursor('Project 2, sort on 2     ', 1011,
                                                         coll.find({'$dist': false}, {'a': 1, 'b': 1}).sort(
                                                             [('a', -1), ('_id', 1)]).limit(upperlimit));
        if self.selected(1012): self.consume_find_cursor('Project 2, sort on 2     ', 1012,
                                                         coll.find({'$dist': false}, {'a': 1, 'b': 1}).sort(
                                                             [('a', 1), ('_id', -1)]).limit(upperlimit));
        if self.selected(1013): self.consume_find_cursor('Project 2, sort on 2     ', 1013,
                                                         coll.find({'$dist': false}, {'a': 1, 'b': 1}).sort(
                                                             [('a', -1), ('_id', -1)]).limit(upperlimit));
        if self.selected(1014): self.consume_find_cursor('Project 3, sort on 3     ', 1014,
                                                         coll.find({'$dist': false}, {'a': 1, 'b': 1, 'c': 1}).sort(
                                                             [('a', 1), ('b', 1), ('_id', 1)]).limit(upperlimit));
        if self.selected(1015): self.consume_find_cursor('Project 3, sort on 3     ', 1015,
                                                         coll.find({'$dist': false}, {'a': 1, 'b': 1, 'c': 1}).sort(
                                                             [('a', -1), ('b', 1), ('_id', 1)]).limit(upperlimit));
        if self.selected(1016): self.consume_find_cursor('Project 3, sort on 3     ', 1016,
                                                         coll.find({'$dist': false}, {'a': 1, 'b': 1, 'c': 1}).sort(
                                                             [('a', 1), ('b', -1), ('_id', 1)]).limit(upperlimit));
        if self.selected(1017): self.consume_find_cursor('Project 3, sort on 3     ', 1017,
                                                         coll.find({'$dist': false}, {'a': 1, 'b': 1, 'c': 1}).sort(
                                                             [('a', 1), ('b', 1), ('_id', -1)]).limit(upperlimit));
        if self.selected(1018): self.consume_find_cursor('Project 3, sort on 3     ', 1018,
                                                         coll.find({'$dist': false}, {'a': 1, 'b': 1, 'c': 1}).sort(
                                                             [('a', -1), ('b', -1), ('_id', -1)]).limit(upperlimit));
        if self.selected(1019): self.consume_find_cursor('Project 4, sort on 4     ', 1019, coll.find({'$dist': false},
                                                                                                      {'a': 1, 'b': 1,
                                                                                                       'c': 1,
                                                                                                       'd': 1}).sort(
            [('a', 1), ('b', 1), ('c', 1), ('_id', -1)]).limit(upperlimit));
        if self.selected(1020): self.consume_find_cursor('Project 4, sort on 4     ', 1020, coll.find({'$dist': false},
                                                                                                      {'a': 1, 'b': 1,
                                                                                                       'c': 1,
                                                                                                       'd': 1}).sort(
            [('a', 1), ('b', -1), ('c', -1), ('_id', -1)]).limit(upperlimit));
        if self.selected(1021): self.consume_find_cursor('Project 4, sort on 4     ', 1021, coll.find({'$dist': false},
                                                                                                      {'a': 1, 'b': 1,
                                                                                                       'c': 1,
                                                                                                       'd': 1}).sort(
            [('a', -1), ('b', 1), ('c', 1), ('_id', 1)]).limit(upperlimit));

        if self.selected(1030): self.consume_find_cursor('Sort on string           ', 1030,
                                                         coll.find({'$dist': false}, {'_id': 0, 'd': 1}).sort('d',
                                                                                                              1).limit(
                                                             upperlimit));
        if self.selected(1031): self.consume_find_cursor('Sort on string           ', 1031,
                                                         coll.find({'$dist': false}, {'_id': 0, 'd': 1}).sort('d',
                                                                                                              -1).limit(
                                                             upperlimit));
        if self.selected(1032): self.consume_find_cursor('Sort on int with range   ', 1032,
                                                         coll.find({'$dist': false}, {'_id': 0, 'g': 1}).sort('g',
                                                                                                              1).limit(
                                                             upperlimit));
        if self.selected(1033): self.consume_find_cursor('Sort on int with range d ', 1033,
                                                         coll.find({'$dist': false}, {'_id': 0, 'g': 1}).sort('g',
                                                                                                              -1).limit(
                                                             upperlimit));
        if self.selected(1034): self.consume_find_cursor('Sort on object           ', 1034,
                                                         coll.find({'$dist': false}, {'_id': 0, 'e': 1}).sort('e',
                                                                                                              1).limit(
                                                             upperlimit));
        if self.selected(1035): self.consume_find_cursor('Sort on object desc      ', 1035,
                                                         coll.find({'$dist': false}, {'_id': 0, 'e': 1}).sort('e',
                                                                                                              -1).limit(
                                                             upperlimit));
        if self.selected(1039): self.consume_find_cursor('Sort with regex          ', 1039,
                                                         coll.find({'$dist': false, 'a': re.compile('01')},
                                                                   {'_id': 0, 'a': 1}).sort('a', 1).limit(upperlimit));

        if self.selected(1040): self.consume_find_cursor('Sort with skip 1k        ', 1040,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).skip(
                                                             1000).sort('a', 1).limit(upperlimit));
        if self.selected(1041): self.consume_find_cursor('Sort with skip 1k desc   ', 1041,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).skip(
                                                             1000).sort('a', -1).limit(upperlimit));
        if self.selected(1042): self.consume_find_cursor('Sort with skip 5k        ', 1042,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).skip(
                                                             5000).sort('a', 1).limit(upperlimit));
        if self.selected(1043): self.consume_find_cursor('Sort with skip 5k desc   ', 1043,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).skip(
                                                             5000).sort('a', -1).limit(upperlimit));

        if self.selected(1050): self.consume_find_cursor('Sort batchsize 200       ', 1050,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).batch_size(
                                                             200).sort('a', 1).limit(upperlimit));
        if self.selected(1051): self.consume_find_cursor('Sort batchsize 200 desc  ', 1051,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).batch_size(
                                                             200).sort('a', -1).limit(upperlimit));
        if self.selected(1052): self.consume_find_cursor('Sort batchsize 500       ', 1052,
                                                         coll.find({'$dist': false}, {'a': 1, 'b': 1}).batch_size(
                                                             500).sort([('a', 1), ('_id', 1)]).limit(upperlimit));
        if self.selected(1053): self.consume_find_cursor('Sort batchsize 500       ', 1053, coll.find({'$dist': false},
                                                                                                      {'a': 1, 'b': 1,
                                                                                                       'c': 1}).batch_size(
            500).sort([('a', 1), ('b', 1), ('_id', 1)]).limit(upperlimit));
        if self.selected(1054): self.consume_find_cursor('Sort batchsize 500 a/desc', 1054, coll.find({'$dist': false},
                                                                                                      {'a': 1, 'b': 1,
                                                                                                       'c': 1}).batch_size(
            500).sort([('a', -1), ('b', -1), ('_id', 1)]).limit(upperlimit));
        if self.selected(1055): self.consume_find_cursor('Sort batchsize 500 a/desc', 1055, coll.find({'$dist': false},
                                                                                                      {'a': 1, 'b': 1,
                                                                                                       'c': 1}).batch_size(
            500).sort([('a', 1), ('b', 1), ('c', 1), ('_id', -1)]).limit(upperlimit));
        if self.selected(1056): self.consume_find_cursor('Sort skip/limit          ', 1056,
                                                         coll.find({'$dist': false}, {'_id': 0}, skip=1500,
                                                                   limit=10).limit(upperlimit).sort('i', 1));
        if self.selected(1057): self.consume_find_cursor('Sort skip/limit          ', 1057,
                                                         coll.find({'$dist': false}, {'_id': 0}, skip=2500,
                                                                   limit=100).limit(upperlimit).sort('i', 1));
        if self.selected(1058): self.consume_find_cursor('Sort skip/limit          ', 1058,
                                                         coll.find({'$dist': false}, {'_id': 0}, skip=3500,
                                                                   limit=1000).limit(upperlimit).sort('i', 1));

        if self.selected(1060): self.consume_find_cursor('Sort on int              ', 1060,
                                                         coll.find({'$dist': false}, {'_id': 0, 'd': 1}).sort('d',
                                                                                                              1).limit(
                                                             upperlimit));
        # SNRD-3995
        #        if self.selected(1061): self.consume_find_cursor('Sort where               ',  1061, coll.find().where('this.c > 0').sort('c',1).limit(upperlimit));
        #        if self.selected(1062): self.consume_find_cursor('Sort complex where       ',  1062, coll.find({},{'a':1,'i':1,'g':1}).where('this.g < 20 && this.i < 1000000').sort('a',1).limit(upperlimit));
        if self.selected(1068): self.consume_find_cursor('Sort maxTimeMS           ', 1068,
                                                         coll.find({'$dist': false}, {'_id': 0, 'e': 1},
                                                                   modifiers={"$maxTimeMS": 5000}).sort('e', 1).limit(
                                                             upperlimit));
        if self.selected(1069): self.consume_find_cursor('Sort comment             ', 1069,
                                                         coll.find({'$dist': false}, {'_id': 0, 'i': 1}).comment(
                                                             "testing").sort('i', 1).limit(upperlimit));
        if self.selected(1070): self.consume_find_cursor('Sort comment             ', 1070,
                                                         coll.find({'$dist': false}, {'_id': 0, 'i': 1}).sort('i',
                                                                                                              1).comment(
                                                             "testing").limit(upperlimit));
        #        if self.selected(1071): self.consume_find_cursor('Sort where after sort    ',  1071, coll.find().sort('c',1).where('this.c > 1000000').limit(upperlimit));

        # SNRD-3999
        #        if self.selected(1072): self.consume_find_cursor('Sort where after sort    ',  1072, coll.find().sort('c',1).where('this.c < 1000000').limit(upperlimit));
        #        if self.selected(1073): self.consume_find_cursor('Sort where before sort   ',  1073, coll.find().where('this.c < 1000000').sort('c',1).limit(upperlimit));

        if self.selected(1080): self.consume_find_cursor('find few                 ', 1080,
                                                         coll.find({'$dist': false, 'a': {'$gt': 11110000}},
                                                                   {'_id': 0, 'a': 1}).sort('a', 1).limit(upperlimit));
        if self.selected(1081): self.consume_find_cursor('find many                ', 1081,
                                                         coll.find({'$dist': false, 'g': {'$gt': 4}},
                                                                   {'_id': 0, 'g': 1}).sort('g', 1).limit(upperlimit));
        if self.selected(1082): self.consume_find_cursor('find complex             ', 1082, coll.find(
            {'$dist': false, 'g': {'$gte': 5}, 'h': {'$lte': 50}}, {'_id': 0, 'g': 1}).sort('g', 1).limit(upperlimit));

        coll = self.getColl("samb")
        if self.selected(1200): self.consume_find_cursor('Sort on boolean          ', 1200,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort('a',
                                                                                                              1).limit(
                                                             upperlimit));
        if self.selected(1201): self.consume_find_cursor('Sort on boolean desc     ', 1201,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort('a',
                                                                                                              -1).limit(
                                                             upperlimit));

        coll = self.getColl("samo")
        if self.selected(1202): self.consume_find_cursor('Sort on object           ', 1202,
                                                         coll.find({'$dist': false}, {'_id': 0, 'e': 1}).sort('e',
                                                                                                              1).limit(
                                                             upperlimit));
        if self.selected(1203): self.consume_find_cursor('Sort on object desc      ', 1203,
                                                         coll.find({'$dist': false}, {'_id': 0, 'e': 1}).sort('e',
                                                                                                              -1).limit(
                                                             upperlimit));

        coll = self.getColl("samstr")
        if self.selected(1204): self.consume_find_cursor('Sort on int              ', 1204,
                                                         coll.find({'$dist': false}, {'_id': 0, 'b': 1}).sort('b',
                                                                                                              1).limit(
                                                             upperlimit));
        if self.selected(1205): self.consume_find_cursor('Sort on int desc         ', 1205,
                                                         coll.find({'$dist': false}, {'_id': 0, 'b': 1}).sort('b',
                                                                                                              -1).limit(
                                                             upperlimit));

        coll = self.getColl("samrex")
        if self.selected(1206): self.consume_find_cursor('Sort on regex            ', 1206,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort('a',
                                                                                                              1).limit(
                                                             upperlimit));
        if self.selected(1207): self.consume_find_cursor('Sort on regex desc       ', 1207,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort('a',
                                                                                                              -1).limit(
                                                             upperlimit));

        coll = self.getColl("sambin")
        if self.selected(2208): self.consume_find_cursor('Sort on binData          ', 1208,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort('a',
                                                                                                              1).limit(
                                                             upperlimit));
        if self.selected(2209): self.consume_find_cursor('Sort on binData desc     ', 1209,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort('a',
                                                                                                              -1).limit(
                                                             upperlimit));

        coll = self.getColl("samatz")
        if self.selected(1210): self.consume_find_cursor('Sort on timestamp        ', 1210,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort('a',
                                                                                                              1).limit(
                                                             upperlimit));
        if self.selected(1211): self.consume_find_cursor('Sort on timestamp desc   ', 1211,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort('a',
                                                                                                              -1).limit(
                                                             upperlimit));

        coll = self.getColl("samasz")
        if self.selected(1212): self.consume_find_cursor('Sort on [string]         ', 1212,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort('a',
                                                                                                              1).limit(
                                                             upperlimit));
        if self.selected(1213): self.consume_find_cursor('Sort on [string] desc    ', 1213,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort('a',
                                                                                                              -1).limit(
                                                             upperlimit));

        coll = self.getColl("samarex")
        if self.selected(1214): self.consume_find_cursor('Sort on [regex]          ', 1214,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort('a',
                                                                                                              1).limit(
                                                             upperlimit));
        if self.selected(1215): self.consume_find_cursor('Sort on [regex] desc     ', 1215,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort('a',
                                                                                                              -1).limit(
                                                             upperlimit));

        coll = self.getColl("samatz")
        if self.selected(1216): self.consume_find_cursor('Sort on [timestamp]      ', 1216,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort('a',
                                                                                                              1).limit(
                                                             upperlimit));
        if self.selected(1217): self.consume_find_cursor('Sort on [timestamp] desc ', 1217,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort('a',
                                                                                                              -1).limit(
                                                             upperlimit));

        coll = self.getColl("samaob")
        if self.selected(1218): self.consume_find_cursor('Sort on [object]         ', 1218,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort('a',
                                                                                                              1).limit(
                                                             upperlimit));
        if self.selected(1219): self.consume_find_cursor('Sort on [object] desc    ', 1219,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort('a',
                                                                                                              -1).limit(
                                                             upperlimit));

        coll = self.getColl("samamix")
        if self.selected(1220): self.consume_find_cursor('Sort on [mixed type]     ', 1220,
                                                         coll.find({'$dist': false}, {'a': 1, '_id': 1}).sort(
                                                             [('a', 1), ('_id', 1)]).limit(upperlimit));
        if self.selected(1221): self.consume_find_cursor('Sort on [mixed type] desc', 1221,
                                                         coll.find({'$dist': false}, {'a': 1, '_id': 1}).sort(
                                                             [('a', -1), ('_id', 1)]).limit(upperlimit));

        coll = self.getColl("samaob")
        if self.selected(1222): self.consume_find_cursor('Sort with elemMatch      ', 1222, coll.find(
            {'$dist': false, 'a': {'$elemMatch': {'x': {'$gt': 'abc'}}}}, {'_id': 0, 'a': 1}).sort('a', 1).limit(
            upperlimit));
        if self.selected(1223): self.consume_find_cursor('Sort with slice          ', 1223, coll.find({'$dist': false},
                                                                                                      {'_id': 0, 'a': {
                                                                                                          '$slice': 3}}).sort(
            'i', 1).limit(upperlimit));

        coll = self.getColl("samsz_collation")
        if self.selected(1300): self.consume_find_cursor('Sorting Collation 1      ', 1300,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort('a',
                                                                                                              1).limit(
                                                             upperlimit).collation(col1));
        if self.selected(1301): self.consume_find_cursor('Sorting Collation 2      ', 1301,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort('a',
                                                                                                              1).limit(
                                                             upperlimit).collation(col2));
        if self.selected(1302): self.consume_find_cursor('Sorting Collation 3      ', 1302,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort('a',
                                                                                                              1).limit(
                                                             upperlimit).collation(col3));
        if self.selected(1303): self.consume_find_cursor('Sorting Collation 4      ', 1303,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col4));
        if self.selected(1304): self.consume_find_cursor('Sorting Collation 5      ', 1304,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', -1), ('i', -1)]).limit(upperlimit).collation(col5));
        if self.selected(1305): self.consume_find_cursor('Sorting Collation 6      ', 1305,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col6));
        if self.selected(1306): self.consume_find_cursor('Sorting Collation 7      ', 1306,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', -1), ('i', 1)]).limit(upperlimit).collation(col7));
        if self.selected(1307): self.consume_find_cursor('Sorting Collation 8      ', 1307,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', -1)]).limit(upperlimit).collation(col8));
        if self.selected(1308): self.consume_find_cursor('Sorting Collation 9      ', 1308,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col9));
        if self.selected(1309): self.consume_find_cursor('Sorting Collation 10     ', 1309,
                                                         coll.find({'$dist': false, 'i': {'$gt': 0}},
                                                                   {'_id': 0, 'a': 1}).sort([('a', 1), ('i', 1)]).limit(
                                                             upperlimit).collation(col10));
        if self.selected(1310): self.consume_find_cursor('Sorting Collation 11     ', 1310,
                                                         coll.find({'$dist': false, 'g': {'$gt': 2}},
                                                                   {'_id': 0, 'a': 1}).sort(
                                                             [('a', -1), ('i', 1)]).limit(upperlimit).collation(col11));
        if self.selected(1311): self.consume_find_cursor('Sorting Collation 12     ', 1311,
                                                         coll.find({'$dist': false, 'g': {'$gt': 1}},
                                                                   {'_id': 0, 'a': 1}).sort(
                                                             [('a', -1), ('i', -1)]).limit(upperlimit).collation(
                                                             col12));

        coll = self.getColl("samasz_collation")
        if self.selected(1400): self.consume_find_cursor('Sorting Collation 1      ', 1400,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col1));
        if self.selected(1401): self.consume_find_cursor('Sorting Collation 2      ', 1401,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col2));
        if self.selected(1402): self.consume_find_cursor('Sorting Collation 3      ', 1402,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col3));
        if self.selected(1403): self.consume_find_cursor('Sorting Collation 4      ', 1403,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col4));
        if self.selected(1404): self.consume_find_cursor('Sorting Collation 5      ', 1404,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', -1), ('i', -1)]).limit(upperlimit).collation(col5));
        if self.selected(1405): self.consume_find_cursor('Sorting Collation 6      ', 1405,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col6));
        if self.selected(1406): self.consume_find_cursor('Sorting Collation 7      ', 1406,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', -1), ('i', 1)]).limit(upperlimit).collation(col7));
        if self.selected(1407): self.consume_find_cursor('Sorting Collation 8      ', 1407,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', -1)]).limit(upperlimit).collation(col8));
        if self.selected(1408): self.consume_find_cursor('Sorting Collation 9      ', 1408,
                                                         coll.find({'$dist': false}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col9));
        if self.selected(1409): self.consume_find_cursor('Sorting Collation 10     ', 1409,
                                                         coll.find({'$dist': false, 'i': {'$gt': 0}},
                                                                   {'_id': 0, 'a': 1}).sort([('a', 1), ('i', 1)]).limit(
                                                             upperlimit).collation(col10));
        if self.selected(1410): self.consume_find_cursor('Sorting Collation 11     ', 1410,
                                                         coll.find({'$dist': false, 'g': {'$gt': 2}},
                                                                   {'_id': 0, 'a': 1}).sort(
                                                             [('a', -1), ('i', 1)]).limit(upperlimit).collation(col11));
        if self.selected(1411): self.consume_find_cursor('Sorting Collation 12     ', 1411,
                                                         coll.find({'$dist': false, 'g': {'$gt': 1}},
                                                                   {'_id': 0, 'a': 1}).sort(
                                                             [('a', -1), ('i', -1)]).limit(upperlimit).collation(
                                                             col12));

        if not dist:
            return
        # distributed
        self.set_distributed()
        coll = self.getColl("samDist")
        # if self.selected(2000): self.consume_find_cursor('Empty find no sort       ',  2000, coll.find({}).sort('field_not_exists',1).limit(upperlimit));
        if self.selected(2001): self.consume_find_cursor('Empty find, sort LONG    ', 2001,
                                                         coll.find({}).sort('i', 1).limit(upperlimit));
        if self.selected(2002): self.consume_find_cursor('Empty find, sort LONG    ', 2002,
                                                         coll.find({}).sort('i', -1).limit(upperlimit));
        if self.selected(2003): self.consume_find_cursor('Project LONG, sort LONG  ', 2003,
                                                         coll.find({}, {'i': 1}).sort('i', 1).limit(upperlimit));
        if self.selected(2004): self.consume_find_cursor('Project LONG, sort LONG  ', 2004,
                                                         coll.find({}, {'i': 1}).sort('i', -1).limit(upperlimit));

        if self.selected(2010): self.consume_find_cursor('Project 2, sort on 2     ', 2010,
                                                         coll.find({}, {'a': 1, 'b': 1}).sort(
                                                             [('a', 1), ('_id', 1)]).limit(upperlimit));
        if self.selected(2011): self.consume_find_cursor('Project 2, sort on 2     ', 2011,
                                                         coll.find({}, {'a': 1, 'b': 1}).sort(
                                                             [('a', -1), ('_id', 1)]).limit(upperlimit));
        if self.selected(2012): self.consume_find_cursor('Project 2, sort on 2     ', 2012,
                                                         coll.find({}, {'a': 1, 'b': 1}).sort(
                                                             [('a', 1), ('_id', -1)]).limit(upperlimit));
        if self.selected(2013): self.consume_find_cursor('Project 2, sort on 2     ', 2013,
                                                         coll.find({}, {'a': 1, 'b': 1}).sort(
                                                             [('a', -1), ('_id', -1)]).limit(upperlimit));
        if self.selected(2014): self.consume_find_cursor('Project 3, sort on 3     ', 2014,
                                                         coll.find({}, {'a': 1, 'b': 1, 'c': 1}).sort(
                                                             [('a', 1), ('b', 1), ('_id', 1)]).limit(upperlimit));
        if self.selected(2015): self.consume_find_cursor('Project 3, sort on 3     ', 2015,
                                                         coll.find({}, {'a': 1, 'b': 1, 'c': 1}).sort(
                                                             [('a', -1), ('b', 1), ('_id', 1)]).limit(upperlimit));
        if self.selected(2016): self.consume_find_cursor('Project 3, sort on 3     ', 2016,
                                                         coll.find({}, {'a': 1, 'b': 1, 'c': 1}).sort(
                                                             [('a', 1), ('b', -1), ('_id', 1)]).limit(upperlimit));
        if self.selected(2017): self.consume_find_cursor('Project 3, sort on 3     ', 2017,
                                                         coll.find({}, {'a': 1, 'b': 1, 'c': 1}).sort(
                                                             [('a', 1), ('b', 1), ('_id', -1)]).limit(upperlimit));
        if self.selected(2018): self.consume_find_cursor('Project 3, sort on 3     ', 2018,
                                                         coll.find({}, {'a': 1, 'b': 1, 'c': 1}).sort(
                                                             [('a', -1), ('b', -1), ('_id', -1)]).limit(upperlimit));
        if self.selected(2019): self.consume_find_cursor('Project 4, sort on 4     ', 2019,
                                                         coll.find({}, {'a': 1, 'b': 1, 'c': 1, 'd': 1}).sort(
                                                             [('a', 1), ('b', 1), ('c', 1), ('_id', -1)]).limit(
                                                             upperlimit));
        if self.selected(2020): self.consume_find_cursor('Project 4, sort on 4     ', 2020,
                                                         coll.find({}, {'a': 1, 'b': 1, 'c': 1, 'd': 1}).sort(
                                                             [('a', 1), ('b', -1), ('c', -1), ('_id', -1)]).limit(
                                                             upperlimit));
        if self.selected(2021): self.consume_find_cursor('Project 4, sort on 4     ', 2021,
                                                         coll.find({}, {'a': 1, 'b': 1, 'c': 1, 'd': 1}).sort(
                                                             [('a', -1), ('b', 1), ('c', 1), ('_id', 1)]).limit(
                                                             upperlimit));

        if self.selected(2030): self.consume_find_cursor('Sort on string           ', 2030,
                                                         coll.find({}, {'_id': 0, 'd': 1}).sort('d', 1).limit(
                                                             upperlimit));
        if self.selected(2031): self.consume_find_cursor('Sort on string           ', 2031,
                                                         coll.find({}, {'_id': 0, 'd': 1}).sort('d', -1).limit(
                                                             upperlimit));
        if self.selected(2032): self.consume_find_cursor('Sort on int with range   ', 2032,
                                                         coll.find({}, {'_id': 0, 'g': 1}).sort('g', 1).limit(
                                                             upperlimit));
        if self.selected(2033): self.consume_find_cursor('Sort on int with range d ', 2033,
                                                         coll.find({}, {'_id': 0, 'g': 1}).sort('g', -1).limit(
                                                             upperlimit));
        if self.selected(2034): self.consume_find_cursor('Sort on object           ', 2034,
                                                         coll.find({}, {'_id': 0, 'e': 1}).sort('e', 1).limit(
                                                             upperlimit));
        if self.selected(2035): self.consume_find_cursor('Sort on object desc      ', 2035,
                                                         coll.find({}, {'_id': 0, 'e': 1}).sort('e', -1).limit(
                                                             upperlimit));
        if self.selected(2039): self.consume_find_cursor('Sort with regex          ', 2039,
                                                         coll.find({'a': re.compile('01')}, {'_id': 0, 'a': 1}).sort(
                                                             'a', 1).limit(upperlimit));

        if self.selected(2040): self.consume_find_cursor('Sort with skip 1k        ', 2040,
                                                         coll.find({}, {'_id': 0, 'a': 1}).skip(1000).sort('a',
                                                                                                           1).limit(
                                                             upperlimit));
        if self.selected(2041): self.consume_find_cursor('Sort with skip 1k desc   ', 2041,
                                                         coll.find({}, {'_id': 0, 'a': 1}).skip(1000).sort('a',
                                                                                                           -1).limit(
                                                             upperlimit));
        if self.selected(2042): self.consume_find_cursor('Sort with skip 5k        ', 2042,
                                                         coll.find({}, {'_id': 0, 'a': 1}).skip(5000).sort('a',
                                                                                                           1).limit(
                                                             upperlimit));
        if self.selected(2043): self.consume_find_cursor('Sort with skip 5k desc   ', 2043,
                                                         coll.find({}, {'_id': 0, 'a': 1}).skip(5000).sort('a',
                                                                                                           -1).limit(
                                                             upperlimit));

        if self.selected(2050): self.consume_find_cursor('Sort batchsize 200       ', 2050,
                                                         coll.find({}, {'_id': 0, 'a': 1}).batch_size(200).sort('a',
                                                                                                                1).limit(
                                                             upperlimit));
        if self.selected(2051): self.consume_find_cursor('Sort batchsize 200 desc  ', 2051,
                                                         coll.find({}, {'_id': 0, 'a': 1}).batch_size(200).sort('a',
                                                                                                                -1).limit(
                                                             upperlimit));
        if self.selected(2052): self.consume_find_cursor('Sort batchsize 500       ', 2052,
                                                         coll.find({}, {'a': 1, 'b': 1}).batch_size(500).sort(
                                                             [('a', 1), ('_id', 1)]).limit(upperlimit));
        if self.selected(2053): self.consume_find_cursor('Sort batchsize 500       ', 2053,
                                                         coll.find({}, {'a': 1, 'b': 1, 'c': 1}).batch_size(500).sort(
                                                             [('a', 1), ('b', 1), ('_id', 1)]).limit(upperlimit));
        if self.selected(2054): self.consume_find_cursor('Sort batchsize 500 a/desc', 2054,
                                                         coll.find({}, {'a': 1, 'b': 1, 'c': 1}).batch_size(500).sort(
                                                             [('a', -1), ('b', -1), ('_id', 1)]).limit(upperlimit));
        if self.selected(2055): self.consume_find_cursor('Sort batchsize 500 a/desc', 2055,
                                                         coll.find({}, {'a': 1, 'b': 1, 'c': 1}).batch_size(500).sort(
                                                             [('a', 1), ('b', 1), ('c', 1), ('_id', -1)]).limit(
                                                             upperlimit));
        if self.selected(2056): self.consume_find_cursor('Sort skip/limit          ', 2056,
                                                         coll.find({}, {'_id': 0}, skip=1500, limit=10).limit(
                                                             upperlimit).sort('i', 1));
        if self.selected(2057): self.consume_find_cursor('Sort skip/limit          ', 2057,
                                                         coll.find({}, {'_id': 0}, skip=2500, limit=100).limit(
                                                             upperlimit).sort('i', 1));
        if self.selected(2058): self.consume_find_cursor('Sort skip/limit          ', 2058,
                                                         coll.find({}, {'_id': 0}, skip=3500, limit=1000).limit(
                                                             upperlimit).sort('i', 1));

        if self.selected(2060): self.consume_find_cursor('Sort on int              ', 2060,
                                                         coll.find({}, {'_id': 0, 'd': 1}).sort('d', 1).limit(
                                                             upperlimit));
        # SNRD-3995
        #        if self.selected(2061): self.consume_find_cursor('Sort where               ',  2061, coll.find().where('this.c > 0').sort('c',1).limit(upperlimit));
        #        if self.selected(2062): self.consume_find_cursor('Sort complex where       ',  2062, coll.find({},{'a':1,'i':1,'g':1}).where('this.g < 20 && this.i < 1000000').sort('a',1).limit(upperlimit));
        if self.selected(2068): self.consume_find_cursor('Sort maxTimeMS           ', 2068,
                                                         coll.find({}, {'_id': 0, 'e': 1},
                                                                   modifiers={"$maxTimeMS": 5000}).sort('e', 1).limit(
                                                             upperlimit));
        if self.selected(2069): self.consume_find_cursor('Sort comment             ', 2069,
                                                         coll.find({}, {'_id': 0, 'i': 1}).comment("testing").sort('i',
                                                                                                                   1).limit(
                                                             upperlimit));
        if self.selected(2070): self.consume_find_cursor('Sort comment             ', 2070,
                                                         coll.find({}, {'_id': 0, 'i': 1}).sort('i', 1).comment(
                                                             "testing").limit(upperlimit));
        #        if self.selected(2071): self.consume_find_cursor('Sort where after sort    ',  2071, coll.find().sort('c',1).where('this.c > 1000000').limit(upperlimit));

        # SNRD-3999
        #        if self.selected(2072): self.consume_find_cursor('Sort where after sort    ',  2072, coll.find().sort('c',1).where('this.c < 1000000').limit(upperlimit));
        #        if self.selected(2073): self.consume_find_cursor('Sort where before sort   ',  2073, coll.find().where('this.c < 1000000').sort('c',1).limit(upperlimit));

        if self.selected(2080): self.consume_find_cursor('find few                 ', 2080,
                                                         coll.find({'a': {'$gt': 11110000}}, {'_id': 0, 'a': 1}).sort(
                                                             'a', 1).limit(upperlimit));
        if self.selected(2081): self.consume_find_cursor('find many                ', 2081,
                                                         coll.find({'g': {'$gt': 4}}, {'_id': 0, 'g': 1}).sort('g',
                                                                                                               1).limit(
                                                             upperlimit));
        if self.selected(2082): self.consume_find_cursor('find complex             ', 2082,
                                                         coll.find({'g': {'$gte': 5}, 'h': {'$lte': 50}},
                                                                   {'_id': 0, 'g': 1}).sort('g', 1).limit(upperlimit));

        coll = self.getColl("samb")
        if self.selected(2200): self.consume_find_cursor('Sort on boolean          ', 2200,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort('a', 1).limit(
                                                             upperlimit));
        if self.selected(2201): self.consume_find_cursor('Sort on boolean desc     ', 2201,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort('a', -1).limit(
                                                             upperlimit));

        coll = self.getColl("samo")
        if self.selected(2202): self.consume_find_cursor('Sort on object           ', 2202,
                                                         coll.find({}, {'_id': 0, 'e': 1}).sort('e', 1).limit(
                                                             upperlimit));
        if self.selected(2203): self.consume_find_cursor('Sort on object desc      ', 2203,
                                                         coll.find({}, {'_id': 0, 'e': 1}).sort('e', -1).limit(
                                                             upperlimit));

        coll = self.getColl("samstr")
        if self.selected(2204): self.consume_find_cursor('Sort on int              ', 2204,
                                                         coll.find({}, {'_id': 0, 'b': 1}).sort('b', 1).limit(
                                                             upperlimit));
        if self.selected(2205): self.consume_find_cursor('Sort on int desc         ', 2205,
                                                         coll.find({}, {'_id': 0, 'b': 1}).sort('b', -1).limit(
                                                             upperlimit));

        coll = self.getColl("samrex")
        if self.selected(2206): self.consume_find_cursor('Sort on regex            ', 2206,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort('a', 1).limit(
                                                             upperlimit));
        if self.selected(2207): self.consume_find_cursor('Sort on regex desc       ', 2207,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort('a', -1).limit(
                                                             upperlimit));

        coll = self.getColl("sambin")
        if self.selected(2208): self.consume_find_cursor('Sort on binData          ', 2208,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort('a', 1).limit(
                                                             upperlimit));
        if self.selected(2209): self.consume_find_cursor('Sort on binData desc     ', 2209,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort('a', -1).limit(
                                                             upperlimit));

        coll = self.getColl("samatz")
        if self.selected(2210): self.consume_find_cursor('Sort on timestamp        ', 2210,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort('a', 1).limit(
                                                             upperlimit));
        if self.selected(2211): self.consume_find_cursor('Sort on timestamp desc   ', 2211,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort('a', -1).limit(
                                                             upperlimit));

        coll = self.getColl("samasz")
        if self.selected(2212): self.consume_find_cursor('Sort on [string]         ', 2212,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort('a', 1).limit(
                                                             upperlimit));
        if self.selected(2213): self.consume_find_cursor('Sort on [string] desc    ', 2213,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort('a', -1).limit(
                                                             upperlimit));

        coll = self.getColl("samarex")
        if self.selected(2214): self.consume_find_cursor('Sort on [regex]          ', 2214,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort('a', 1).limit(
                                                             upperlimit));
        if self.selected(2215): self.consume_find_cursor('Sort on [regex] desc     ', 2215,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort('a', -1).limit(
                                                             upperlimit));

        coll = self.getColl("samatz")
        if self.selected(2216): self.consume_find_cursor('Sort on [timestamp]      ', 2216,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort('a', 1).limit(
                                                             upperlimit));
        if self.selected(2217): self.consume_find_cursor('Sort on [timestamp] desc ', 2217,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort('a', -1).limit(
                                                             upperlimit));

        coll = self.getColl("samaob")
        if self.selected(2218): self.consume_find_cursor('Sort on [object]         ', 2218,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort('a', 1).limit(
                                                             upperlimit));
        if self.selected(2219): self.consume_find_cursor('Sort on [object] desc    ', 2219,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort('a', -1).limit(
                                                             upperlimit));

        coll = self.getColl("samamix")
        if self.selected(2220): self.consume_find_cursor('Sort on [mixed type]     ', 2220,
                                                         coll.find({}, {'a': 1, '_id': 1}).sort(
                                                             [('a', 1), ('_id', 1)]).limit(upperlimit));
        if self.selected(2221): self.consume_find_cursor('Sort on [mixed type] desc', 2221,
                                                         coll.find({}, {'a': 1, '_id': 1}).sort(
                                                             [('a', -1), ('_id', 1)]).limit(upperlimit));

        coll = self.getColl("samaob")
        if self.selected(2222): self.consume_find_cursor('Sort with elemMatch      ', 2222,
                                                         coll.find({'a': {'$elemMatch': {'x': {'$gt': 'abc'}}}},
                                                                   {'_id': 0, 'a': 1}).sort('a', 1).limit(upperlimit));
        if self.selected(2223): self.consume_find_cursor('Sort with slice          ', 2223,
                                                         coll.find({}, {'_id': 0, 'a': {'$slice': 3}}).sort('i',
                                                                                                            1).limit(
                                                             upperlimit));

        coll = self.getColl("samsz_collation")
        if self.selected(2300): self.consume_find_cursor('Sorting Collation 1      ', 2300,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort('a', 1).limit(
                                                             upperlimit).collation(col1));
        if self.selected(2301): self.consume_find_cursor('Sorting Collation 2      ', 2301,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort('a', 1).limit(
                                                             upperlimit).collation(col2));
        if self.selected(2302): self.consume_find_cursor('Sorting Collation 3      ', 2302,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort('a', 1).limit(
                                                             upperlimit).collation(col3));
        if self.selected(2303): self.consume_find_cursor('Sorting Collation 4      ', 2303,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col4));
        if self.selected(2304): self.consume_find_cursor('Sorting Collation 5      ', 2304,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', -1), ('i', -1)]).limit(upperlimit).collation(col5));
        if self.selected(2305): self.consume_find_cursor('Sorting Collation 6      ', 2305,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col6));
        if self.selected(2306): self.consume_find_cursor('Sorting Collation 7      ', 2306,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', -1), ('i', 1)]).limit(upperlimit).collation(col7));
        if self.selected(2307): self.consume_find_cursor('Sorting Collation 8      ', 2307,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', -1)]).limit(upperlimit).collation(col8));
        if self.selected(2308): self.consume_find_cursor('Sorting Collation 9      ', 2308,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col9));
        if self.selected(2309): self.consume_find_cursor('Sorting Collation 10     ', 2309,
                                                         coll.find({'i': {'$gt': 0}}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col10));
        if self.selected(2310): self.consume_find_cursor('Sorting Collation 11     ', 2310,
                                                         coll.find({'g': {'$gt': 2}}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', -1), ('i', 1)]).limit(upperlimit).collation(col11));
        if self.selected(2311): self.consume_find_cursor('Sorting Collation 12     ', 2311,
                                                         coll.find({'g': {'$gt': 1}}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', -1), ('i', -1)]).limit(upperlimit).collation(
                                                             col12));

        coll = self.getColl("samasz_collation")
        if self.selected(2400): self.consume_find_cursor('Sorting Collation 1      ', 2400,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col1));
        if self.selected(2401): self.consume_find_cursor('Sorting Collation 2      ', 2401,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col2));
        if self.selected(2402): self.consume_find_cursor('Sorting Collation 3      ', 2402,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col3));
        if self.selected(2403): self.consume_find_cursor('Sorting Collation 4      ', 2403,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col4));
        if self.selected(2404): self.consume_find_cursor('Sorting Collation 5      ', 2404,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', -1), ('i', -1)]).limit(upperlimit).collation(col5));
        if self.selected(2405): self.consume_find_cursor('Sorting Collation 6      ', 2405,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col6));
        if self.selected(2406): self.consume_find_cursor('Sorting Collation 7      ', 2406,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', -1), ('i', 1)]).limit(upperlimit).collation(col7));
        if self.selected(2407): self.consume_find_cursor('Sorting Collation 8      ', 2407,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', -1)]).limit(upperlimit).collation(col8));
        if self.selected(2408): self.consume_find_cursor('Sorting Collation 9      ', 2408,
                                                         coll.find({}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col9));
        if self.selected(2409): self.consume_find_cursor('Sorting Collation 10     ', 2409,
                                                         coll.find({'i': {'$gt': 0}}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', 1), ('i', 1)]).limit(upperlimit).collation(col10));
        if self.selected(2410): self.consume_find_cursor('Sorting Collation 11     ', 2410,
                                                         coll.find({'g': {'$gt': 2}}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', -1), ('i', 1)]).limit(upperlimit).collation(col11));
        if self.selected(2411): self.consume_find_cursor('Sorting Collation 12     ', 2411,
                                                         coll.find({'g': {'$gt': 1}}, {'_id': 0, 'a': 1}).sort(
                                                             [('a', -1), ('i', -1)]).limit(upperlimit).collation(
                                                             col12));

    @classmethod
    def run_test(self):
        self.testDate = datetime.datetime.now()
        printLn("")
        printLn("SORT Load Test %s" % (iterationNumberToString(upperlimit)))
        print
        self.record_results = self.dbconnection.sort_results.results;
        skipped = 0
        self.sonarVersion = 0
        self.skippedColumns = ""
        now = datetime.datetime.now()
        self.class_to_db = ["none", "sam", "citi", "gdm3", "gdm3"]
        printLn("Sorting %s Rows with %d loops %s" % (iterationNumberToString(iterations), loops, self.vm()))
        for c in range(len(self.class_to_db)):
            self.database = self.class_to_db[c]
            if 0 == self.sonarVersion:
                self.sonarVersion = self.sonar_version(self.database)
                if self.sonarVersion >= 1.4 and 0 == args.unauth:
                    self.set_block_allocation_size_percentage(50)
            for i in range(len(data)):
                if exitReceived == 1:
                    break
                self.get_data(i, data)
                if c != self.c:
                    continue
                # If exclude version is set, then exclude that version
                if self.starting_version != 99 and self.starting_version != 0 and self.sonarVersion != self.starting_version:
                    deb2("skipping version: %8.1f %8.1f" % (self.starting_version, self.sonarVersion))
                    continue
                if self.starting_version == 99 and args.distributed:
                    deb2("Skipping non-dist test: %d" % (self.t))
                    continue
                if global_iterations > 10000000 and c != 3:
                    deb("Outside range: %d" % self.t);
                    continue
                if (self.t >= args.start and self.t <= args.end) and (len(tests) == 0 or self.ts in tests):
                    if args.distributed:
                        (duration, durationDist, rate, status, statusDist, passed,
                         workersPassed) = self.run_distributed_test()
                        deb("Duration: %8.2f durationDist: %8.2f rate: %d status: %s passed: %d workersPassed: %d" % (
                        duration, durationDist, rate, status, passed, workersPassed))
                    else:
                        durationDist = -1
                        workersPassed = -1
                        (duration, rate, status, passed) = self.run_individual_test()
                    deb("individual result: %8.2f %d %d" % (rate, duration, passed))
                    repeats = 0
                    if 0 == showQuery:
                        self.test = ""
                    now = datetime.datetime.now()
                    if not baselineExists(self.t):
                        status = "NO_BASELINE"
                        passed = False
                    if args.distributed:
                        deb("passed: %d workersPassed: %d status: %s" % (passed, workersPassed, status))
                        if passed and not workersPassed:
                            status = "FAILED_WORKERS"
                        printLn("%s %4d %-35s %d %12.3fk/s %8.0fms %8.0fms %4.0f%% %s %s/%s %s" % (
                        global_iterations_str, self.t, self.desc, repeats, rate, duration, durationDist,
                        (int(durationDist) * 100 / duration), self.test, status, statusDist, self.vm()))
                        try:
                            self.record_results.insert(
                                {'testDate': self.testDate, 'dist': 0, 'iterations': global_iterations, 'date': now,
                                 'testNumber': self.t, 'testName': data[i][2], 'collection': data[i][3],
                                 'duration': duration, 'rate': rate, 'status': status});
                            self.record_results.insert(
                                {'testDate': self.testDate, 'dist': 1, 'iterations': global_iterations, 'date': now,
                                 'testNumber': self.t, 'testName': data[i][2], 'collection': data[i][3],
                                 'duration': durationDist, 'status': status});
                        except Exception as e:
                            printLn("Lost DB Connection 9: %s" % e.message)
                            pass
                        if args.removeSuccessfulResults and passed and workersPassed:
                            cmd("rm -f %s" % (self.result_file_name))
                    else:
                        printLn("%s %4d %-35s %d %12.3fk/s %8.0fms  %s %s %s" % (
                        global_iterations_str, self.t, self.desc, repeats, rate, duration, self.test, status,
                        self.vm()))
                        try:
                            self.record_results.insert(
                                {'testDate': self.testDate, 'dist': 0, 'iterations': global_iterations, 'date': now,
                                 'testNumber': self.t, 'testName': data[i][2], 'collection': data[i][3],
                                 'duration': duration, 'rate': rate, 'status': status});
                        except Exception as e:
                            printLn("Lost DB Connection 10: %s" % e.message)
                            pass
                        if args.removeSuccessfulResults and passed:
                            cmd("rm -f %s" % (self.result_file_name))
                    sys.stdout.flush()
                    logResult(now, self.t, duration, rate, durationDist)
                    time.sleep(args.wait)
        self.execute_find_query(args.distributed);
        if skipped:
            printLn("Skipped: %d %s" % (skipped, skippedColumns))
        if args.queries:
            printLn("Queries: ")
            for i in range(len(data)):
                if i >= Start and i <= End:
                    printLn("%s %s Query: db.%s.aggregate(%s)" % (data[i][0], data[i][1], data[i][2], data[i][3]))


if __name__ == '__main__':
    SORT_TEST.setUpClass()
    SORT_TEST.run_test()
    sys.exit(1 if gFailures > 0 else 0)