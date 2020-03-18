#!/usr/bin/env python3
# Parses a CSV into JSON format. File and field names are hard-coded.
# Author: Andy Boyle, http://www.andymboyle.com/2011/11/02/quick-csv-to-json-parser-in-python/
# Updated By: Andrew Ebl

import sys
import csv
import json
from datetime import datetime
import re

DOCUMENT_LIMIT = 10000
insert = []
update = []
query = []
MAX_ARRAY_SIZE = 10
TARGET_RUN_TIME = 3000
REGEX = '(?P<date_ran>[\d\-.T:+]*)\s(\w)\s(?P<opid>[\d]*)\s([\w\W]*):\s(?P<type>[\w]*)\s(?P<database>[\w\w]*).(?P<collection>[\w\W]*)\s(command:|filter:)(?P<query>[\w\W]*)\sop:([\w\W]*)(w:|reslen:)([\d]*)\s(?P<run_time>\d*)ms'
#group 1: Datetime Ran
#group 4: OPID
#group 6: query type
#group 7: database
#group 8: collection
#group 9: query
#group 22: run time

def get_run_time(obj):
    return obj.group('run_time')

#Checks if the array has less than MAX_SIZE, otherwise compares the last value
def append_row(array, line):
    if len(array) < 1:
        array.append(line)
    elif check_for_duplicate(array, line):
            if len(array) < (MAX_ARRAY_SIZE):
                array.append(line)
            else:
                if is_bigger(array[MAX_ARRAY_SIZE-1], line):
                    array[MAX_ARRAY_SIZE-1] = line
    array.sort(key=(get_run_time), reverse=True)

def check_run_time(line_run_time):
    if int(line_run_time) >= TARGET_RUN_TIME:
        return True

def is_bigger(line_one, line_two):
    if line_one.group('run_time') < line_two.group('run_time'):
        return True

def check_for_duplicate(array, line):
    result = True
    for document in array:
        if line.group('query') == document.group('query'):
            result = False
    return result

def parse_log(filename, log_pattern):
    with open(filename) as f:
        for line in f:
            match = log_pattern.search(str(line))
                        if not match:
                continue
            if check_run_time(match.group('run_time')):
                if match.group('type') == 'insert':
                    append_row(insert, match)
                if match.group('type') == 'update':
                    append_row(update, match)
                if match.group('type') == 'query':
                    append_row(query, match)

def write_list_to_file(out_dir, out_file, list_name):
    with open((out_dir + out_file), 'w') as file:
        for document in list_name:
            file.write(document.group('date_ran'))
            file.write(", Opid: ")
            file.write(document.group('opid'))
            file.write(", Type: ")
            file.write(document.group('type'))
            file.write(", Database: ")
            file.write(document.group('database'))
            file.write(", Collection: ")
            file.write(document.group('collection'))
            file.write(", Query: ")
            file.write(document.group('query'))
            file.write(", Run Time: ")
            file.write(document.group('run_time'))
            file.write("ms\n")

def main():
    output_dir = sys.argv[1] + '/'
    input_file = output_dir + sys.argv[2]
    output_inserts = sys.argv[3]
    output_updates = sys.argv[4]
    output_queries = sys.argv[5]

    log_pattern = re.compile(REGEX)

    parse_log(input_file, log_pattern)

    write_list_to_file(output_dir, output_inserts, insert)
    write_list_to_file(output_dir, output_queries, query)
    write_list_to_file(output_dir, output_updates, update)

if __name__ == '__main__':
    main()
