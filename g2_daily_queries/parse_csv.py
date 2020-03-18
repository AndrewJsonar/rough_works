# Parses a CSV into JSON format. File and field names are hard-coded.
# Author: Andy Boyle, http://www.andymboyle.com/2011/11/02/quick-csv-to-json-parser-in-python/
# Updated By: Shawn Thai
import sys
import csv
import json
import datetime
import pymongo

MAX_ARRAY_SIZE = 10
TARGET_RUN_TIME = 3
NO_QUERY_TEXT = "There were no {} over {} seconds.\n\n"
QUERY_TEXT = "{} over {} seconds: \n"

#Returns the run_time value
def getRunTime(obj):
    return obj['run_sec']

#Checks if the array has less than MAX_SIZE, otherwise compares the last value
def appendRow(array, row):
    if len(array) < MAX_ARRAY_SIZE:
        array.append(row)
    else:
        if(getRunTime(array[MAX_ARRAY_SIZE-1]) < getRunTime(row)):
            array[MAX_ARRAY_SIZE-1] = row
    array.sort(key=getRunTime)

#Change format of row
#def makeRowPretty(row):

if len(sys.argv) < 4:
    print('ERROR: parse_json.csv requires 3 arguments')
    print('ERROR: Please specify output directory, input filename, and output filename')
    sys.exit('Exiting...')

outputDir = sys.argv[1] + '/'
inputFile = sys.argv[2]
outputFile = sys.argv[3]

# Open the CSV
f = open( (outputDir + inputFile), 'rU' )

# Change each fieldname to the appropriate field name.
reader = csv.DictReader( f, fieldnames = ("timestamp","tid", "opid", "database", "collection", "run_sec", "type", "query"), dialect='excel-tab')

# Skip header
next(f)

# Parse the CSV into JSON
insert = []
update = []
remove = []

for row in reader:
    if(float(row["run_sec"]) > TARGET_RUN_TIME):
        type = (row["type"])
        if(type == "insert"):
            appendRow(insert, row)
        elif(type == "update"):
            appendRow(update, row)
                    elif(type == "remove"):
            appendRow(remove, row)

# Save the JSON
f = open( (outputDir + outputFile), 'w')
if len(insert) == 0:
    f.write(NO_QUERY_TEXT.format("Inserts"))
else:
    f.write(QUERY_TEXT.format("Inserts", TARGET_RUN_TIME))
    for row in insert:
        f.write(str(row))
        f.write("\n")
    f.write("\n")

if len(update) == 0:
    f.write(NO_QUERY_TEXT.format("Updates"))
else:
    f.write(QUERY_TEXT.format("Updates", TARGET_RUN_TIME))
    for row in update:
        f.write(str(row))
        f.write("\n")
    f.write("\n")

if len(remove) == 0:
    f.write(NO_QUERY_TEXT.format("Removes"))
else:
    f.write(QUERY_TEXT.format("Removes", TARGET_RUN_TIME))
    for row in remove:
        f.write(str(row))
        f.write("\n")
    f.write("\n")
