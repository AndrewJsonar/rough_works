#!/bin/bash

DATE_TODAY=$(date -ud "now" "+%Y%m%d")
baseDIR=$PWD
outDIR="output"
inLOG="sonarw.$DATE_TODAY.log"
outCSV="sonarw-long_queries.$DATE_TODAY.csv"
outJSON="sonarw-long_queries.$DATE_TODAY.json"

###############################################

if [ ! -d $outDIR ] ; then
        echo 'Directory' $outDIR 'not found. Creating...'
        mkdir $outDIR
fi

if [ -e "$outDIR"/"$outCSV" ] ; then
        rm "$outDIR"/"$outCSV"
fi

if [ -e "$outDIR"/"$outJSON" ]; then
        rm "$outDIR"/"$outJSON"
fi

###############################################

echo $(date -u +%FT%T.%3NZ) - BEGIN TO SCAN $inLOG
echo $(date -u +%FT%T.%3NZ) - parse_log.sh $outDIR $inLOG $outCSV
. parse_log.sh $outDIR $inLOG $outCSV

echo $(date -u +%FT%T.%3NZ) - CONVERT CSV TO JSON
echo $(date -u +%FT%T.%3NZ) - python3 parse_csv.py $outDIR $outCSV $outJSON
python3 parse_csv.py $outDIR $outCSV $outJSON

echo $(date -u +%FT%T.%3NZ) - SCANNING COMPLETE

###############################################

###############################################

echo $(date -u +%FT%T.%3NZ) '- DONE'
