#!/bin/env bash

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
outDIR="$parent_path/output"
inLOG1="/data/sonar/sonarw/log/sonarw.log"
inLOG2="/data/sonar/sonarw/log/sonarw.log.1"
inLOG3="/data/sonar/sonarw/log/sonarw.log.2"
outLOG="relevent.log"
inserts="sonarw-long_queries_inserts.json"
updates="sonarw-long_queries_updates.json"
queries="sonarw-long_queries_queries.json"
recepients="andrew@jsonar.com"

###############################################

if [ -e "$outDIR"/"$inserts" ]; then
        rm "$outDIR"/"$inserts"
fi

if [ -e "$outDIR"/"$updates" ]; then
        rm "$outDIR"/"$updates"
fi

if [ -e "$outDIR"/"$queries" ]; then
        rm "$outDIR"/"$queries"
fi

###############################################

echo $(date -u +%FT%T.%3NZ) - GET SONARW LOGS
echo $(date -u +%FT%T.%3NZ) - $parent_path/parse_log.sh $outDIR $outLOG $inLOG1 $inLOG2 $inLOG3
"$parent_path/parse_log.sh $outDIR $outLOG $inLOG1 $inLOG2 $inLOG3"

echo $(date -u +%FT%T.%3NZ) - PARSE SONARW LOGS
echo $(date -u +%FT%T.%3NZ) - python3 $parent_path/parse_csv.py $outDIR $outLOG $inserts $updates $queries
python3 "$parent_path/parse_csv.py $outDIR $outLOG $inserts $updates $queries"

echo $(date -u +%FT%T.%3NZ) - SCANNING COMPLETE

###############################################

#echo $(date -u +%FT%T.%3NZ) - SENDING EMAIL
#python3 "$parent_path/email_sender.py --inserts $outDIR/$inserts --updates $outDIR/$updates --queries $outDIR/$queries --recipients $recepients --subject 'Top ten queries from G2'"

###############################################

echo $(date -u +%FT%T.%3NZ) '- DONE'
