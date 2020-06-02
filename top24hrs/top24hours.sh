#!/bin/env bash

source /home/qa/.bashrc

machine_name="$1"
SONAR_LOGDIR="$2"
DAY="$3"
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
today_date=`date -ud "now" "+%Y-%m-%d"`
top24_log="$parent_path/logs/top24.$today_date"

declare -a queries=("update" "aggregate" "insert");


if [ -z "$SONAR_LOGDIR" ]; then
  SONAR_LOGDIR="$JSONAR_LOGDIR/sonarw";
fi


if [ -z "$DAY" ]; then
  DAY=`date -ud "-1 day" "+%d"`;
fi


grab_relevent_day(){
  type=$1
  if [ "$type" = "aggregate" ]; then
    grep -E "^([0-9]{4})-([0-9]{2})-($DAY)T" "$SONAR_LOGDIR"/sonarw* | grep "aggregate" | awk '{if ($NF-0 > 3000) print $NF, $0}' | sort -n | tail -1000 > "$parent_path/logs/tmp_$type"
  elif [ "$type" = "update" ]; then
    grep -E "^([0-9]{4})-([0-9]{2})-($DAY)T" "$SONAR_LOGDIR"/sonarw* | grep " update " | awk '{if ($NF-0 > 3000) print $NF, $9, $10, $11, $12, $14, $16, $18}' | sort -n | tail -1000 > "$parent_path/logs/tmp_$type"
  elif [ "$type" = "insert" ]; then
    grep -E "^([0-9]{4})-([0-9]{2})-($DAY)T" "$SONAR_LOGDIR"/sonarw* | grep " insert " | awk '{if ($NF-0 > 3000) print $NF, $9, $10, $12}' | sort -n | tail -1000 > "$parent_path/logs/tmp_$type"
  fi
}


grab_single_query(){
  type=$1
  opid=$2
  grep "$1" "$SONAR_LOGDIR"/queries* | grep "opid: $2 " | awk '{ for (i=8;i<=NF;i++) print $i }' ORS=" "
}


get_opid(){
  log=$1
  tail -1 "$log" | awk '{print $(NF-13)}' | awk -F ':' '{print $2}'
}


get_runtime(){
  log=$1
  tail -1 "$log" | awk '{print $1}'
}


get_simplified_line(){
  type=$1
  log=$2
  if [ "$type" = "aggregate" ]; then
    tail -1 "$2" | awk '{ for (i=(NF-17);i<=(NF-14);i++) print $i }' ORS=" "
  elif [ "$type" = "update" ]; then
    tail -1 "$2" | awk '{print $3, $4}'
  elif [ "$type" = "insert" ]; then
    tail -1 "$2" | awk '{print $3}'
  fi
}


remove_line_from_log(){
  log=$1
  line=$2
  grep -v "$line" "$log" > "$log.buffer"
  rm "$log"
  mv "$log.buffer" "$log"
}


create_simple_list(){
  query_type=$1
  grab_relevent_day "$query_type"
  queries_log="$parent_path/logs/tmp_$query_type"

  echo "Top $query_type of the past 24hrs" >> "$top24_log"
  for line in `seq 1 10`
  do
    tail=`tail -1 "$queries_log"`
    if [ -n "$tail" ]; then
      if [ $query_type == "aggregate" ]; then
        opid=`get_opid "$queries_log"`
        agg_query=`grab_single_query "$query_type" "$opid"`
        echo "#$line: `get_runtime "$queries_log"` $agg_query" >> "$top24_log"
        echo "" >> "$top24_log"
      else
        echo "#$line: $tail" >> "$top24_log"
      fi
      comparison_line=`get_simplified_line "$query_type" "$queries_log"`
      remove_line_from_log "$queries_log" "$comparison_line"
    elif [ "$line" == 1 ]; then
      echo "No $query_type in the past 24hrs over 3 seconds" >> "$top24_log"
    else
      break
    fi
  done
  echo "" >> "$top24_log"
}


main(){
  for query_type in "${queries[@]}"
  do
    create_simple_list "$query_type"
  done
}

main