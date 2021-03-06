#!/usr/bin/env bash
# Andrew Ebl Mar 20 2020

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
number_of_args=3
connection_string=$1
database=$2
collection=$3

help_function() {
  echo "needs $number_of_args arguments"
  echo 'Usage: $1 connection string, example: mongodb://user:password@localhost:27117/admin'
  echo 'Usage: $2 database'
  echo 'Usage: $3 collection'
}

if [ "$#" -ne "$number_of_args" ]; then
  help_function
  exit
fi

echo $(date -u +%FT%T.%3NZ) - Starting reformator:
python3 "$parent_path"/reformator.py "$1" "$2" "$3"

echo $(date -u +%FT%T.%3NZ) - Done!