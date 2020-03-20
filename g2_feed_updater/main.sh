#!/usr/bin/env bash
# Andrew Ebl Mar 20 2020

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
number_of_args=2
connection_string=$1
database=$2

help_function() {
  echo "Argument 1 is the connection string, example: 'mongodb://user:password@localhost:27117/admin'"
  echo "Argument 2 is the database to use"
}

if [ "$#" -ne "$number_of_args" ]; then
  echo "needs $number_of_args arguemnts"
  help_function
  exit
fi

echo $(date -u +%FT%T.%3NZ) - Starting reformater:
python parent_path/reformater.py "$1" "$2"

echo $(date -u +%FT%T.%3NZ) - Done!