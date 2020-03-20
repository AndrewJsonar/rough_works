#!/usr/bin/env bash
# Andrew Ebl Mar 20 2020

NUMBER_OF_ARGS=2
CONNECTION_STRING=$1
DATABASE=$2

help_function() {
  echo "Argument 1 is connection string"
  echo "Argument 2 is database to use"
}

if [ "$#" -ne "$NUMBER_OF_ARGS" ]; then
  echo "needs $NUMBER_OF_ARGS arguemnts"
  help_function
fi

