#!/bin/env bash

today_date=`date -ud "now" "+%Y-%m-%d"`
target_machine_logdir='/data/sonar/daily_report/logs'
ganitor_logdir='/home/qa/today_reports'
logname="top24.$today_date"
from_address='andrew@jsonar.com'
email_subject="Top Ten updates, aggregates, inserts on "

machines=($@)


send_mail(){
  for machine in "${machines[@]}"
  do
    echo "$(date -u +%FT%T.%3NZ) - Sending email of Top 10 queries from $machine"
    if [ "$machine" = big4-google ]; then
      scp "big4-google:$target_machine_logdir/$logname" "$ganitor_logdir/$machine.$logname"
      cat "$ganitor_logdir/$machine.$logname" | mail -r "$from_address" -s "$email_subject $machine" "andrew@jsonar.com"
    fi
  done
  echo "$(date -u +%FT%T.%3NZ) - done"
}

send_mail