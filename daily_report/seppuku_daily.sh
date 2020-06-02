#!/usr/bin/env bash

source $(dirname "$0")/functions.sh

HOSTNAME=`cat /etc/hostname`;
FULL_DATE=`date -ud "now" "+%Y-%m-%dT%T"`;
DATE_ONLY=`date -ud "now" "+%Y-%m-%d"`;
DATE_YESTERDAY=`date -ud "yesterday" "+%Y-%m-%d%T" | tr -d "-" |tr -d ":" | cut -c1-14`;
HOME_PATH="/local/raid0/seppuku-test";
LOGS_HOME="$HOME_PATH/daily_report/logs";

exec 3>&1 4>&2
trap 'exec 2>&4 1>&3' 0 1 2 3
exec 1>"$LOGS_HOME/daily_report_$DATE_ONLY.log" 2>&1

start_day=`date -u | cut -c1-10`

((seppuku_append=seppuku_check=seppuku_create=
  seppuku_rename=seppuku_removeseppuku_truncate=0));

pid_age=TOO-OLD!;
#seppuku_truncate=`awk "/$start_day/,0" /local/raid0/seppuku-test/sonard.log | grep "needs block recovery." | wc -l`;
last_line_time=`grep "$start_day" /local/raid0/seppuku-test/sonard.log | tail -n1` | awk '{print $4}';
sonar_version=`"$HOME_PATH/sonard-seppuku" --version | grep SonarW | awk '{print $3}'`;
last_modified=`stat /local/raid0/seppuku-test/sonarw-home/sonard.pid | grep Modify | awk '{print $2" "$3}'`;
modified_seconds=`date --date="$last_modified" +%s`;
seconds_now=`date --date="now" +%s`;
let "tDiff=$seconds_now-$modified_seconds";
if [ "$tDiff" -lt 7200 ];then
	pid_age=OK;
fi

get_stats_from_log "$HOME_PATH"/sonarw-home/log/sonarw.log 0 seppuku;

exec 1>"$LOGS_HOME/daily_report_$DATE_ONLY.csv"

printf '%s\n' seppuku "$FULL_DATE" "$seppuku_append" "$seppuku_check" "$seppuku_create" "$seppuku_rename" "$seppuku_remove" "$seppuku_truncate" "$pid_age" "$sonar_version" "$seppuku_errors"| paste -sd ',' ;
