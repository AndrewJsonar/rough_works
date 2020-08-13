#!/usr/bin/env bash

source $(dirname "$0")/functions.sh
source /etc/sysconfig/jsonar

values["hostname"]="\"`cat /etc/hostname`\"";
values["test_starting_time"]="\"`date -ud "now" "+%Y-%m-%dT%T"`\"";
DATE_ONLY=`date -ud "now" "+%Y-%m-%d"`;
DATE_YESTERDAY=`date -ud "yesterday" "+%Y%m%d%H%M%S"`
URI="$1";
SCRIPT_DIR="/data/sonar/daily_report";
LOGS_HOME="$SCRIPT_DIR/logs";
MONGO_LOG="$LOGS_HOME/mongo_stats_$DATE_ONLY.json";

exec 3>&1 4>&2
trap 'exec 2>&4 1>&3' 0 1 2 3
exec 1>"$LOGS_HOME/daily_report_$DATE_ONLY.log" 2>&1

main(){

	logger "Log interval(UTC): ${values[test_starting_time]} - $DATE_YESTERDAY"

	#RUNNING MONGO STATS
	logger "Running mongo stats..."
	mongo "$URI" "$SCRIPT_DIR/long_mongo_stats.js" > "$MONGO_LOG"
	get_data_from_mongo_stats;

	logger "Running general machine stats..."
	get_all_general_machine_stats;

	logger "Running logs stats..."
	get_all_logs_stats;

	printf '%s\n' `grep -E "new_feed_count" "$MONGO_LOG"` | paste -sd ',';

	print_long_csv > "$LOGS_HOME/daily_report_$DATE_ONLY.csv";
	print_json > "$LOGS_HOME/daily_report_$DATE_ONLY.json";

	logger "Done." >> "$LOGS_HOME/daily_report_$DATE_ONLY.log";
}

main;