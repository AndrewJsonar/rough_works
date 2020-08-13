#!/usr/bin/env bash

#*************************************************************
# Prints heartbeat to sonard.log every X minutse             *
# To be run with crontab (user: sonarw) i.e(every 5 minutes):*
# */5 * * * * /data/sonar/daily_report/sonard_heartbeat.sh   *
#*************************************************************

SONAR_HOME="$1";

if [ -z "$SONAR_HOME" ]; then
        SONAR_HOME="/var/lib/sonarw/";
fi

echo `date -ud "now" "+%Y-%m-%dT%T"` - heartbeat >> "$SONAR_HOME/log/sonard.log";