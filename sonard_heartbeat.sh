#/bin/bash

#*************************************************************
# Prints heartbeat to sonard.log every X minutse             *
# To be run with crontab (user: sonarw) i.e(every 5 minutes):*
# */5 * * * * /data/sonar/daily_report/sonard_heartbeat.sh   *
#*************************************************************

source /etc/sysconfig/jsonar

SONAR_HOME="$1";

if [ -z "$SONAR_HOME" ]; then
        SONAR_HOME="$JSONAR_LOGDIR";
fi

echo `date -d "now" "+%FT%T.%6N"` - heartbeat >> "$SONAR_HOME/sonarw/sonard.log";