#!/usr/bin/env bash

JSONAR_HOME='/data/tarball-sonar/jsonar'
SONARD_HOME="$JSONAR_HOME/apps/4.2.a/bin/sonard"
SONARGD_HOME="$JSONAR_HOME/apps/4.2.a/bin/sonargd"
VERSIONS_HOME="$JSONAR_HOME/apps/4.2.a/versions"
SONARW_LOG_HOME="$JSONAR_HOME/logs/sonarw"
SONARGD_LOG_HOME="$JSONAR_HOME/logs/sonargd"
REPLICATION_LOG_HOME="$JSONAR_HOME/logs/sonarw"
GATEWAY_LOG_HOME=
KIBANA_LOG_HOME="$JSONAR_HOME/logs"
SONARFINDER_LOG_HOME="$JSONAR_HOME/apps/4.2.a/sonarfinder/sonarFinder/logs"
CATALINA_LOG_HOME="$JSONAR_HOME/logs/sonarfinder"
DISPATCHER_LOG_HOME="$JSONAR_HOME/logs/dispatcher"
SONARD_DATA="$JSONAR_HOME/data/sonarw"
SONARGD_DATA="/data/sonar/sonargd"

SCRIPT_DIR="/data/sonar/daily_report";
LOGS_HOME="$SCRIPT_DIR/logs";
