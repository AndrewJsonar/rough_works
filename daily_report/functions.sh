#!/usr/bin/env bash

source $(dirname "$0")/utils.sh
source $(dirname "$0")/sonar_paths.sh

declare -A values;
declare -a sonargd_collections=( session instance full_sql exception policy_violations subarray_object_feed)
declare -a sections=(GENERAL VERSIONS UPTIME LOGS REPLICATION DATA_AND_RAM FEEDS GP OPCOUNTERS COLLECTIONS_COUNT PURGE UEBA_COUNTS)
declare -a GENERAL=(test_starting_time hostname)
declare -a VERSIONS=(sonarw.version sonarg.version sonarfinder.version)
declare -a UPTIME=(sonard.uptime.seconds sonargd.uptime.seconds sonarfinder.uptime.seconds)
declare -a REPLICATION=(replication.started.count replication.completed.count replication.done.with.errors
  replication.average_time replication.last_finished)
declare -a LOGS=(sonarw.log.warnings sonarw.log.errors sonarw.log.criticals sonard.log.errors
  sonarfinder.log.warnings sonarfinder.log.errors sonarfinder.log.criticals sonargd.log.errors
  sonargd.log.criticals catalina.log.warnings catalina.log.errors catalina.log.criticals
  dispatcher.log.warnings dispatcher.log.errors kibana.log.warnings kibana.log.errors
  replication.log.errors sonargateway.log.errors mariadb.log.errors pubsub.log.errors stackdriver.log.errors
  sorty.log.errors)
declare -a DATA_AND_RAM=(sonard.ram.res sonard.ram.virt sonard.ram.anon stackdriver.gateway.virt
  pubsub.gateway.virt raw_json.gateway.virt mariadb.gateway.virt kafka.gateway.virt
  eventhub.gateway.virt splunk.gateway.virt avg.sonarResidentMemoryKb.last24h avg.sonarVirtualMemoryKb.last24h
  avg.jemalloc.active avg.jemalloc.mapped sonard.disk.usage sonargd.disk.usage
  sonar.disk.usage.percentage /.disk.usage.percentage collection.full_sql.local_size.du_command
  collection.full_sql.local_size.sonar_command collection.full_sql.cloud_size.cli
  collection.full_sql.cloud_size.sonar_command sonargd.audit.file_count)
declare -a GP=( gp.running gp.failed gp.query.count gp.pipeline.count gp.group.count )
declare -a PURGE=()
declare -a PURGE_JSON=()
declare -a FEEDS=()
declare -a OPCOUNTERS=()

values["Attribute"]="Value,24H_delta,Remarks";

((failed_count=insert_1_count=ingest_2_count=insert_3_count=
  ingest_4_count=insert_5_count=ingest_6_count=total_inserts=0));


get_data_from_mongo_stats(){
  declare -a keys=(result_id is_master cloudwatch_new kafka_new
    mariadb_new splunk_new azure_sql_new stackdriver_new pubsub_new gfake_new
    g2_new subarray_new "actions.counts.query" "actions.counts.insert"
    "actions.counts.update" "actions.counts.remove");

  for col in "${sonargd_collections[@]}"; do
    keys+=( ${col}_path  "sonargd.${col}.count" )
  done

  for key in "${keys[@]}"; do
    values["$key"]=`get_val_from_json "$MONGO_LOG" "$key"`;
  done

  values[gp.failed]=`get_val_from_json "$MONGO_LOG" gp_failed_count`;
  values[gp.query.count]=`get_val_from_json "$MONGO_LOG" gp_query_count`;
  values[gp.pipeline.count]=`get_val_from_json "$MONGO_LOG" gp_pipeline_count`;
  values[gp.group.count]=`get_val_from_json "$MONGO_LOG" gp_group_count`;
  values[gp.query.count.comment]=\"$(sed -e '1,/gp_query_failed/d' -e '/gp_pipeline_failed/,$d' "$MONGO_LOG" | awk '{$1=$1};1' | tr -d '"]\' | head -n -1)\";
  values[gp.pipeline.count.comment]=\"$(sed -e '1,/gp_pipeline_failed/d' -e '/gp_group_failed/,$d' "$MONGO_LOG" | awk '{$1=$1};1' | tr -d '"]\'| head -n -1)\";
  values[gp.group.count.comment]=\"$(sed -e '1,/gp_group_failed/d' -e '/stats/,$d' "$MONGO_LOG" | awk '{$1=$1};1' | tr -d '"]\'| head -n -2)\";
}

#######################
## RUNTIME & USAGE
#######################

get_sonar_versions(){
  values["sonarw.version"]=$( ls "$VERSIONS_HOME" | grep "sonarw-4" );
  values["sonarg.version"]=$( ls "$VERSIONS_HOME" | grep "sonarg-4" );
  if [ -z "${values[sonarg.version]}" ]; then
      values[sonarg.version]="\"N/R\"";
    else
      values[sonarg.version]="\"${values[sonarg.version]}\""
  fi
  values["sonarfinder.version"]=$( ls "$VERSIONS_HOME" | grep "sonarfinder-4" );
  if [ -z "${values[sonarfinder.version]}" ]; then
      values["sonarfinder.version"]="\"N/R\"";
    else
      values["sonarfinder.version"]="\"${values[sonarfinder.version]}\""
  fi
}

get_sonar_pids(){

  values[sonard.pid]=$( get_service_pid_by_name sonard );
  values[sonargd.pid]=$( get_service_pid_by_name sonargd )
  values[sonarfinder.pid]=$( get_service_pid_by_name sonarfinder )


  if [ "${values[hostname]}" = "\"big4-google\"" ]; then
    values[pubsub.gateway.pid]=$( get_service_pid_by_name gateway-gcp@pubsub );
  elif [ "${values[hostname]}" = "\"main.local\"" ]; then
    values[splunk.gateway.pid]=$( get_pid_by_name gateway/splunk );
  elif [ "${values[hostname]}" = "\"big4-azure\"" ]; then
    values[eventhub.gateway.pid]=$( get_pid_by_name gateway/mssql_eventhub );
  elif [ "${values[hostname]}" = "\"big4-aws\"" ]; then
    values[mariadb.gateway.pid]=$( get_pid_by_name mariadb );
    values[raw.json.gateway.pid]=$( get_pid_by_name raw_json );
    values[kafka.gateway.pid]=$( get_pid_by_name gateway/kafka );
  # else
  fi

}

get_uptime(){
  values["sonard.uptime.seconds"]=$( get_running_time ${values[sonard.pid]} );
  values["sonard.uptime.seconds.remark"]="since:";
  values["sonard.uptime.seconds.comment"]="\"$( get_running_since_utc ${values[sonard.pid]})\"";

  if [ "${values[sonarg.version]}" = "\"N/R\"" ]; then
    values["sonargd.uptime.seconds"]="\"N/R\"";
    values["sonargd.running.since"]="\"N/R\"";
  else
    values["sonargd.uptime.seconds"]=$( get_running_time ${values[sonargd.pid]} );
    values["sonargd.uptime.seconds.remark"]="since:";
    values["sonargd.uptime.seconds.comment"]="\"$( get_running_since_utc ${values[sonargd.pid]})\"";
  fi

  if [ "${values[sonarfinder.version]}" = "\"N/R\"" ]; then
    values["sonarfinder.uptime.seconds"]="\"N/R\"";
    values["sonarfinder.running.since"]="\"N/R\"";
  else
    values["sonarfinder.uptime.seconds"]=$( get_running_time ${values[sonarfinder.pid]} );
    values["sonarfinder.uptime.seconds.remark"]="since:";
    values["sonarfinder.uptime.seconds.comment"]="\"$( get_running_since_utc ${values[sonarfinder.pid]})\"";
  fi
}

get_data_and_ram(){
  values["sonard.ram.res"]=$(get_data_from_proc_file ${values[sonard.pid]} VmRSS);
  values["sonard.ram.virt"]=$(get_data_from_proc_file ${values[sonard.pid]} VmSize);
  values["sonard.ram.anon"]=$(get_process_anon ${values[sonard.pid]});

  values["stackdriver.gateway.virt"]="\"N/R\"";
  values["pubsub.gateway.virt"]="\"N/R\"";
  values["mariadb.gateway.virt"]="\"N/R\"";
  values["raw_json.gateway.virt"]="\"N/R\"";
  # get_sonar_services_stats;

  values["kafka.gateway.virt"]="\"N/R\"";
  values["eventhub.gateway.virt"]="\"N/R\"";
  values["splunk.gateway.virt"]="\"N/R\"";

  if [ "${values[hostname]}" = "\"big4-google\"" ];then
    values["pubsub.gateway.virt"]=`get_data_from_proc_file ${values[pubsub.gateway.pid]} VmSize`;
  elif [ "${values[hostname]}" = "\"big4-aws-v3\"" ];then
    values["mariadb.gateway.virt"]=`get_data_from_proc_file ${values[mariadb.gateway.pid]} VmSize`;
    values["raw_json.gateway.virt"]=`get_data_from_proc_file ${values[raw.json.gateway.pid]} VmSize`;
    values["kafka.gateway.virt"]=`get_data_from_proc_file ${values[kafka.gateway.pid]} VmSize`;
  elif [ "${values[hostname]}" = "\"big4-azure\"" ];then
    values["eventhub.gateway.virt"]=`get_data_from_proc_file ${values[eventhub.gateway.pid]} VmSize`;
  elif [ "${values[hostname]}" = "\"main.local\"" ];then
    values["splunk.gateway.virt"]=`get_data_from_proc_file ${values[splunk.gateway.pid]} VmSize`;
  fi

  #get memory stats from client
  values["avg.sonarResidentMemoryKb.last24h"]=`get_val_from_json "$MONGO_LOG" avg_res_mem`;
  values["avg.sonarVirtualMemoryKb.last24h"]=`get_val_from_json "$MONGO_LOG" avg_virt_mem`;
  values["avg.jemalloc.active"]=`get_val_from_json "$MONGO_LOG" avg_jam_active`;
  values["avg.jemalloc.mapped"]=`get_val_from_json "$MONGO_LOG" avg_jam_mapped`;

  #disk usage
  values["sonard.disk.usage"]=` du -sbL "$SONARD_DATA" | awk '{print $1}'`;
  if [[ ${values[sonargd.pid]} =~ ^-?[0-9]+$ ]];then
    values["sonargd.disk.usage"]=` du -sbL "$SONARGD_DATA" | awk '{print $1}'`;
  else
    values["sonargd.disk.usage"]="\"N/R\"";
  fi

  if [ "${values["full_sql_path"]}" != "\"N/A\"" ] && [ "${values["full_sql_path"]}" != "undefined" ];then
    values["collection.full_sql.local_size.du_command"]=` du -sbL "${values["full_sql_path"]}" | awk '{print $1}'`;
    values["collection.full_sql.local_size.sonar_command"]=`get_val_from_json "$MONGO_LOG" full_sql_local_storage`;
    values["collection.full_sql.cloud_size.sonar_command"]=`get_val_from_json "$MONGO_LOG" full_sql_cloud_storage`;
  else
    values["collection.full_sql.local_size.du_command"]="\"N/R\"";
    values["collection.full_sql.local_size.sonar_command"]="\"N/R\"";
    values["collection.full_sql.cloud_size.sonar_command"]="\"N/R\"";
  fi

  values["sonar.disk.usage.percentage"]=`df -h "$SONARD_DATA/data" | awk '{print $5}' | grep -v Use% | tr -d '%'`;
  values["/.disk.usage.percentage"]=`df -h / | awk '{print $5}' | grep -v Use% | tr -d '%'`;

  if [ "${values[hostname]}" = "\"big4-google\"" ];then
    values["collection.full_sql.cloud_size.cli"]=`gsutil du -s gs://big4_googl/30082263-f1ea-4c92-8871-403ab4ef7842/ | awk '{print $1}'`;
  elif [ "${values[hostname]}" = "\"main.local\"" ] || [ "${values[hostname]}" = "\"dr.local\"" ];then
    values["collection.full_sql.cloud_size.cli"]=` du -bs /disk-cloud/sonarw/sonarw-main.local/0fc0b6af-95f3-4912-8d53-720f378f1ff6 |awk '{print $1}'`;
  else
    values["collection.full_sql.cloud_size.cli"]="\"N/R\"";
  fi
}

#########################
## GP
#########################
get_gp_alive(){
  values[gp.running]=false;
  values[gp.query.count.remark]=false;
  values[gp.pipeline.count.remark]=false;
  values[gp.group.count.remark]=false;

  if ps ax | grep "run_everything" | grep -v grep > /tmp/daily_temp; then
    values[gp.running]=true;
    if ps ax | grep "test_pipelines.js" | grep query_flag=true | grep -v grep > /tmp/daily_temp; then
      values[gp.query.count.remark]=true;
    fi
    if ps ax | grep "test_pipelines.js" | grep query_flag=false | grep -v grep > /tmp/daily_temp; then
      values[gp.pipeline.count.remark]=true;
    fi
    if ps ax | grep "test_group_pipelines.py" | grep -v grep > /tmp/daily_temp;then
      values[gp.group.count.remark]=true;
    fi
  elif [ ! -f /data/sonar/logs/reversal_tests.log ];then
    values[gp.running]="N/R";
  fi
}

#########################
## PURGE
#########################

get_purge_stats(){
  col_path="$1"
  # col_name="$2";
  if [ "$col_path" != "\"N/A\"" ] && [ "$col_path" != "undefined" ];then
    first_local_part=`get_first_local_part "$col_path"`;
    if ! [[ "$first_local_part" =~ ^-?[0-9]+$ ]]; then
      first_local_part=\"N/A\";
    fi
    first_cloud_part=`get_first_cloud_part "$col_path"`;
    if ! [[ "$first_cloud_part" =~ ^-?[0-9]+$ ]]; then
      first_cloud_part=\"N/A\";
    fi
    total_parts=`get_total_parts "$col_path"`;
    if ! [[ "$total_parts" =~ ^-?[0-9]+$ ]]; then
      total_parts=\"N/A\";
    fi
  else
    first_local_part=\"N/R\";
    first_cloud_part=\"N/R\";
    total_parts=\"N/R\";
  fi
  echo "{\"first_local_part\": $first_local_part, \"first_cloud_part\": $first_cloud_part, \"total_parts\": $total_parts}";
}

get_all_purge_stats(){
  # shellcheck disable=SC2068
  for col in ${sonargd_collections[@]}; do
    PURGE+=( first.local.part.${col} first.cloud.part.${col} );
    PURGE_JSON+=( ${col}_purge_stats );
    values[${col}_purge_stats]=`get_purge_stats "${values[${col}_path]}"`;
    values[first.local.part.${col}]=`echo "${values[${col}_purge_stats]}" | cut -d " " -f 2 | tr -d '},'`;
    values[first.local.part.${col}.remark]=`echo "${values[${col}_purge_stats]}" | cut -d " " -f 6 | tr -d '},'`;
    values[first.local.part.${col}.comment]=` grep "full cloud purge" "$HOME_PATH/sonarw/data/sonargd/history.log" | grep ${col} | tail -n1 | tr -d ','`;
    values[first.cloud.part.${col}]=`echo "${values[${col}_purge_stats]}" | cut -d " " -f 4 | tr -d '},'`;
    values[first.cloud.part.${col}.comment]=`get_purge_obj_from_json "$MONGO_LOG" ${col}_last_purge`
  done
}

#########################
## CHECKING LOG FILES
#########################

grab_errors(){
  local log_path="$LOGS_HOME/$1_$DATE_ONLY.log";
  local gateway_logs=(sonargateway pubsub stackdriver mariadb raw_json)
  if [[ " ${gateway_logs[*]} " == *"$1"* ]]; then
    get_gateway_errors ${1} ${2} ${3} ${log_path};
  else
    get_${1}_errors ${1} ${2} ${3} ${log_path};
  fi
}

get_sorty_errors(){
  local log_path=${4};
  sed -n "$2,$ p" "$3" | grep "FAILED" >> "$log_path";
  values["$1.log.errors"]=`wc -l "$log_path" | awk '{print $1}'`;
  values["$1.log.errors.comment"]=`sort -k2 "$log_path" | uniq -f1 -c | head -n50 | sort -k2 | awk 'length < 50000'`;
  values["$1.log.errors.comment"]=\"${values["sorty.log.errors.comment"]//\"/ }\";
  values["$1.tests.count"]=` sed -n "$2,$ p" "$3" | grep -E "PASSED|OK" | wc -l`;
}

get_insert_test_errors(){
  local log_path=${4};
  sed -n "$2,$ p" "$3" | grep "FAIL!" >> "$log_path";
  local cnt=`grep "FAIL!" "$log_path" | wc -l`;
  let "failed_count+=$cnt";
  cnt=` sed -n "$2,$ p" "$3" | grep "Elapse time for ebsco_all_insert_1" | wc -l`;
  let "insert_1_count+=$cnt";
  cnt=` sed -n "$2,$ p" "$3" | grep "Elapse time for ebsco_all_ingest_2" | wc -l`;
  let "ingest_2_count+=$cnt";
  cnt=` sed -n "$2,$ p" "$3" | grep "Elapse time for bonds_small_insert_3" | wc -l`;
  let "insert_3_count+=$cnt";
  cnt=` sed -n "$2,$ p" "$3" | grep "Elapse time for bonds_small_ingest_4" | wc -l`;
  let "ingest_4_count+=$cnt";
  cnt=` sed -n "$2,$ p" "$3" | grep "Elapse time for gdm_fullSQL_insert_5" | wc -l`;
  let "insert_5_count+=$cnt";
  cnt=` sed -n "$2,$ p" "$3" | grep "Elapse time for gdm_fullSQL_ingest_6" | wc -l`;
  let "ingest_6_count+=$cnt";
}

get_seppuku_errors(){
  local log_path=${4};
   sed -n "$2,$ p" "$3" | grep -E " W | E | C | F " | grep -v "Last line repeated" >> "$log_path";
  seppuku_errors=`cat "$log_path" | wc -l`;
  local cnt=` sed -n "$2,$ p" "$3" | grep "RECOVERY: appending" | wc -l`;
  let "seppuku_append+=$cnt";
  cnt=` sed -n "$2,$ p" "$3" | grep "RECOVERY: checking/appending" | wc -l`;
  let "seppuku_check+=$cnt";
  cnt=` sed -n "$2,$ p" "$3" | grep "RECOVERY: creating" | wc -l`;
  let "seppuku_create+=$cnt";
  cnt=` sed -n "$2,$ p" "$3" | grep "RECOVERY: rename" | wc -l`;
  let "seppuku_rename+=$cnt";
  cnt=` sed -n "$2,$ p" "$3" | grep "RECOVERY: Removing" | wc -l`;
  let "seppuku_remove+=$cnt";
  cnt=` sed -n "$2,$ p" "$3" | grep "needs block recovery" | wc -l`;
  let "seppuku_truncate+=$cnt";
}

get_replication_errors(){
  local log_name=${1};
  local start_index=${2};
  local origin_log_path=${3};
  local log_path=${4};

   sed -n "$start_index,$ p" "$origin_log_path" | grep -E "ERROR|WARNING|SonarRS started|replication complete|FINISHED WITH ERROR|FINISHED WITH WARNINGS" >> "$log_path";
  values["$log_name.log.errors"]=`grep -E " ERROR | WARNING " "$log_path" | wc -l`;
  values["$log_name.log.errors.comment"]=`grep -E " ERROR | WARNING " "$log_path" | sort -k4 | uniq -f3 -c | head -n50 | sort -k2 | awk 'length < 50000'`;
  values["$log_name.log.errors.comment"]=\"${values["$log_name.log.errors.comment"]//\"/ }\";
  values["$log_name.started.count"]=`grep "SonarRS started" "$log_path" | wc -l`;
  values["$log_name.completed.count"]=`grep "replication complete." "$log_path" | wc -l`;
  values["$log_name.done.with.errors"]=`grep -E "FINISHED WITH ERROR|FINISHED WITH WARNINGS" "$log_path" | wc -l`;
  if [[ "${values[$log_name.completed.count]}" =~ ^-?[0-9]+$ ]] && [ "${values[$log_name.completed.count]}" -gt 0 ];then
    values["$log_name.average_time"]=` sed -n "$start_index,$ p" "$origin_log_path" | grep -E "Total sync time" | awk '{ total=(total+$8) } END { print (total)/NR }'`;
  else
    values["$log_name.average_time"]="\"N/R\"";
  fi
  values["$log_name.last_finished"]="\""` grep -E "SonarRS replication complete.|SonarRS FINISHED WITH WARNINGS" "$3" | tail -n1 | awk '{print $1"T"$2}' | tr -d ','`"\"";
}


get_gateway_errors(){
  local log_name=${1};
  local start_index=${2};
  local origin_log_path=${3};
  local log_path=${4};

   sed -n "$start_index,$ p" "$origin_log_path" | grep -E " FATAL | ERROR | WARNING " >> "$log_path";
  values["$log_name.log.errors"]=`cat "$log_path" | wc -l`;
  values["$log_name.log.errors.comment"]=`sort -k3 "$log_path" | uniq -f2 -c | head -n50 | sort -k2 | awk 'length < 50000'`;
  values["$log_name.log.errors.comment"]=\"${values["$log_name.log.errors.comment"]//\"/ }\";
}

get_kibana_errors(){
  local log_name=${1};
  local start_index=${2};
  local origin_log_path=${3};
  local log_path=${4};

   sed -n "$start_index,$ p" "$origin_log_path" | grep -Ei "warning|error|fatal|exception"| grep -vE "INFO" >> "$log_path";
  values["$log_name.log.warnings"]=`grep -Ev "error|fatal|exception" "$log_path" | wc -l`;
  values["$log_name.log.warnings.comment"]=`grep -Ev "error|fatal" "$log_path" | sort -k4 | uniq -f3 -c | head -n50 | sort -k2 | awk 'length < 100'`;
  values["$log_name.log.warnings.comment"]=\"${values["$log_name.log.warnings.comment"]//\"/ }\";
  values["$log_name.log.errors"]=`grep -E "error|fatal" "$log_path" | wc -l`;
  values["$log_name.log.errors.comment"]=`grep -E "error|fatal|exception" "$log_path" | sort -k4 | uniq -f3 -c | head -n50 | sort -k2 | awk 'length < 100'`;
  values["$log_name.log.errors.comment"]=\"${values["$log_name.log.errors.comment"]//\"/ }\";
}

get_sonard_errors(){
  local log_name=${1};
  local start_index=${2};
  local origin_log_path=${3};
  local log_path=${4};

   sed -n "$start_index,$ p" "$origin_log_path" | grep -vE "heartbeat|SonarW exiting.|Using configuration file|System Starting up" >> "$log_path";

  values["$log_name.log.errors"]=`cat "$log_path" | wc -l`;
  values["$log_name.log.errors.comment"]=`cat "$log_path" | sort -k2 | uniq -f1 -c | head -n50 | sort -k2 | awk 'length < 50000'`;
  values["$log_name.log.errors.comment"]=\"${values["$log_name.log.errors.comment"]//\"/ }\";
}

get_sonarw_errors(){
    local log_name=${1};
    local start_index=${2};
    local origin_log_path=${3};
    local log_path=${4};

     sed -n "$start_index,$ p" "$origin_log_path" | grep -vE " I | D |^ |Last line repeated" >> "$log_path";
    grep " W " "$log_path" > "$log_path.W.tmp";
    values["$log_name.log.warnings"]=`cat "$log_path.W.tmp" | wc -l`;
    get_uniq_prints_in_file_by_col 4 "$log_path.W.tmp" ${log_name}_warnings;
    values["$log_name.log.warnings.comment"]=`cat /tmp/${log_name}_warnings.uniqe_results.tmp | awk 'length < 50000'`;
    rm -f /tmp/${log_name}_warnings.uniqe_results.tmp "$log_path.W.tmp";
    values["$log_name.log.warnings.comment"]=\""${values["$log_name.log.warnings.comment"]//\"/ }"\";

    grep " E " "$log_path" > "$log_path.E.tmp";
    values["$log_name.log.errors"]=`cat "$log_path.E.tmp" | wc -l`;
    get_uniq_prints_in_file_by_col 4 "$log_path.E.tmp" ${log_name}_errors;
    values["$log_name.log.errors.comment"]=`cat /tmp/${log_name}_errors.uniqe_results.tmp | awk 'length < 50000'`;
    rm -f /tmp/${log_name}_errors.uniqe_results.tmp "$log_path.E.tmp";
    values["$log_name.log.errors.comment"]=\""${values["$log_name.log.errors.comment"]//\"/ }"\";

    grep -E " C | F " "$log_path" > "$log_path.C.tmp";
    values["$log_name.log.criticals"]=`cat "$log_path.C.tmp" | wc -l`;
    get_uniq_prints_in_file_by_col 4 "$log_path.C.tmp" ${log_name}_criticals
    values["$log_name.log.criticals.comment"]=`cat /tmp/${log_name}_criticals.uniqe_results.tmp | awk 'length < 50000'`;
    rm -f /tmp/${log_name}_criticals.uniqe_results.tmp "$log_path.C.tmp";
    values["$log_name.log.criticals.comment"]=\""${values["$log_name.log.criticals.comment"]//\"/ }"\";
}

get_dispatcher_errors(){
  local log_name=${1};
  local start_index=${2};
  local origin_log_path=${3};
  local log_path=${4};

   sed -n "$start_index,$ p" "$origin_log_path" | grep -Ev "INFO|DEBUG" >> "$log_path";
  values["$log_name.log.warnings"]=`grep -E "WARN" "$log_path" | wc -l`;
  values["$log_name.log.warnings.comment"]=`grep -E "WARN" "$log_path" | sort -k4 | uniq -f3 -c | head -n50 | sort -k1 | awk 'length < 50000'`;
  values["$log_name.log.warnings.comment"]=\"${values["$log_name.log.warnings.comment"]//\"/ }\";
  values["$log_name.log.errors"]=`grep "ERROR" "$log_path" | wc -l`;
  values["$log_name.log.errors.comment"]=`grep "ERROR" "$log_path" | sort -k4 | uniq -f3 -c | head -n50 | sort -k1 | awk 'length < 50000'`;
  values["$log_name.log.errors.comment"]=\"${values["$log_name.log.errors.comment"]//\"/ }\";
}

get_sonarfinder_errors(){
    local log_name=${1};
    local start_index=${2};
    local origin_log_path=${3};
    local log_path=${4};

     sed -n "$start_index,$ p" "$origin_log_path" | grep -E " WARN | ERROR | FATAL | SEVERE" >> "$log_path";

    grep "WARN" "$log_path" > "$log_path.W.tmp";
    values["$log_name.log.warnings"]=`cat "$log_path.W.tmp" | wc -l`;
    get_uniq_prints_in_file_by_col 3 "$log_path.W.tmp" ${log_name}_warnings;
    values["$log_name.log.warnings.comment"]=`cat /tmp/${log_name}_warnings.uniqe_results.tmp | awk 'length < 50000'`;
    values["$log_name.log.warnings.comment"]=\"${values["$log_name.log.warnings.comment"]//\"/ }\";
    rm -f /tmp/${log_name}_warnings.uniqe_results.tmp "$log_path.W.tmp"

    grep "ERROR" "$log_path" > "$log_path.E.tmp";
    values["$log_name.log.errors"]=`cat "$log_path.E.tmp" | wc -l`;
    get_uniq_prints_in_file_by_col 3 "$log_path.E.tmp" ${log_name}_errors;
    values["$log_name.log.errors.comment"]=`cat /tmp/${log_name}_errors.uniqe_results.tmp | awk 'length < 50000'`;
    values["$log_name.log.errors.comment"]=\"${values["$log_name.log.errors.comment"]//\"/ }\";
    rm -f /tmp/${log_name}_errors.uniqe_results.tmp "$log_path.E.tmp";

    grep -Ev " ERROR | WARN " "$log_path" > "$log_path.C.tmp";
    values["$log_name.log.criticals"]=`cat "$log_path.C.tmp" | wc -l`;
    get_uniq_prints_in_file_by_col 3 "$log_path.C.tmp" ${log_name}_criticals;
    values["$log_name.log.criticals.comment"]=`cat /tmp/${log_name}_criticals.uniqe_results.tmp | awk 'length < 50000'`;
    values["$log_name.log.criticals.comment"]=\"${values["$log_name.log.criticals.comment"]//\"/ }\";
    rm -f /tmp/${log_name}_warnings.uniqe_results.tmp "$log_path.C.tmp"
}

get_catalina_errors(){
    local log_name=${1};
    local start_index=${2};
    local origin_log_path=${3};
    local log_path=${4};

     sed -n "$start_index,$ p" "$origin_log_path" | grep -vE " INFO " >> "$log_path";

    values["$log_name.log.warnings"]=`grep " WARN " "$log_path" | wc -l`;
    values["$log_name.log.warnings.comment"]=`grep " WARN " "$log_path" | sort -k4 | uniq -f3 -c | head -n50 | sort -k2 | awk 'length < 50000'`;
    values["$log_name.log.warnings.comment"]=\"${values["$log_name.log.warnings.comment"]//\"/ }\";

    values["$log_name.log.errors"]=`grep " ERROR " "$log_path" | wc -l`;
    values["$log_name.log.errors.comment"]=`grep " ERROR " "$log_path" | sort -k4 | uniq -f3 -c | head -n50 | sort -k2 | awk 'length < 50000'`;
    values["$log_name.log.errors.comment"]=\"${values["$log_name.log.errors.comment"]//\"/ }\";

    values["$log_name.log.criticals"]=`grep -E " SEVERE | FATAL" "$log_path" | wc -l`;
    values["$log_name.log.criticals.comment"]=`grep -E " SEVERE " "$log_path" | sort -k4 | uniq -f3 -c | head -n50 | sort -k2 | awk 'length < 50000'`;
    values["$log_name.log.criticals.comment"]=\"${values["$log_name.log.criticals.comment"]//\"/ }\";
}

get_sonargd_errors(){
    local log_name=${1};
    local start_index=${2};
    local origin_log_path=${3};
    local log_path=${4};
     sed -n "$start_index,$ p" "$origin_log_path" | grep -Ev "^I |^D " >> "$log_path";
    values["$log_name.log.errors"]=`grep -E "^E " "$log_path" | wc -l`;
    values["$log_name.log.errors.comment"]=`grep -E "^E " "$log_path" | sort -k5 | uniq -f4 -c | head -n50 | sort -k2 | awk 'length < 50000'`;
    values["$log_name.log.errors.comment"]=\"${values["$log_name.log.errors.comment"]//\"/ }\";
    values["$log_name.log.criticals"]=`grep -E "^C |^F " "$log_path" | wc -l`;
    values["$log_name.log.criticals.comment"]=`grep -E "^C |^F " "$log_path" | sort -k5 | uniq -f4 -c | head -n50 | sort -k2 | awk 'length < 50000'`;
    values["$log_name.log.criticals.comment"]=\"${values["$log_name.log.criticals.comment"]//\"/ }\";
}

get_starting_point_from_file(){
  local path="$1";
  local date_to_compare="$2";
  local log_name="$3";

   tac "$path" > "$LOGS_HOME/$log_name.reversed.tmp"
  local rows=`wc -l "$LOGS_HOME/$log_name.reversed.tmp" | awk '{print $1}'`;
  let "index=$rows+1";

  while IFS='' read -r line || [[ -n "$line" ]]; do
    local line_date=`parse_date_from_line "$line" | tr -d ' |T|:|-'`;
    if [[ "$line_date" =~ ^([0-9])+$ ]] && [[ "$line_date" < "$date_to_compare" ]];then
      break;
    fi
    let "index--";
  done < "$LOGS_HOME/$log_name.reversed.tmp"

  rm -f "$LOGS_HOME/$log_name.reversed.tmp";
  echo "$index";
}

get_stats_from_log(){
  #Handle log rotate
  if [ "$2" -eq 0 ];then
    local LOG_PATH="$1"
  elif [ "$2" -eq 1 ];then
    local LOG_PATH="$1.$2"
  else
    local LOG_PATH="${1%?}$2"
  fi

  logger "Opening log: $LOG_PATH"

  file_exists=` ls "$LOG_PATH"`;
  if [ "$file_exists" ];then
    local LINE_INDEX=1;

     cat "$LOG_PATH" > "$LOGS_HOME/$3.tmp";
    while IFS='' read -r first_line || [[ -n "$first_line" ]]; do
      LOG_STARTING_TIME=`parse_date_from_line "$first_line" | tr -d ' |T|:|-'`;
      if [ "$LOG_STARTING_TIME" != "N/A" ]; then
        break;
      fi
    done < "$LOGS_HOME/$3.tmp"
    rm -f "$LOGS_HOME/$3.tmp"

    logger "log start: $LOG_STARTING_TIME";

    if  [[ "$LOG_STARTING_TIME" =~ ^([0-9])+$ ]] && [[ "$DATE_YESTERDAY" < "$LOG_STARTING_TIME" ]];then
      let "FILE_INDEX=$2+1";
      get_stats_from_log "$LOG_PATH" "$FILE_INDEX" "$3";
      LINE_INDEX=1;
    else
      LINE_INDEX=`get_starting_point_from_file "$LOG_PATH" "$DATE_YESTERDAY" "$3"`;
    fi

    logger "grab_errors $3 $LINE_INDEX $LOG_PATH";
    grab_errors "$3" "$LINE_INDEX" "$LOG_PATH";

  else
    logger "file NOT found";
    return;
  fi
}

get_replication_stats(){
  if [ "${values[is_master]}" = "false" ] && [ "${values[sonarg.version]}" != "\"N/R\"" ]; then
    get_stats_from_log "$REPLICATION_LOG_HOME"/replication.log 0 replication;
  else
    values["replication.log.errors"]="\"N/R\"";
    values["replication.started.count"]="\"N/R\"";
    values["replication.completed.count"]="\"N/R\"";
    values["replication.done.with.errors"]="\"N/R\"";
    values["replication.average_time"]="\"N/R\"";
    values["replication.last_finished"]="\"N/R\"";
  fi
}

get_gateway_log_errors(){
  values["sonargateway.log.errors"]="\"N/R\"";
  values["pubsub.log.errors"]="\"N/R\"";
  values["stackdriver.log.errors"]="\"N/R\"";
  values["raw_json.log.errors"]="\"N/R\"";
  values["mariadb.log.errors"]="\"N/R\"";

  if [ "${values[hostname]}" = "\"big4-azure\"" ]; then
    get_stats_from_log "$GATEWAY_LOG_HOME"/gateway/sonargateway.log 0 sonargateway;
  elif [ "${values[hostname]}" = "\"big4-google\"" ]; then
     get_stats_from_log "$GATEWAY_LOG_HOME"/gateway/cloud/gcp/pubsub/sonargateway.log 0 pubsub;
  elif [ "${values[hostname]}" = "\"big4-aws\"" ];then
     # get_stats_from_log /var/log/sonar/gateway/cloud/aws/raw_json/sonargateway 0 raw_json;
     get_stats_from_log "$GATEWAY_LOG_HOME"/gateway/cloud/aws/mariadb/sonargateway.log 0 mariadb;
  fi
}

get_sorty_log_errors(){
  if [ "${values[hostname]}" = "\"sorty.local\"" ]; then
     get_stats_from_log /data/sonar/logs/sort.log 0 sorty;
  else
     values["sorty.log.errors"]="\"N/R\"";
     values["sorty.tests.count"]="\"N/R\"";
  fi
}

get_inserts_errors(){
  if [ -f "$HOME_PATH"/insert_test.log ]; then
     get_stats_from_log "$HOME_PATH"/insert_test.log 0 insert_test;
  fi
}

get_sonarg_errors(){
  values["sonargd.log.warnings"]="\"N/R\"";
  values["sonargd.log.errors"]="\"N/R\"";
  values["sonargd.log.criticals"]="\"N/R\"";
  values["kibana.log.warnings"]="\"N/R\"";
  values["kibana.log.errors"]="\"N/R\"";
  values["sonarfinder.log.warnings"]="\"N/R\"";
  values["sonarfinder.log.errors"]="\"N/R\"";
  values["sonarfinder.log.criticals"]="\"N/R\"";
  values["catalina.log.warnings"]="\"N/R\"";
  values["catalina.log.errors"]="\"N/R\"";
  values["catalina.log.criticals"]="\"N/R\"";
  values["dispatcher.log.warnings"]="\"N/R\"";
  values["dispatcher.log.errors"]="\"N/R\"";

  if [ "${values[sonarg.version]}" != "\"N/R\"" ];then
    get_stats_from_log "$SONARGD_LOG_HOME"/sonargd.log 0 sonargd;
    get_stats_from_log "$KIBANA_LOG_HOME"/sonar-kibana.log 0 kibana;
  fi

  if [ "${values[sonarfinder.version]}" != "\"N/R\"" ];then
    get_stats_from_log "$DISPATCHER_LOG_HOME"/dispatcher.log 0 dispatcher;
    get_stats_from_log "$SONARFINDER_LOG_HOME"/sonarFinder.log 0 sonarfinder;
    get_stats_from_log "$CATALINA_LOG_HOME"/catalina.out 0 catalina;
  fi

}

get_all_logs_stats(){
  # get sonar logs errors
  get_stats_from_log "$SONARW_LOG_HOME"/sonarw.log 0 sonarw;
  get_stats_from_log "$SONARW_LOG_HOME"/sonard.log 0 sonard;
  get_replication_stats;
  get_gateway_log_errors;
  get_sorty_log_errors;
  get_sonarg_errors;
  get_inserts_errors;
}

########################
## REPORT SONAR
########################
get_all_general_machine_stats(){
  get_sonar_pids;
  get_sonar_versions;
  get_uptime;
  get_data_and_ram;
  get_gp_alive;
  get_all_purge_stats;
  if [ "${values[sonarg.version]}" != "\"N/R\"" ];then
      values["sonargd.audit.file_count"]=` ls "$SONARGD_DATA"/audit | wc -l`;
    else
      values["sonargd.audit.file_count"]="\"N/R\"";
  fi
}

print_long_csv(){

  #print csv header
  print_csv_line "Attribute" "${values[Attribute]}";

  for section in "${sections[@]}" ;
  do
    print_csv_line "${section}" ;
      case $section in
       FEEDS)
            printf '%s\n' `sed -e '1,/"feeds"/d' -e '/new_feed_source/,$d' "$MONGO_LOG" | awk '{$1=$1};1' | tr : , | tr -d " " `;
            print_csv_line sorty_count "${values[sorty.tests.count]}";
            ;;
       OPCOUNTERS)
            printf '%s\n' `sed -e '1,/"op_counters"/d' -e '/}/,$d' "$MONGO_LOG" | awk '{$1=$1};1' | tr : , | tr -d " " `;
            ;;
       COLLECTIONS_COUNT)
            printf '%s\n' `sed -e '1,/"collection_counts"/d' -e '/}/,$d' "$MONGO_LOG" | awk '{$1=$1};1' | tr : , | tr -d " " `;
            ;;
       UEBA_COUNTS)
            local ueba_running=`get_val_from_json "$MONGO_LOG" ueba_counts`
            if [ "$ueba_running" != "N/R" ];then
              printf '%s\n' `sed -e '1,/"ueba_counts"/d' -e '/}/,$d' "$MONGO_LOG" | awk '{$1=$1};1' | tr : , | tr -d " " `;
            fi
            ;;
       *)
            tmp=$section[@]
            for key in "${!tmp}" ;
            do
             printf '%s\n' "$key","${values[$key]}",,"${values[$key.remark]}","${values[$key.comment]}"
            done
            ;;
    esac
  done

  if [ -f "$SCRIPT_DIR"/query_list.py ]; then
    print_csv_line QUERIES;
    ` grep '"name":' "$SCRIPT_DIR"/query_list.py | awk '{print $2}' | grep -v name | tr -d '",'\' > "$SCRIPT_DIR"/temp_query_list`;
    while read q; do
      echo "$q," ` cat "$SCRIPT_DIR"/report.json | jq ."$q"`;
    done < "$SCRIPT_DIR"/temp_query_list
  fi

  if [ -f "$HOME_PATH"/insert_test.log ];then
    print_csv_line INSERTS;
    print_csv_line total_failed_inserts "$failed_count";
    print_csv_line ebsco_all_insert_1.count "$insert_1_count";
    print_csv_line ebsco_all_ingest_2.count "$ingest_2_count";
    print_csv_line bonds_small_insert_3.count "$insert_3_count";
    print_csv_line bonds_small_ingest_4.count "$ingest_4_count";
    print_csv_line gdm_fullSQL_insert_5.count "$insert_5_count";
    print_csv_line gdm_fullSQL_ingest_6.count "$ingest_6_count";
  fi

}

print_json(){

  ########################
  ####JSON
  ########################

  echo "{";
  sec_pos=$(( ${#sections[*]} - 1 ))
  last_sec=${sections[$sec_pos]}
  for section in "${sections[@]}" ;
  do
    echo "\"${section}\": {" ;
        case $section in
           FEEDS)
                printf '%s\n' `sed -e '1,/feeds/d' -e '/purge/,$d' "$MONGO_LOG" | awk '{$1=$1};1' | tr -d " "| head -n -1`;
                ;;
           GP)
                obj=`sed -e '1,/gp/d' -e '/gp_query_failed/,$d' "$MONGO_LOG" | awk '{$1=$1};1' | tr -d " "`;
                obj=${obj%?};
                printf '%s\n' "$obj";
                ;;
           OPCOUNTERS)
                printf '%s\n' `sed -e '1,/op_counters/d' -e '/}/,$d' "$MONGO_LOG" | awk '{$1=$1};1' | tr -d " "`;
                ;;
           COLLECTIONS_COUNT)
                printf '%s\n' `sed -e '1,/collection_counts/d' -e '/}/,$d' "$MONGO_LOG" | awk '{$1=$1};1' | tr -d " "`;
                ;;
           UEBA_COUNTS)
                printf '%s\n' `sed -e '1,/ueba_counts/d' -e '/}/,$d' "$MONGO_LOG" | awk '{$1=$1};1' | tr -d " "`;
                ;;
           *)
                if [[ ${section} = PURGE ]];then
                  section=PURGE_JSON;
                fi
                tmp=#$section[*]
                pos=$(( ${!tmp} - 1 ))
                tmp1=$section[$pos]
                last_key=${!tmp1}
                tmp2=$section[@]
                for key in "${!tmp2}" ;
                do
                  if [ "${section}" = "LOGS" ] && [ "${values[$key]}" = "\"N/R\"" ];then
                    values[$key]=0;
                  fi
                  if [[ "$key" == "$last_key" ]];then
                    echo "\"${key}\": ${values[$key]}";
                  else
                    echo "\"${key}\": ${values[$key]}",;
                  fi
                done
                ;;
        esac
    if [[ "$section" == "$last_sec" ]];then
      echo "}";
    else
      echo "},";
    fi
    done
  echo "}";

}