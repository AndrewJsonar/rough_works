#!/usr/bin/env bash

logger(){
    echo `date -u` - "$@";
}

get_pid_by_name(){
  if [ $# -gt 0 ];then
    local process_name="$1"
    local result=$(ps -elf|grep "/$1" | grep -vE 'grep|dispatcher|tail|fuser|gdb|vi |vim|^vi$|sonargdm|$JSONAR_BASEDIR/bin/sonard|sonard.sh' | awk '{print $4}');
  fi
  echo $(return_if_not_empty ${result});
}

get_service_pid_by_name(){
  if [ $# -gt 0 ];then
    local name="$1"
    local result=$(systemctl status "${name}" | grep "Main PID" | awk '{print $3}');
  fi
  echo $(return_if_not_empty ${result});
}

get_val_from_json(){
  if [ $# -gt 0 ];then
    local file="$1";
    local field="$2";
    local result=`grep -E "$2" "$1" | awk '{print $3}'| tr -d '"|,'`
  fi
  echo $(return_if_not_empty ${result});
}

get_purge_obj_from_json(){
  if [ $# -gt 0 ];then
  	local file="$1";
  	local field="$2";
    local result=$(grep "$field" "$file" | tr -d '"\\|' | awk '{$1=$1};1');
  fi
  echo $(return_if_not_empty ${result});
}

get_running_time() {
  local pid="$1";
  if [[ "$pid" =~ ^-?[0-9]+$ ]]; then
    local result=$(ps -p "$pid" -o etimes=);
  fi
  echo $(return_if_not_empty ${result});
}

get_running_since_utc(){
  local pid="$1";
  local result="N/A";
  if [[ "$pid" =~ ^-?[0-9]+$ ]];then
    local date_string=`ps -eo pid,lstart,cmd |grep -w "$pid" | grep -vE 'grep|dispatcher|tail|fuser|gdb' | awk '{print $2" "$3" "$4" "$5" "$6}'`;
    local local_time=`date -d "$date_string"`;
    running_since_utc=`date -u -d "$local_time"`;
    if [[ -n  ${running_since_utc} ]];then
      result=${running_since_utc};
    fi
  fi
  echo ${result};
}

print_if_num(){
	local field="$1";
	local value="$2";
	if [[ "$value" =~ ^-?[0-9]+$ ]]; then
		echo "\"$field\": $value";
	elif [ "$value" = N/R ]; then
		echo "\"$field\": \"N/R\"";
	else
		echo "\"$field\": \"N/A\"";
  fi
}

get_data_from_proc_file(){
  local pid="$1";
  local field="$2";
  if [[ "$pid" =~ ^-?[0-9]+$ ]];then
    local result=`cat /proc/"$pid"/status | grep "$field"| awk '{print $2}'`;
  fi
  echo $(return_if_not_empty ${result});
}

get_process_anon(){
  local pid="$1";
  if [[ "$pid" =~ ^-?[0-9]+$ ]];then
    local result=`sudo pmap "$pid" | grep anon | awk '{ total=total+substr($2, 1, length($2)-1) } END { print (total) }'`;
  fi
  echo $(return_if_not_empty ${result});
}

print_csv_line(){
	printf '%s\n' "$@"  | paste -sd ',';
}

#helper func to return timestamp(4 formats) from a text(1 line from a log)
parse_date_from_line(){
  local text="$1";
  local date=`echo "$text" | grep -Eo '[0-9.]{4}-[0-9.]{2}-[0-9.]{2}[T][0-9.]{2}:[0-9.]{2}:[0-9.]{2}' | head -n1`; #1st priority
  if [ -z "$date" ]; then
      date=`echo "$text" | grep -Eo '[0-9.]{4}-[0-9.]{2}-[0-9.]{2}[ ][0-9.]{2}:[0-9.]{2}:[0-9.]{2}' | head -n1`; #2nd priority
      if [ -z "$date" ]; then
          local tmp_date=`echo "$text" | grep -Eo '[0-9.]{2}-[A-Za-z]{3}-[0-9.]{4}[ ][0-9.]{2}:[0-9.]{2}:[0-9.]{2}' | head -n1`; #3rd priority
          if [ -z "$tmp_date" ]; then
            tmp_date=`echo "$text" | grep -Eo '[A-Za-z]{3} [0-9.]{2} [0-9.]{2}:[0-9.]{2}:[0-9.]{2}' | head -n1`; #4th priority
          fi
          if [ -z "$tmp_date" ];then
            date="N/A";
          else
            date=`date -d "$tmp_date" +%Y-%m-%dT%R:%S`;
          fi
      fi
  fi
  echo "$date";
}

return_if_not_empty(){
  local result="\"N/A\"";
  if [ $# -gt 0 ];then
  	result="$@";
  fi
  echo "$result"
}

return_if_num(){
  local result="\"N/A\"";
  if [[ "$1" =~ ^-?[0-9]+$ ]];then
  	result="$1";
  fi
  echo "$result"
}

get_first_local_part(){
  local prefix="$1";
  local suffix="col_*";
  local first_local_part_string=`sudo find "$prefix" -name "$suffix" | sort -V -k1.4 | head -n1`;
  local result=`[[ $first_local_part_string =~ /([^/]+)/[^/]*$ ]] && printf '%s\n' "${BASH_REMATCH[1]}"`;
  echo `return_if_num "$result"`;
}

get_first_cloud_part(){
  local col_path="$1";
  local result=`sudo ls -v "$col_path" | head -n1`;
  echo `return_if_num "$result"`;
}

get_total_parts(){
  local prefix="$1";
  local suffix="hdr_*";
  local total_parts_string=`sudo find "$prefix" -name "$suffix" | grep -vE "sonarBackup|sonarRS" | sort -V -k1.4 | tail -n1`;
  local result=`[[ $total_parts_string =~ /([^/]+)/[^/]*$ ]] && printf '%s\n' "${BASH_REMATCH[1]}"`;
  echo $(return_if_num ${result});
}

get_uniq_prints_in_file_by_col(){
  local col=${1};
  local file=${2};
  local name=${3};
  rm -f /tmp/${name}.uniqe_prints.tmp /tmp/${name}.uniqe_results.tmp;
  sort -u -k${col},${col} ${file} | awk -v var="$col" '{print $var}' >> /tmp/${name}.uniqe_prints.tmp
  touch /tmp/${name}.uniqe_results.tmp
  while read key; do
    key=${key//[\[]/\\[};
    key=${key//[\]]/\\]};
    cnt=$(grep -E ${key} ${file} | wc -l);
    msg=$(grep -E -m1 ${key} ${file});
    echo ${cnt} - ${msg} >> /tmp/${name}.uniqe_results.tmp
  done < /tmp/${name}.uniqe_prints.tmp
  rm -f /tmp/${name}.uniqe_prints.tmp
}