#!/usr/bin/env bash

get_pid(){
  echo `ps -elf|grep "$1" | grep -vE 'grep|dispatcher|tail|fuser|gdb|vim|^vi$|sonargdm|/usr/bin/sonard' | awk '{print $4}'`;
}

SONAR_PID=`get_pid sonard`;

if ! [[ ${SONAR_PID} =~ ^-?[0-9]+$ ]]; then
	echo SONARD is NOT running...
else
	echo SONARD: `grep -E "VmRSS|VmSize" /proc/"$SONAR_PID"/status` Anon: `sudo pmap "$SONAR_PID" | grep anon | awk '{ total=total+substr($2, 1, length($2)-1) } END { print (total) }'`;
fi