#!/bin/env bash

outDIR=$1
outFILE=$2
DAY_NOW=$(date -d "now" +"%d")
DAY_YESTERDAY=$(date -d "1 day ago" +"%d")

if [ ! -d $outDIR ] ; then
        echo 'Directory' $outDIR 'not found. Creating...'
        mkdir $outDIR
fi

if [ -e "$outDIR"/"$outFILE" ]; then
        rm "$outDIR"/"$outFILE"
fi

for i in "${@:3}"; do    grep -E "^([0-9]{4})-([0-9]{2})-($DAY_NOW|$DAY_YESTERDAY)T" "$i" >> "$outDIR"/"$outFILE"
    grep -E "^([0-9]{4})-([0-9]{2})-($DAY_NOW|$DAY_YESTERDAY)T" "$i" >> "$outDIR"/"$outFILE"
done
