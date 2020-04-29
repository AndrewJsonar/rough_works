#/bin/bash

##################################################################
# pulling repoerts from big4 machines and trigger node script to #
# upload them into google sheets                                 #
##################################################################

DATE_ONLY=`date -ud "now" "+%Y-%m-%d"`;
YESTERDAY=`date -ud "yesterday" "+%Y-%m-%d"`;
TARBALL_HOME=":/data/tarball-sonar/jsonar"
NON_TARBALL_HOME=":/data/sonar"
REMOTE_PATH="/daily_report/logs/daily_report_";
LOCAL_PATH="/home/$USER/today_reports/";

declare -a machines=("g2" "qa@main.local" "qa@dr.local" "qa@rambo.local" "qa@concur64.local" "qa@seppuku.local" "qa@gury.local" "qa@insert-test.local" "qa@sorty.local" "big4-google" "big4-aws-v3" "big4-azure" "g3" "u1-main" "u1-dr" "u1-minor");

echo "***************************************************************";
echo `date -u` - Starting to pull dailies...;

for m in "${machines[@]}"
do
        prefix="qa@";
        suffix=".local";
        short_name=${m#"$prefix"}
        short_name=${short_name%"$suffix"}
        echo `date -u` - Getting daily report from machine: "$short_name";
        if [ "$short_name" = "seppuku" ]; then
                scp "$m$NON_TARBALL_HOME$REMOTE_PATH$DATE_ONLY.csv" "$LOCAL_PATH""$short_name"_daily_report_"$DATE_ONLY".csv
        elif echo "$short_name" | grep -q "g2\|g3\|concur64\|gury\|insert-test\|sorty"; then
                scp "$m$NON_TARBALL_HOME$REMOTE_PATH$DATE_ONLY.csv" "$LOCAL_PATH""$short_name"_daily_report_"$DATE_ONLY".csv
                scp "$m$NON_TARBALL_HOME$REMOTE_PATH$DATE_ONLY.json" "$LOCAL_PATH"daily_report_"$short_name"_"$DATE_ONLY".json
                scp "$LOCAL_PATH"daily_report_"$short_name"_"$DATE_ONLY".json ganitor:/home/qa/daily_reports/incoming/
        else
                scp "$m$TARBALL_HOME$REMOTE_PATH$DATE_ONLY.csv" "$LOCAL_PATH""$short_name"_daily_report_"$DATE_ONLY".csv
                scp "$m$TARBALL_HOME$REMOTE_PATH$DATE_ONLY.json" "$LOCAL_PATH"daily_report_"$short_name"_"$DATE_ONLY".json
                scp "$LOCAL_PATH"daily_report_"$short_name"_"$DATE_ONLY".json ganitor:/home/qa/daily_reports/incoming/
        fi
done

#upload reports to google sheets
cd /data/sonar/git/qa/QANG/big4/push_reports_to_google_sheets/
node index.js
