#!/bin/bash

#                   $1                 $2   $3 $4 %5 $6              $7               $8
# ./run-hivezero.sh wmf_raw.webrequest 2014 10 1  31 zero_webstats   zero-counts.hql
# ./run-hivezero.sh wmf.webrequest     2014 10 1  31 zero_webstats2  zero-counts2.hql
# ./run-hivezero.sh webreq_archive     2014 10 1  31 zero_webstats__ zero-counts.hql  overwrite

set -e

if [[ -z "$5" ]]; then
	last=$4
else
	last=$5
fi

table=$1
year=$2

if [[ "$3" -eq "all" ]]; then
	monthFrom=1
	monthTo=12
else
	monthFrom=$3
	monthTo=$3
fi

if [[ -z "$6" ]]; then
	dsttable=zero_webstats2
else
	dsttable=$6
fi

if [[ -z "$7" ]]; then
	script=zero-counts2.hql
else
	script=$7
fi


for ((month = $monthFrom; month <= $monthTo; month++)); do
for ((day = $4; day <= $last; day++)); do

	printf -v date "%04d-%02d-%02d" $year $month $day

	if [ "$( date -d "$date" +%F 2>&1 | grep invalid )" = "" ] ; then

		if [[ "$table" == 'wmf_raw.webrequest' ]]; then
			path="/mnt/hdfs/wmf/data/raw/webrequest/webrequest_upload/hourly/$year/$(printf "%02d" $month)/$(printf "%02d" $day)/23"
		elif [[ "$table" == 'wmf.webrequest' ]]; then
			path="/mnt/hdfs/wmf/data/wmf/webrequest/webrequest_source=mobile/year=$year/month=$month/day=$day/hour=23"
		else
			path="/mnt/hdfs/user/hive/warehouse/yurik.db/$table/year=$year/month=$month/day=$day"
		fi
		if [ ! -d "$path" ]; then
			echo "***** '$path' does not exists"
			continue
		fi
		pathSize=$(du -sb $path | cut -f1)
		if (( $pathSize < 50000 )); then
			echo "***** '$path' is $pathSize bytes -- too small"
			continue
		fi

		path="/mnt/hdfs/user/hive/warehouse/yurik.db/"$dsttable"/date="$date
		echo "***** Checking if '$path' exists"
		if [ -d $path ]; then
			if [ "$8" == "overwrite" ]; then
				echo "***** Droping partition '$date'"
				hive -e "use yurik; ALTER TABLE "$dsttable" DROP IF EXISTS PARTITION(date = '$date');"
			else
				echo "***** Skipping '$date'"
				continue
			fi
		fi
		echo -e "*****\n*****\n*****\n*****"
		echo "*****" hive -f $script -d "table="$table  -d "dsttable="$dsttable -d "year="$year -d "month="$month -d "day="$day -d "date="$date
		export HADOOP_HEAPSIZE=2048 && hive -f $script -d "table="$table  -d "dsttable="$dsttable -d "year="$year -d "month="$month -d "day="$day -d "date="$date

	fi

done
done
