#!/bin/bash

if [[ -z "$4" ]]; then
	last=$3
else
	last=$4
fi

for ((day = $3; day <= $last; day++)); do
	printf -v p "%04d-%02d-%02d" $1 $2 $day
	echo hive -f zero-counts.hql -d "year="$1 -d "month="$2 -d "day="$day -d "date="$p
	hive -f zero-counts.hql -d "year="$1 -d "month="$2 -d "day="$day -d "date="$p
done
