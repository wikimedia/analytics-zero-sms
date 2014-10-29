#!/bin/bash

#                $1                 $2     $3   $4 $5 %6 $7
# ./run-clone.sh wmf_raw.webrequest 515-05 2014 10 11 0  23

if [[ -z "$7" ]]; then
	last=$6
else
	last=$7
fi

for ((hour = $6; hour <= $last; hour++)); do
	printf -v t "tmp_%04d_%02d_%02d_%02d" $3 $4 $5 $hour
	echo hive -f clone-xcs.hql -d "table="$1 -d "xcs="$2 -d "year="$3 -d "month="$4 -d "day="$5 -d "hour="$hour -d "table="$t
	export HADOOP_HEAPSIZE=1024 && hive -f clone-xcs.hql -d "xcs="$2 -d "year="$3 -d "month="$4 -d "day="$5 -d "hour="$hour -d "table="$t
done
