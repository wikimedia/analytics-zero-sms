#!/bin/bash
# ./run-clone.sh 515-05 2014 10 11 0 23

if [[ -z "$6" ]]; then
	last=$5
else
	last=$6
fi

for ((hour = $5; hour <= $last; hour++)); do
	printf -v t "tmp_%04d_%02d_%02d_%02d" $2 $3 $4 $hour
	echo hive -f clone-xcs.hql -d "xcs="$1 -d "year="$2 -d "month="$3 -d "day="$4 -d "hour="$hour -d "table="$t
	export HADOOP_HEAPSIZE=1024 && hive -f clone-xcs.hql -d "xcs="$1 -d "year="$2 -d "month="$3 -d "day="$4 -d "hour="$hour -d "table="$t
done
