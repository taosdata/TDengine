#!/bin/bash

for N in -1 0 1 10000 10001
do
	for l in 1023 1024 1073741824 1073741825 
	do
		for S in udp tcp 
		do
			taos -n speed -P 6030 -N $N -l $l -S $S
		done
	done
done
