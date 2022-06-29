#!/bin/bash
for i in {1..100}
do 
	echo $i
        python3 ./test.py -f query/nestedQuery/nestedQuery_datacheck.py >>log 2>&1
        if [ $? -eq 0 ] 
	then
		echo success
	else
		echo failed
		break
	fi
done
