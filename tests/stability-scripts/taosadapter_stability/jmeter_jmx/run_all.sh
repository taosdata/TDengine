#!/bin/bash
jmeter -n -t createStaticData.jmx >> createStaticData.log
jmeter -n -t longInsertData.jmx >> longInsertData.log
jmeter -n -t error_insert.jmx >> error_insert.log
jmeter -n -t query.jmx >> query.log
while true
do
	jmeter -n -t shortInsertData.jmx >> ./shortInsertData.log
done
