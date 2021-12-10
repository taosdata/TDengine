#!/bin/bash
while true
do
	jmeter -n -t shortInsertData.jmx >> ./shortInsertData.log 
done
