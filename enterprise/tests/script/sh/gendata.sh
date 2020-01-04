#!/bin/sh
echo "input parameter, fileName:$1, numOfRows:$2"
fileName=$1
numOfRows=`expr $2 - 1`

startTime=1568891862056
echo "$startTime,\"XXX\",\"FDD-DX-AAAS\",\"46***306\",\"\",\"N/A\",\"1\",\"N/A\",\"N/A\",\"N/A\",\"0\"" > $fileName

for i in  `seq $numOfRows`;do
  ts=`expr $startTime + $i`
  echo "$ts,\"XXX\",\"FDD-DX-AAAS\",\"46***306\",\"\",\"N/A\",\"1\",\"N/A\",\"N/A\",\"N/A\",\"0\"" >> $fileName
done  
