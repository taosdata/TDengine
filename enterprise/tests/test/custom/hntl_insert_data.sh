#!/bin/sh

# if [ $# != 4 || $# != 5 ]; then 
  # echo "argument list need input : "
  # echo "  -n nodeName"
  # echo "  -s start/stop"
  # echo "  -c clear"
  # exit 1
# fi

LOOP=1
TS=1519747200000
PREFIX=
TABLENUM=60000

while getopts "l:t:p:n:" arg 
do
  case $arg in
    l)
      LOOP=$OPTARG
      ;;    
    t)
      TS=$OPTARG
      ;; 
	p)
      PREFIX=$OPTARG
      ;; 
	n)
      TABLENUM=$OPTARG
      ;;   
	?)
      echo "unkonw argument"
      ;;
  esac
done

#echo ./hntl_insert_data hntl0.data  1483200000000 a 60000 1
#nohup ./hntl_insert_data hntl0.data  1483200000000 a 60000 1 &

#a-l 600000  ./hntl_insert_data.sh -l 1 -t 1483200000000 -p a


count=0
while [ $count -lt 13 ]; do    
	echo ./hntl_insert_data hntl${count}.data  ${TS} ${PREFIX}  ${TABLENUM} ${LOOP}
	nohup ./hntl_insert_data hntl${count}.data  ${TS} ${PREFIX}  ${TABLENUM} ${LOOP} &
	count=$((count + 1))
done
