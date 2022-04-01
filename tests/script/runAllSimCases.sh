#!/bin/bash

##################################################
# 
# Do simulation test 
#
##################################################

set -e
#set -x

while read line
do
  firstChar=`echo ${line:0:1}`
  if [[ -n "$line" ]]  && [[ $firstChar != "#" ]]; then
    echo "======== $line ========"
    $line 
  fi
done < ./jenkins/basic.txt


