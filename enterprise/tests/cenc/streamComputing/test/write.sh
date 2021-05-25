#!/bin/bash

EXE=$PWD/fileToDatabaseRaw

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 directory TQname"
elif [[ ! -d $1 ]]; then
  echo "directory $1 not exists"
elif [[ ! -f $EXE ]]; then
  echo "$EXE not exists"
else
  index=0;
  FILES=`ls *.mseed`
  for i in $FILES; do
    if [[ $index -eq 1 ]]; then
      sleep 3
      $EXE -i $i -t $2 &
    elif [[ $index -eq 2 ]]; then
      sleep 5
      $EXE -i $i -t $2 &
    elif [[ $index -eq 3 ]]; then
      sleep 3
      $EXE -i $i -t $2 &
    elif [[ $index -eq 4 ]]; then
      sleep 3
      $EXE -i $i -t $2 &
    else
      $EXE -i $i -t $2 &
    fi

    index=`expr $index + 1`
  done
fi

wait
