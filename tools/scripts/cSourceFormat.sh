#!/bin/bash

PRJ_ROOT_DIR=$( readlink -f -- "`dirname $0`/../.." )

ORIGIN_DIR=`pwd`

cd ${PRJ_ROOT_DIR}

FORMAT_DIR_LIST=(
    "include" 
    "source"
)

for d in ${FORMAT_DIR_LIST[@]}; do
    for f in `find ${PRJ_ROOT_DIR}/$d/ -regex '.*\.\(cpp\|hpp\|cu\|c\|h\)'`; do
        clang-format-12 -i $f
    done
done

cd ${ORIGIN_DIR}