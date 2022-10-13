#!/bin/bash

PRJ_ROOT_DIR=$(readlink -f -- "$(dirname $0)/../..")
FORMAT_BIN=clang-format-14

ORIGIN_DIR=$(pwd)

cd ${PRJ_ROOT_DIR}

FORMAT_DIR_LIST=(
    "include"
    "source/os"
    "source/util"
    "source/common"
    # "source/libs"
    # "source/client"
    "source/dnode"
)

for d in ${FORMAT_DIR_LIST[@]}; do
    for f in $(find ${PRJ_ROOT_DIR}/$d/ -regex '.*\.\(cpp\|hpp\|c\|h\)'); do
        ${FORMAT_BIN} -i $f
    done
done

cd ${ORIGIN_DIR}
