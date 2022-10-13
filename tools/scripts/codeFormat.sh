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
    "source/libs/cache"
    "source/libs/catalog"
    "source/libs/command"
    "source/libs/executor"
    # "source/libs/function"
    # "source/libs/index"
    # "source/libs/monitor"
    # "source/libs/nodes"
    # "source/libs/parser"
    # "source/libs/planner"
    # "source/libs/qcom"
    # "source/libs/qworker"
    # "source/libs/scalar"
    # "source/libs/stream"
    # "source/libs/sync"
    "source/libs/tdb"
    "source/libs/tfs"
    # "source/libs/transport"
    "source/libs/wal"
    # "source/client"
    "source/dnode"
)

for d in ${FORMAT_DIR_LIST[@]}; do
    for f in $(find ${PRJ_ROOT_DIR}/$d/ -regex '.*\.\(cpp\|hpp\|c\|h\)'); do
        ${FORMAT_BIN} -i $f
    done
done

cd ${ORIGIN_DIR}
