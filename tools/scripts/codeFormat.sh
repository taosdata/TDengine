#!/bin/bash

PRJ_ROOT_DIR=$(readlink -f -- "$(dirname $0)/../..")
FORMAT_BIN=clang-format-14

ORIGIN_DIR=$(pwd)

cd ${PRJ_ROOT_DIR}

FORMAT_DIR_LIST=(
    "${PRJ_ROOT_DIR}/include"
    "${PRJ_ROOT_DIR}/source/os"
    "${PRJ_ROOT_DIR}/source/util"
    "${PRJ_ROOT_DIR}/source/common"
    "${PRJ_ROOT_DIR}/source/libs/cache"
    "${PRJ_ROOT_DIR}/source/libs/catalog"
    "${PRJ_ROOT_DIR}/source/libs/command"
    "${PRJ_ROOT_DIR}/source/libs/executor"
    "${PRJ_ROOT_DIR}/source/libs/function"
    "${PRJ_ROOT_DIR}/source/libs/index"
    "${PRJ_ROOT_DIR}/source/libs/monitor"
    "${PRJ_ROOT_DIR}/source/libs/nodes"
    # "${PRJ_ROOT_DIR}/source/libs/parser"
    "${PRJ_ROOT_DIR}/source/libs/planner"
    "${PRJ_ROOT_DIR}/source/libs/qcom"
    "${PRJ_ROOT_DIR}/source/libs/qworker"
    "${PRJ_ROOT_DIR}/source/libs/scalar"
    "${PRJ_ROOT_DIR}/source/libs/stream"
    "${PRJ_ROOT_DIR}/source/libs/sync"
    "${PRJ_ROOT_DIR}/source/libs/tdb"
    "${PRJ_ROOT_DIR}/source/libs/tfs"
    "${PRJ_ROOT_DIR}/source/libs/transport"
    "${PRJ_ROOT_DIR}/source/libs/wal"
    "${PRJ_ROOT_DIR}/source/client/inc"
    "${PRJ_ROOT_DIR}/source/client/src"
    "${PRJ_ROOT_DIR}/source/client/test"
    "${PRJ_ROOT_DIR}/source/dnode"
)

EXCLUDE_DIR_LIST=(
)

EXCLUDE_FILE_LIST=(
    "source/libs/parser/sql.c"
)

for d in ${FORMAT_DIR_LIST[@]}; do
    for f in $(find $d -type f -regex '.*\.\(cpp\|hpp\|c\|h\)'); do
        ${FORMAT_BIN} -i $f
    done
done

cd ${ORIGIN_DIR}

# find source -type f -regex '.*\.\(cpp\|hpp\|c\|h\)' ! -name
