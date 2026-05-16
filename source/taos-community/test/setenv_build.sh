export TEST_DIR=`pwd`

IN_TDINTERNAL="community"
if [[ "$TEST_DIR" == *"$IN_TDINTERNAL"* ]]; then
  export TAOS_DIR=$TEST_DIR/../..
else
  export TAOS_DIR=$TEST_DIR/..
fi

export TAOS_BIN_PATH=$TAOS_DIR/debug/build/bin
export WORK_DIR=$TAOS_DIR/sim
