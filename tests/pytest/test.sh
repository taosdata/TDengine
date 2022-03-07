EXEC_DIR=`dirname "$0"`
if [[ $EXEC_DIR != "." ]]
then
    echo "ERROR: Please execute `basename "$0"` in its own directory (for now anyway, pardon the dust)"
    exit -1
fi
CURR_DIR=`pwd`
IN_TDINTERNAL="community"
if [[ "$CURR_DIR" == *"$IN_TDINTERNAL"* ]]; then
  TAOS_DIR=$CURR_DIR/../../..
else
  TAOS_DIR=$CURR_DIR/../..
fi
TAOSD_DIR=`find $TAOS_DIR -name "taosd"|grep bin|head -n1`
LIB_DIR=`echo $TAOSD_DIR|rev|cut -d '/' -f 3,4,5,6|rev`/lib
export PYTHONPATH=$(pwd)/../../src/connector/python
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LIB_DIR

if [[ "$1" == *"test.py"* ]]; then
  python3 ./test.py $@
else
  python3 $1 $@
fi
