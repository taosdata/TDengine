
set -e 

CURR_DIR=$(pwd)
SCRIPT_DIR="$(dirname $(readlink -f $0))"
CODE_DIR="$(readlink -f ${SCRIPT_DIR}/..)"
DRIVER_DIR=${CODE_DIR}/driver

cd ${DRIVER_DIR}/linux/python2
python setup.py sdist bdist_wheel

cd ${DRIVER_DIR}/linux/python3
python setup.py sdist bdist_wheel

cd ${DRIVER_DIR}/windows/python2
python setup.py sdist bdist_wheel

cd ${DRIVER_DIR}/windows/python3
python setup.py sdist bdist_wheel

cd ${CURR_DIR}
