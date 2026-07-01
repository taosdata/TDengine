#!/bin/bash
set +e

FILE=/usr/local/lib/libtaospyudf.so
if [ ! -f "$FILE" ]; then
    echo "$FILE does not exist."
    apt install -y python3 python3-dev python3-venv
    /usr/bin/python3 -m venv /udfenv
    source /udfenv/bin/activate
    pip3 install taospyudf
    ldconfig
    deactivate
else
    echo "show dependencies of $FILE"
    ldd $FILE
fi
