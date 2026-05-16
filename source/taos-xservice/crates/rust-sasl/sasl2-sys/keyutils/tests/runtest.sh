#!/bin/bash

# set the $AUTOMATED environment variable to non-zero for automated mode
# automated mode will run all the tests to completion
# non-automated mode (default) stops running the test suite on the first error
AUTOMATED=${AUTOMATED:-0}

TESTS=$*

PARENTTEST=${TEST}

if [ `id -u` != 0 ]
then
    echo "#### Some tests require root privileges." >&2
    echo "#### It is recommended that this be run as root." >&2
fi

for i in ${TESTS}; do
    export TEST=$i
    pushd $i >/dev/null
    echo "### RUNNING TEST $i"
    if [[ $AUTOMATED != 0 ]] ; then
        bash ./runtest.sh
    else
        bash ./runtest.sh || exit 1
    fi
    popd >/dev/null
done

if [ `id -u` != 0 ]
then
    echo "#### Some tests required root privileges." >&2
    echo "#### They have been tested for the appropriate failure." >&2
    echo "#### It is recommended that this be run as root." >&2
fi
