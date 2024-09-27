#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
if [ $skip_install_required -eq 1 ]
then
    echo "++++ SKIPPING TEST" >$OUTPUTFILE
    marker "SKIP BECAUSE TEST REQUIRES FULL INSTALL (for /sbin/request-key)"
    toolbox_report_result $TEST PASS
    exit 0
else
    echo "++++ BEGINNING TEST" >$OUTPUTFILE
fi

marker "NO ARGS"
expect_args_error keyctl request
expect_args_error keyctl request2
expect_args_error keyctl prequest2

marker "ONE ARG"
expect_args_error keyctl request 0
expect_args_error keyctl request2 0
expect_args_error keyctl prequest2 0

marker "TWO ARGS"
expect_args_error keyctl request2 0 0

marker "FOUR ARGS"
expect_args_error keyctl request 0 0 0 0
expect_args_error keyctl prequest2 0 0 0 0

marker "FIVE ARGS"
expect_args_error keyctl request2 0 0 0 0 0

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
