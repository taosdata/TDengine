#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

if [ $have_dh_compute = 0 ]
then
    toolbox_skip_test $TEST "SKIPPING DUE TO LACK OF DIFFIE-HELLMAN"
    exit 0
fi

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

marker "NO ARGS"
expect_args_error keyctl dh_compute

marker "TWO ARGS"
expect_args_error keyctl dh_compute 0 0

marker "FOUR ARGS"
expect_args_error keyctl dh_compute 0 0 0 0

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
