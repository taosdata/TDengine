#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

marker "NO ARGS"
expect_args_error keyctl list
expect_args_error keyctl rlist

marker "TWO ARGS"
expect_args_error keyctl list 0 0
expect_args_error keyctl rlist 0 0

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
