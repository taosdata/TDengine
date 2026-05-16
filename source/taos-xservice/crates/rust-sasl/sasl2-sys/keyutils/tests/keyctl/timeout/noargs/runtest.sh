#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh

# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that no arguments fails correctly
marker "ADD NO ARGS"
expect_args_error keyctl timeout

# check that one argument fails correctly
marker "ADD ONE ARG"
expect_args_error keyctl timeout 0

# check that three arguments fail correctly
marker "ADD THREE ARGS"
expect_args_error keyctl timeout 0 0 0

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
