#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that no arguments fails correctly
marker "NO ARGS"
expect_args_error keyctl search

# check that one argument fails correctly
marker "ONE ARGS"
expect_args_error keyctl search 0

# check that two arguments fails correctly
marker "TWO ARGS"
expect_args_error keyctl search 0 0

# check that five arguments fails correctly
marker "FIVE ARGS"
expect_args_error keyctl search 0 0 0 0 0

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
