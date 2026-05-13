#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh

# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that no arguments fails correctly
marker "ADD NO ARGS"
expect_args_error keyctl revoke

# check that two arguments fail correctly
marker "ADD TWO ARGS"
expect_args_error keyctl revoke 0 0

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
