#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

if [ $have_key_invalidate = 0 ]
then
    toolbox_skip_test $TEST "SKIPPING DUE TO LACK OF KEY INVALIDATION"
    exit 0
fi

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that no arguments fails correctly
marker "NO ARGS"
expect_args_error keyctl invalidate

# check that two arguments fail correctly
marker "TWO ARGS"
expect_args_error keyctl invalidate 0 0

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
