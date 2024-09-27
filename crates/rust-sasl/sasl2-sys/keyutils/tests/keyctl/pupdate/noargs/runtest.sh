#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that no arguments fails correctly
marker "PUPDATE NO ARGS"
expect_args_error keyctl pupdate

# check that two arguments fail correctly
marker "PUPDATE TWO ARGS"
expect_args_error keyctl pupdate yyy xxxx

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
