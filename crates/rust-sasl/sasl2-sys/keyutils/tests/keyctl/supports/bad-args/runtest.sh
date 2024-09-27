#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that two arguments fails correctly
marker "TWO ARGS"
expect_args_error keyctl support 0 0

# check that three arguments fails correctly
marker "THREE ARGS"
expect_args_error keyctl support 0 0 0

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
