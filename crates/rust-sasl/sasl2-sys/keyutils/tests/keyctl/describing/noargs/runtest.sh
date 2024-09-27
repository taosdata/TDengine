#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

marker "NO ARGS"
expect_args_error keyctl describe
expect_args_error keyctl rdescribe

marker "TWO ARGS"
expect_args_error keyctl describe 0 0

marker "THREE ARGS"
expect_args_error keyctl rdescribe 0 0 0

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
