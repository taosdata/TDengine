#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

marker "NO ARGS"
expect_args_error keyctl read
expect_args_error keyctl pipe
expect_args_error keyctl print

marker "TWO ARGS"
expect_args_error keyctl read 0 0
expect_args_error keyctl pipe 0 0
expect_args_error keyctl print 0 0

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
