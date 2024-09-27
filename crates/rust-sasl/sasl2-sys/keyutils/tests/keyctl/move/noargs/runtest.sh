#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that no arguments fails correctly
marker "NO ARGS"
expect_args_error keyctl move
marker "NO ARGS (F)"
expect_args_error keyctl move -f

# check that one argument fails correctly
marker "ONE ARGS"
expect_args_error keyctl move 0
marker "ONE ARGS (F)"
expect_args_error keyctl move -f 0

# check that two arguments fails correctly
marker "TWO ARGS"
expect_args_error keyctl move 0 0
marker "TWO ARGS (F)"
expect_args_error keyctl move -f 0 0

# check that four arguments fails correctly
marker "FOUR ARGS"
expect_args_error keyctl link 0 0 0 0
marker "FOUR ARGS (F)"
expect_args_error keyctl link -f 0 0 0 0

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
