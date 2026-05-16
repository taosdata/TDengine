#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

marker "NO ARGS"
expect_args_error keyctl instantiate
expect_args_error keyctl pinstantiate
expect_args_error keyctl negate

marker "ONE ARG"
expect_args_error keyctl instantiate 0
expect_args_error keyctl pinstantiate 0
expect_args_error keyctl negate 0

marker "TWO ARGS"
expect_args_error keyctl instantiate 0 0
expect_args_error keyctl negate 0 0

marker "THREE ARGS"
expect_args_error keyctl pinstantiate 0 0 0

marker "FOUR ARGS"
expect_args_error keyctl instantiate 0 0 0 0
expect_args_error keyctl negate 0 0 0 0

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
