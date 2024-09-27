#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that no arguments fails correctly
marker "ADD NO ARGS"
expect_args_error keyctl padd

# check that one argument fails correctly
marker "ADD ONE ARG"
expect_args_error keyctl padd user

# check that two arguments fail correctly
marker "ADD TWO ARGS"
expect_args_error keyctl padd user wibble

# check that four arguments fail correctly
marker "ADD FOUR ARGS"
expect_args_error keyctl padd user wibble @s x

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
