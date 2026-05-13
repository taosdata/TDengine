#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that no arguments fails correctly
marker "ADD NO ARGS"
expect_args_error keyctl add

# check that one argument fails correctly
marker "ADD ONE ARG"
expect_args_error keyctl add user

# check that two arguments fail correctly
marker "ADD TWO ARGS"
expect_args_error keyctl add user wibble

# check that three arguments fail correctly
marker "ADD THREE ARGS"
expect_args_error keyctl add user wibble stuff

# check that five arguments fail correctly
marker "ADD FIVE ARGS"
expect_args_error keyctl add user wibble stuff @s x

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
