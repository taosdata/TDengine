#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that no arguments fails correctly
marker "WATCH NO ARGS"
expect_args_error keyctl watch

# check that two arguments fail correctly
marker "WATCH TWO ARGS"
expect_args_error keyctl watch @s @s

# Try a dodgy filter
marker "CHECK BAD FILTER"
expect_args_error keyctl watch_key -f 0

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
