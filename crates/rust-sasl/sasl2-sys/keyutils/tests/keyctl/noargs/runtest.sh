#!/bin/bash

. ../../prepare.inc.sh
. ../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

marker "CHECK NO ARGS"
expect_args_error keyctl

if [ "`sed -n -e 3p $OUTPUTFILE | cut -d: -f1`" != "Format" ]
then
    failed
fi


echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
