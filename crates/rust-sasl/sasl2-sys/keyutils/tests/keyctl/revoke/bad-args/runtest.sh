#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that a bad key ID fails correctly
marker "CHECK BAD KEY ID"
revoke_key --fail 0
expect_error EINVAL

# check that a non-existent key ID fails correctly
marker "CHECK NON-EXISTENT KEY ID"
revoke_key --fail @t
expect_error ENOKEY

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
