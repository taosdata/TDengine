#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that a bad key ID fails correctly
marker "CHECK BAD KEY ID"
timeout_key --fail 0 10
expect_error EINVAL

# get a key
marker "CREATE KEY"
create_key --new=keyid user a a @s

# dispose of the key so we can use its ID
marker "DESTROY KEY ID"
unlink_key --wait $keyid @s

# check that a non-existent key ID fails correctly
marker "CHECK NON-EXISTENT KEY ID"
timeout_key --fail $keyid 10
expect_error ENOKEY

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
