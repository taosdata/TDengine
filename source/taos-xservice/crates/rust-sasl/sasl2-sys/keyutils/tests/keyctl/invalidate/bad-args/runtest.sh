#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

if [ $have_key_invalidate = 0 ]
then
    toolbox_skip_test $TEST "SKIPPING DUE TO LACK OF KEY INVALIDATION"
    exit 0
fi

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that a bad key ID fails correctly
marker "CHECK INVALIDATE BAD KEY ID"
invalidate_key --fail 0
expect_error EINVAL

# create a key
marker "CREATE KEY"
create_key --new=keyid user lizard gizzard @s

# and dispose of it
marker "UNLINK KEY"
unlink_key --wait $keyid @s

# check that a non-existent key ID fails correctly
marker "CHECK INVALIDATE NON-EXISTENT KEY ID"
invalidate_key --fail $keyid
expect_error ENOKEY

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
