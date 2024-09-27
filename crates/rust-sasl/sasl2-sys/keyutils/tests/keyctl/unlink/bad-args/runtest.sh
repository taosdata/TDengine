#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that a bad key ID fails correctly
marker "CHECK UNLINK BAD KEY ID"
unlink_key --fail 0 @s
expect_error EINVAL

marker "CHECK UNLINK FROM BAD KEY ID"
unlink_key --fail @s 0
expect_error EINVAL

# create a non-keyring
marker "CREATE KEY"
create_key --new=keyid user lizard gizzard @s

# check that unlinking from a non-keyring ID fails correctly
marker "CHECK UNLINK FROM NON-KEYRING KEY"
unlink_key --fail @s $keyid
expect_error ENOTDIR

# dispose of the key we were using
marker "UNLINK KEY"
unlink_key --wait $keyid @s

# check that a non-existent key ID fails correctly
marker "CHECK UNLINK FROM NON-EXISTENT KEY ID"
unlink_key --fail @s $keyid
expect_error ENOKEY

marker "CHECK UNLINK NON-EXISTENT KEY ID"
unlink_key --fail $keyid @s
expect_error ENOKEY

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
