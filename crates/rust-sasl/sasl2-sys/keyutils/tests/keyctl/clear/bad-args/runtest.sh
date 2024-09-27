#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that a bad key ID fails correctly
marker "CHECK CLEAR BAD KEY ID"
clear_keyring --fail 0
expect_error EINVAL

# create a non-keyring
marker "CREATE KEY"
create_key --new=keyid user lizard gizzard @s

# check that a non-keyring ID fails correctly
marker "CHECK CLEAR NON-KEYRING KEY"
clear_keyring --fail $keyid
expect_error ENOTDIR

# dispose of the key we were using
marker "UNLINK KEY"
unlink_key --wait $keyid @s

# check that a non-existent key ID fails correctly
marker "CHECK CLEAR NON-EXISTENT KEY ID"
clear_keyring --fail $keyid
expect_error ENOKEY

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
