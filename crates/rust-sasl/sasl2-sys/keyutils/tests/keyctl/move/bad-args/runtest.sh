#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that a bad key ID fails correctly
marker "CHECK MOVE OF BAD KEY ID"
move_key --fail 0 @u @s
expect_error EINVAL

marker "CHECK MOVE FROM BAD KEYRING ID"
move_key --fail @u 0 @s
expect_error EINVAL

marker "CHECK MOVE TO BAD KEYRING ID"
move_key --fail @u @s 0
expect_error EINVAL

marker "CHECK FORCED MOVE OF BAD KEY ID"
move_key --fail -f 0 @u @s
expect_error EINVAL

marker "CHECK FORCED MOVE FROM BAD KEYRING ID"
move_key --fail -f @u 0 @s
expect_error EINVAL

marker "CHECK FORCED MOVE TO BAD KEYRING ID"
move_key --fail -f @u @s 0
expect_error EINVAL

# create a pair of non-keyrings
marker "CREATE KEY"
create_key --new=keyid user lizard gizzard @s

marker "CREATE KEY2"
create_key --new=keyid2 user zebra stripes @s

# check that linking to a non-keyring ID fails correctly
marker "CHECK MOVE FROM NON-KEYRING KEY"
move_key --fail $keyid $keyid2 @s
expect_error ENOTDIR

marker "CHECK MOVE TO NON-KEYRING KEY"
move_key --fail $keyid @s $keyid2
expect_error ENOTDIR

# dispose of the keys we were using
marker "UNLINK KEY"
unlink_key --wait $keyid @s
marker "UNLINK KEY2"
unlink_key --wait $keyid2 @s

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
