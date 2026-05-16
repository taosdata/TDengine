#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that a bad key ID fails correctly
marker "CHECK LINK FROM BAD KEY ID"
link_key --fail 0 @s
expect_error EINVAL

marker "CHECK LINK TO BAD KEY ID"
link_key --fail @s 0
expect_error EINVAL

# create a non-keyring
marker "CREATE KEY"
create_key --new=keyid user lizard gizzard @s

# check that linking to a non-keyring ID fails correctly
marker "CHECK LINK TO NON-KEYRING KEY"
link_key --fail @s $keyid
expect_error ENOTDIR

# dispose of the key we were using
marker "UNLINK KEY"
unlink_key --wait $keyid @s

# check that a non-existent key ID fails correctly
marker "CHECK LINK TO NON-EXISTENT KEY ID"
link_key --fail @s $keyid
expect_error ENOKEY

marker "CHECK LINK FROM NON-EXISTENT KEY ID"
link_key --fail $keyid @s
expect_error ENOKEY

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
