#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that a bad key ID fails correctly
marker "CHECK BAD KEY ID"
chown_key --fail 0 0
expect_error EINVAL
chgrp_key --fail 0 0
expect_error EINVAL
set_key_perm --fail 0 0
expect_error EINVAL

# create a non-keyring
marker "CREATE KEY"
create_key --new=keyid user lizard gizzard @s

# check that unsupported permissions aren't permitted
marker "CHECK PERMS"
set_key_perm --fail $keyid 0xffffffff
expect_error EINVAL
set_key_perm --fail $keyid 0x7f7f7f7f
expect_error EINVAL

# dispose of the key we just made
marker "UNLINK KEY"
unlink_key --wait $keyid @s

# check that a non-existent key ID fails correctly
marker "CHECK CLEAR NON-EXISTENT KEY ID"
chown_key --fail $keyid 0
expect_error ENOKEY
chgrp_key --fail $keyid 0
expect_error ENOKEY
set_key_perm --fail $keyid 0
expect_error ENOKEY

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
