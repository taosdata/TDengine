#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# attempt to update the session keyring
marker "CHECK UPDATE SESSION KEYRING"
echo -n "a" | pupdate_key --fail @s
expect_error EOPNOTSUPP

# attempt to update an invalid key
marker "CHECK UPDATE INVALID KEY"
echo -n "a" | pupdate_key --fail 0
expect_error EINVAL

# add a user key to the session keyring for us to play with
marker "ADD USER KEY"
create_key --new=keyid user wibble stuff @s

# remove the key we just added
marker "UNLINK KEY"
unlink_key --wait $keyid @s

# it should fail when we attempt to update it
marker "UPDATE UNLINKED KEY"
echo -n "a" | pupdate_key --fail $keyid
expect_error ENOKEY

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
