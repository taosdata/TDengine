#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

if [ $have_notify = 0 ]
then
    toolbox_skip_test $TEST "SKIPPING DUE TO LACK OF NOTIFICATION SUPPORT"
    exit 0
fi

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# Attempt to watch an invalid key
marker "CHECK WATCH INVALID KEY"
watch_key --fail 0
expect_error EINVAL

# Add a user key to the session keyring for us to play with
marker "ADD USER KEY"
create_key --new=keyid user wibble stuff @s

# Remove the key we just added
marker "UNLINK KEY"
unlink_key --wait $keyid @s

# It should fail when we attempt to watch it
marker "UPDATE UNLINKED KEY"
watch_key --fail $keyid
expect_error ENOKEY

# Try a number of dodgy filters
marker "CHECK DODGY FILTERS"
watch_key --fail2 -fZ @s
watch_key --fail2 -fZ -fQ @s
watch_key --fail2 -f: @s

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
