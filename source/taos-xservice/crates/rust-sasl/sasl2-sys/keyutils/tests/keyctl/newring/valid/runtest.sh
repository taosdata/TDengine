#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# create a keyring and attach it to the session keyring
marker "ADD KEYRING"
create_keyring --new=keyringid wibble @s

# check that we now have an empty keyring
marker "LIST KEYRING"
list_keyring $keyringid
expect_keyring_rlist rlist empty

# check that creating a second keyring of the same name displaces the first
marker "ADD KEYRING AGAIN"
create_keyring --new=keyringid2 wibble @s

# should be different keyrings
if [ "x$keyringid" == "x$keyringid2" ]
then
    failed
fi

# the first should no longer exist in the session keyring
marker "LIST SESSION KEYRING"
list_keyring @s
expect_keyring_rlist sessionrlist $keyringid --absent

# and should no longer be accessible
marker "VALIDATE NEW KEYRING"
pause_till_key_destroyed $keyringid
describe_key --fail $keyringid
expect_error ENOKEY

# list the session keyring
marker "LIST SESSION KEYRING2"
list_keyring @s
expect_keyring_rlist sessionrlist $keyringid2

# validate the new keyring's name and type
marker "VALIDATE NEW KEYRING2"
describe_key $keyringid2
expect_key_rdesc rdesc 'keyring@.*@wibble'

# remove the keyring we added
marker "UNLINK KEY"
unlink_key $keyringid2 @s

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
