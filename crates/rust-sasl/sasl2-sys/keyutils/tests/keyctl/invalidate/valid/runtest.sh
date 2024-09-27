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

# create a keyring and attach it to the session keyring
marker "ADD KEYRING"
create_keyring --new=keyringid wibble @s

# check that we have an empty keyring
marker "LIST KEYRING"
list_keyring $keyringid
expect_keyring_rlist rlist empty

# stick a key in the keyring
marker "ADD KEY"
create_key --new=keyid user lizard gizzard $keyringid

# check that we can list it
marker "LIST KEYRING 2"
list_keyring $keyringid
expect_keyring_rlist rlist $keyid

# invalidate the key
marker "INVALIDATE KEY"
invalidate_key $keyid

# need to wait for the gc
sleep 1

# check that it's now empty again
marker "LIST KEYRING 3"
list_keyring $keyringid
expect_keyring_rlist rlist empty

# stick another key in the keyring
marker "ADD KEY"
create_key --new=keyid user lizard2 gizzard $keyringid

# check that we can list it
marker "LIST KEYRING 4"
list_keyring $keyringid
expect_keyring_rlist rlist $keyid

# invalidate the keyring
marker "INVALIDATE KEYRING"
invalidate_key $keyringid

# need to wait for the gc
sleep 1

# check that the keyring no longer exists
marker "CHECK KEYRING"
list_keyring --fail $keyringid
expect_error ENOKEY

# check that the key got gc'd also
marker "CHECK KEY"
describe_key --fail $keyid
expect_error ENOKEY

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
