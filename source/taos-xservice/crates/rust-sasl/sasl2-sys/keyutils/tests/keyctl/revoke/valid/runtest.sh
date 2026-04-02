#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh

# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# create a keyring and attach it to the session keyring
marker "ADD KEYRING"
create_keyring --new=keyringid wibble @s

# create a key and attach it to the new keyring
marker "ADD KEY"
create_key --new=keyid user lizard gizzard $keyringid

# check that we can list the keyring
marker "LIST KEYRING"
list_keyring $keyringid
expect_keyring_rlist ringlist $keyid

# check we can read the key description
marker "CHECK VALIDATE KEY"
describe_key $keyid
expect_key_rdesc kdesc 'user@.*@lizard'

# check we can read the key's payload
marker "CHECK READ PAYLOAD"
print_key $keyid
expect_payload kpayload "gizzard"

# revoke the key
marker "REVOKE KEY"
revoke_key $keyid

# check we can no longer read the key description
marker "CHECK NO VALIDATE KEY"
describe_key --fail $keyid
expect_error EKEYREVOKED

# check we can no longer read the key's payload
marker "CHECK NO READ PAYLOAD"
print_key --fail $keyid
expect_error EKEYREVOKED

# remove the key we added
marker "UNLINK KEY"
unlink_key $keyid $keyringid

# revoke the keyring
marker "REVOKE KEYRING"
revoke_key $keyringid

# listing the session keyring should fail
marker "CHECK NO LIST SESSION KEYRING"
list_keyring --fail $keyringid
expect_error EKEYREVOKED

# validating the new keyring's name and type should also fail
marker "CHECK NO VALIDATE KEYRING"
describe_key --fail $keyringid
expect_error EKEYREVOKED

# remove the keyring we added
marker "UNLINK KEYRING"
unlink_key $keyringid @s

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
