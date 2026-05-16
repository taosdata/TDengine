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

# set a silly timeout on the key
marker "SET BIG TIMEOUT"
timeout_key $keyid 10000000

# check we can still read the key's payload
marker "CHECK READ PAYLOAD 2"
print_key $keyid
expect_payload kpayload "gizzard"

# set a small timeout on the key
marker "SET SMALL TIMEOUT"
timeout_key $keyid 2

marker "WAIT FOR TIMEOUT"
sleep_at_least 2

# check the key has expired
marker "CHECK NO READ PAYLOAD"
print_key --fail $keyid
if kernel_at_or_later_than 3.8 && kernel_older_than 3.13 &&
	! rhel7_kernel_at_or_later_than 3.10.0-42.el7
then
	expect_error ENOKEY
else
	expect_error EKEYEXPIRED
fi

# check revocation doesn't work
marker "CHECK NO REVOKE KEY"
revoke_key --fail $keyid
expect_error EKEYEXPIRED

# check timeout setting doesn't work
marker "CHECK NO TIMEOUT KEY"
timeout_key --fail $keyid 20
expect_error EKEYEXPIRED

# remove the key we added
marker "UNLINK KEY"
unlink_key $keyid $keyringid

###############################################################################
# create a key and attach it to the new keyring
marker "ADD KEY"
create_key --new=keyid user lizard gizzard $keyringid

# set a silly timeout on the key
marker "SET BIG TIMEOUT"
timeout_key $keyid 10000000

# revoke the key
marker "REVOKE KEY"
revoke_key $keyid

# check we can no longer set the key's timeout
marker "CHECK NO SET KEY TIMEOUT"
timeout_key --fail $keyid 20
expect_error EKEYREVOKED

# remove the key we added
marker "UNLINK KEY"
unlink_key $keyid $keyringid

# revoke the keyring
marker "TIMEOUT KEYRING"
timeout_key $keyringid 1

marker "WAIT FOR KEYRING TIMEOUT"
sleep_at_least 1

# listing the session keyring should fail
marker "CHECK NO LIST SESSION KEYRING"
list_keyring --fail $keyringid
if kernel_at_or_later_than 3.8 && kernel_older_than 3.13 &&
	! rhel7_kernel_at_or_later_than 3.10.0-42.el7
then
	expect_error ENOKEY
else
	expect_error EKEYEXPIRED
fi

# validating the new keyring's name and type should also fail
marker "CHECK NO VALIDATE KEYRING"
describe_key --fail $keyringid
expect_error EKEYEXPIRED

# validating the new keyring's name and type should also fail
marker "CHECK NO SET KEYRING TIMEOUT"
timeout_key --fail $keyringid 20
expect_error EKEYEXPIRED

# validating the new keyring's name and type should also fail
marker "CHECK NO INVALIDATE KEYRING"
invalidate_key --fail $keyringid
expect_error EKEYEXPIRED

# validating the new keyring's name and type should also fail
marker "CHECK NO REVOKE KEYRING"
revoke_key --fail $keyringid
expect_error EKEYEXPIRED

# remove the keyring we added
marker "UNLINK KEYRING"
unlink_key $keyringid @s

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
