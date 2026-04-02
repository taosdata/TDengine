#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# create a keyring and attach it to the session keyring
marker "ADD KEYRING"
create_keyring --new=keyringid wibble @s

# validate the new keyring's name and type
marker "VALIDATE KEYRING"
describe_key $keyringid
expect_key_rdesc rdesc 'keyring@.*@wibble'

# validate a pretty description of the keyring
marker "VALIDATE PRETTY KEYRING"
pretty_describe_key $keyringid
expect_key_rdesc pdesc " *$keyringid: [-avrwsl]* *[-0-9]* *[-0-9]* keyring: wibble"

# check that we have an empty keyring
marker "LIST KEYRING"
list_keyring $keyringid
expect_keyring_rlist rlist empty

# stick a key in the keyring
marker "ADD KEY"
create_key --new=keyid user lizard gizzard $keyringid

# validate the new key's name and type
marker "VALIDATE KEY"
describe_key $keyid
expect_key_rdesc rdesc 'user@.*@lizard'

# validate a pretty description of the key
marker "VALIDATE PRETTY KEY"
pretty_describe_key $keyid
expect_key_rdesc pdesc " *$keyid: [-avrwsl]* *[0-9]* *[-0-9]* user: lizard"

# turn off view permission on the key
marker "DISABLE VIEW PERM"
set_key_perm $keyid 0x3e0000
describe_key --fail $keyid
expect_error EACCES

# turn on view permission on the key
marker "REINSTATE VIEW PERM"
set_key_perm $keyid 0x3f0000
describe_key $keyid

# revoke the key
marker "REVOKE KEY"
revoke_key $keyid
describe_key --fail $keyid
expect_error EKEYREVOKED

# remove the keyring we added
marker "UNLINK KEY"
unlink_key $keyringid @s

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
