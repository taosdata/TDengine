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

# check that we have an empty keyring
marker "LIST KEYRING"
list_keyring $keyringid
expect_keyring_rlist rlist empty

marker "PRETTY LIST KEYRING"
pretty_list_keyring $keyringid
expect_payload payload "keyring is empty"

# stick a key in the keyring
marker "ADD KEY"
create_key --new=keyid user lizard gizzard $keyringid

# check that we can list it
marker "LIST KEYRING WITH ONE"
list_keyring $keyringid
expect_keyring_rlist rlist $keyid

# check that we can pretty list it
marker "PRETTY LIST KEYRING WITH ONE"
pretty_list_keyring $keyringid
expect_payload payload

if ! expr "$payload" : " *$keyid:.*user: lizard" >&/dev/null
then
    failed
fi

# stick a second key in the keyring
marker "ADD KEY 2"
create_key --new=keyid2 user snake skin $keyringid

# check that we can see both keys
marker "LIST KEYRING WITH TWO"
list_keyring $keyringid
expect_keyring_rlist rlist

if [ "x$rlist" != "x$keyid $keyid2" ]
then
    failed
fi

# check that we can see both keys prettily
marker "PRETTY LIST KEYRING WITH TWO"
pretty_list_keyring $keyringid
prlist=""
for i in `tail -2 $OUTPUTFILE | cut -d: -f1 | sed -e 's@ +@@g'`
  do
  prlist="$prlist $i"
done

if [ "x$prlist" != "x $keyid $keyid2" ]
then
    failed
fi

# turn off read permission on the keyring
marker "DISABLE READ PERM"
set_key_perm $keyringid 0x3d0000
list_keyring $keyringid

# turn off read and search permission on the keyring
marker "DISABLE SEARCH PERM"
set_key_perm $keyringid 0x350000
list_keyring --fail $keyringid
expect_error EACCES

# turn on read permission on the keyring
marker "REINSTATE READ PERM"
set_key_perm $keyringid 0x370000
list_keyring $keyringid

# revoke the keyring
marker "REVOKE KEYRING"
revoke_key $keyringid
list_keyring --fail $keyringid
expect_error EKEYREVOKED

# remove the keyring we added
marker "UNLINK KEY"
unlink_key $keyringid @s

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
