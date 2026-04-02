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

# clear the empty keyring
marker "CLEAR EMPTY KEYRING"
clear_keyring $keyringid

# check that it's empty again
marker "LIST KEYRING 2"
list_keyring $keyringid
expect_keyring_rlist rlist empty

# stick a key in the keyring
marker "ADD KEY"
create_key --new=keyid user lizard gizzard $keyringid

# check that we can list it
marker "LIST KEYRING WITH ONE"
list_keyring $keyringid
expect_keyring_rlist rlist $keyid

# clear the keyring
marker "CLEAR KEYRING WITH ONE"
clear_keyring $keyringid

# check that it's now empty again
marker "LIST KEYRING 3"
list_keyring $keyringid
expect_keyring_rlist rlist empty

# stick forty keys in the keyring
marker "ADD FORTY KEYS"
keys=""
for ((i=0; i<40; i++))
  do
  create_key --new=x user lizard$i gizzard$i $keyringid
  keys="$keys $x"
  list_keyring $keyringid
  expect_keyring_rlist rlist $x
done

marker "CHECK KEYRING CONTENTS"
list_keyring $keyringid
for i in $keys
do
    expect_keyring_rlist rlist $i
done

marker "SHOW KEYRING"
if ! keyctl show >>$OUTPUTFILE 2>&1
then
    failed
fi

# clear the keyring
marker "CLEAR KEYRING WITH MANY"
clear_keyring $keyringid

# check that it's now empty yet again
marker "LIST KEYRING 4"
list_keyring $keyringid
expect_keyring_rlist rlist empty

# remove the keyring we added
marker "UNLINK KEY"
unlink_key $keyringid @s

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
