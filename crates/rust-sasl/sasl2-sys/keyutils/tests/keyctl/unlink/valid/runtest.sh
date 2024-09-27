#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# create a keyring and attach it to the session keyring
marker "ADD KEYRING"
create_keyring --new=keyringid wibble @s

# stick a key in the keyring
marker "ADD KEY"
create_key --new=keyid user lizard gizzard $keyringid

# check that we can list it
marker "LIST KEYRING WITH ONE"
list_keyring $keyringid
expect_keyring_rlist rlist $keyid

# dispose of the key and make sure it gets destroyed
marker "UNLINK KEY FROM KEYRING"
unlink_key --wait $keyid $keyringid

# trying again should fail
marker "CHECK NO UNLINK KEY FROM KEYRING"
unlink_key --fail $keyid $keyringid
expect_error ENOKEY

# check that the keyring is now empty
marker "LIST KEYRING"
list_keyring $keyringid
expect_keyring_rlist rlist empty

# stick twenty keys and twenty keyrings in the keyring
marker "ADD TWENTY KEYS"
keys=""
for ((i=0; i<20; i++))
  do
  create_key --new=x user lizard$i gizzard$i $keyringid
  keys="$keys $x"
  list_keyring $keyringid
  expect_keyring_rlist rlist $x
done

marker "ADD TWENTY KEYRINGS"
for ((i=0; i<20; i++))
  do
  create_keyring --new=x ring$i $keyringid
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

marker "SHOW"
if ! keyctl show >>$OUTPUTFILE 2>&1
then
    failed
fi

# delete all the keys from the keyring
marker "DELETE CONTENTS OF KEYRING"
for i in $keys
  do
  unlink_key --wait $i $keyringid
  unlink_key --fail $i $keyringid
  expect_error ENOKEY
done

keyctl show

# check that it's now empty
marker "LIST KEYRING"
list_keyring $keyringid
expect_keyring_rlist rlist empty

# remove the keyring we added
marker "UNLINK KEY"
unlink_key $keyringid @s

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
