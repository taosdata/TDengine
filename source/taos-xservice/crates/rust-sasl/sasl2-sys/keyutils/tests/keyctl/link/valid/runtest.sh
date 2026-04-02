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

# check that we can list it
marker "LIST KEYRING"
list_keyring $keyringid
expect_keyring_rlist rlist empty

# stick a key in the keyring
marker "ADD KEY"
create_key --new=keyid user lizard gizzard $keyringid

# check that we can list it
marker "LIST KEYRING WITH ONE"
list_keyring $keyringid
expect_keyring_rlist rlist $keyid

# link the key across to the session keyring
marker "LINK KEY 1"
link_key $keyid @s

marker "CHECK KEY LINKAGE"
list_keyring @s
expect_keyring_rlist srlist $keyid

# link the key across to the session keyring again and again
marker "LINK KEY 2"
link_key $keyid @s

marker "LINK KEY 3"
link_key $keyid @s

# subsequent links should displace earlier links, giving us a maximum of 1 link
marker "COUNT LINKS"
list_keyring @s
expect_keyring_rlist srlist

nlinks=0
for i in $srlist
  do
  if [ "x$i" = "x$keyid" ]
  then
      nlinks=$(($nlinks + 1))
  fi
done

if [ $nlinks != 1 ]
then
    failed
fi

# remove the links
marker "UNLINK KEY FROM SESSION"
unlink_key $keyid @s

# removing again should fail
unlink_key --fail $keyid @s
expect_error ENOENT

# remove that key from the keyring (the key should be destroyed)
marker "UNLINK KEY FROM KEYRING"
unlink_key --wait $keyid $keyringid

# and a second time should fail, but now the key doesn't exist
unlink_key --fail $keyid $keyringid
expect_error ENOKEY

# create a second keyring in the first
create_keyring --new=keyring2id "zebra" $keyringid

# link thrice across to the session keyring
marker "LINK 2ND KEYRING TO SESSION"
link_key $keyring2id @s
link_key $keyring2id @s
link_key $keyring2id @s

# subsequent links should displace earlier links, giving us a maximum of 1 link
marker "COUNT KEYRING LINKS"
list_keyring @s
expect_keyring_rlist srlist

nlinks=0
for i in $srlist
  do
  if [ "x$i" = "x$keyring2id" ]
  then
      nlinks=$(($nlinks + 1))
  fi
done

if [ $nlinks != 1 ]
then
    failed
fi

# remove the keyring links
marker "UNLINK 2ND KEYRING FROM SESSION"
unlink_key $keyring2id @s

# removing again should fail
unlink_key --fail $keyring2id @s
expect_error ENOENT

# make another keyring link
marker "LINK 2ND KEYRING TO SESSION"
link_key $keyring2id @s

# remove the first keyring we added
marker "UNLINK KEYRING"
unlink_key --wait $keyringid @s

# remove the second keyring we added
marker "UNLINK 2ND KEYRING"
unlink_key --wait $keyring2id @s

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
