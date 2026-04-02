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

# move the key across to the session keyring
marker "MOVE KEY 1"
move_key $keyid $keyringid @s

marker "CHECK KEY LINKAGE"
list_keyring @s
expect_keyring_rlist srlist $keyid

marker "CHECK KEY REMOVED"
list_keyring $keyringid
expect_keyring_rlist rlist $keyid --absent

# Repeating the move should fail
marker "MOVE KEY 2"
move_key --fail $keyid $keyringid @s
expect_error ENOENT

marker "FORCE MOVE KEY 2"
move_key --fail -f $keyid $keyringid @s
expect_error ENOENT

# Move the key back again
marker "MOVE KEY 3"
move_key $keyid @s $keyringid

marker "MOVE KEY 4"
move_key --fail -f $keyid @s $keyringid
expect_error ENOENT

# Create a conflicting key and try to have an unforced move displace it
marker "ADD KEY 2"
create_key --new=keyid2 user lizard gizzard @s

marker "MOVE KEY 5"
move_key --fail $keyid $keyringid @s
expect_error EEXIST

marker "CHECK KEY UNMOVED"
list_keyring $keyringid
expect_keyring_rlist rlist $keyid

marker "CHECK KEY UNDISPLACED"
list_keyring @s
expect_keyring_rlist srlist $keyid --absent
expect_keyring_rlist srlist $keyid2

# Now try a forced move
marker "FORCE MOVE KEY 6"
move_key -f $keyid $keyringid @s

marker "CHECK KEY REMOVED"
list_keyring $keyringid
expect_keyring_rlist rlist $keyid --absent
expect_keyring_rlist rlist $keyid2 --absent

marker "CHECK KEY DISPLACED"
list_keyring @s
expect_keyring_rlist srlist $keyid
expect_keyring_rlist srlist $keyid2 --absent

# Remove the link (the key should be destroyed)
marker "UNLINK KEY FROM SESSION"
unlink_key --wait $keyid @s

# Removing again should fail
unlink_key --fail $keyid @s
expect_error ENOKEY

# Remove that key from the keyring should also fail
marker "UNLINK KEY FROM KEYRING"
unlink_key --fail $keyid $keyringid
expect_error ENOKEY

###############################################################################
# Create a second keyring in the first
create_keyring --new=keyring2id "zebra" $keyringid

# Move thrice between the session keyring and back
marker "LINK 2ND KEYRING TO SESSION"
move_key $keyring2id $keyringid @s
move_key $keyring2id @s $keyringid
move_key $keyring2id $keyringid @s

# Subsequent links should displace earlier links, giving us a maximum of 1 link
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

# Remove the keyring links, destroying it
marker "UNLINK 2ND KEYRING FROM SESSION"
unlink_key --wait $keyring2id @s

# Removing again should fail
marker "RE-UNLINK"
unlink_key --fail $keyring2id @s
expect_error ENOKEY
marker "RE-UNLINK 2"
unlink_key --fail $keyring2id $keyringid
expect_error ENOKEY

###############################################################################
# Create a second keyring in the session keyring
create_keyring --new=keyring2id "zebra" @s

# Add a key to the session keyring and link it into each keyring
marker "ADD KEY 3"
create_key --new=keyid user lizard gizzard @s

marker "LINK KEY"
link_key $keyid $keyringid
marker "LINK KEY 2"
link_key $keyid $keyring2id

# Try to move the links from the keyrings into the session keyring
marker "MOVE LINK"
move_key --fail $keyid $keyringid @s
expect_error EEXIST

marker "CHECK LINK"
list_keyring $keyringid
expect_keyring_rlist rlist $keyid

marker "MOVE LINK 2"
move_key --fail $keyid $keyring2id @s
expect_error EEXIST

marker "CHECK LINK 2"
list_keyring $keyring2id
expect_keyring_rlist rlist $keyid

marker "MOVE LINK 3"
move_key $keyid @s @s

marker "CHECK LINK 3"
list_keyring @s
expect_keyring_rlist srlist $keyid

# Try to force move the links from the keyrings into the session keyring
marker "FORCE MOVE LINK"
move_key -f $keyid $keyringid @s

marker "CHECK LINK 4"
list_keyring $keyringid
expect_keyring_rlist rlist $keyid --absent

marker "CHECK LINK 4s"
list_keyring @s
expect_keyring_rlist srlist $keyid

marker "FORCE MOVE LINK 2"
move_key -f $keyid $keyring2id @s

marker "CHECK LINK 5"
list_keyring $keyring2id
expect_keyring_rlist rlist $keyid --absent

marker "CHECK LINK 5s"
list_keyring @s
expect_keyring_rlist srlist $keyid

marker "FORCE MOVE LINK 3"
move_key -f $keyid @s @s

marker "CHECK LINK 6"
list_keyring @s
expect_keyring_rlist srlist $keyid

# Move the key between keyrings
marker "ROTATE"
move_key $keyid @s $keyringid
move_key $keyid $keyringid $keyring2id
move_key $keyid $keyring2id @s

marker "UNLINK KEY"
unlink_key $keyid @s

# remove the keyrings
marker "UNLINK KEYRING 1"
unlink_key --wait $keyringid @s
marker "UNLINK KEYRING 2"
unlink_key --wait $keyring2id @s

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
