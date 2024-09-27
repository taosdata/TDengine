#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# create a keyring and attach it to the session keyring
marker "CREATE KEYRING 1"
create_keyring --new=keyringid "first" @s
set_key_perm $keyringid 0x3f3f0000

# attempt to move a keyring to itself
marker "RECURSE 1"
move_key --fail $keyringid @s $keyringid
expect_error EDEADLK
marker "RECURSE 1F"
move_key --fail -f $keyringid @s $keyringid
expect_error EDEADLK

# create a second keyring in the first
marker "CREATE KEYRING 2"
create_keyring --new=keyring2id "second" $keyringid
set_key_perm $keyring2id 0x3f3f0000

# attempt to move a keyring to its child keyring
marker "RECURSE 2"
move_key --fail $keyringid @s $keyring2id
expect_error EDEADLK
marker "RECURSE 2F"
move_key --fail -f $keyringid @s $keyring2id
expect_error EDEADLK

# create a third keyring in the second
marker "CREATE KEYRING 3"
create_keyring --new=keyring3id "third" $keyring2id
set_key_perm $keyring3id 0x3f3f0000

# attempt to move a keyring to its grandchild keyring
marker "RECURSE 3"
move_key --fail $keyringid @s $keyring3id
expect_error EDEADLK
marker "RECURSE 3F"
move_key --fail -f $keyringid @s $keyring3id
expect_error EDEADLK

# create a fourth keyring in the third
marker "CREATE KEYRING 4"
create_keyring --new=keyring4id "fourth" $keyring3id
set_key_perm $keyring4id 0x3f3f0000

# attempt to move a keyring to its great grandchild keyring
marker "RECURSE 4"
move_key --fail $keyringid @s $keyring4id
expect_error EDEADLK
marker "RECURSE 4F"
move_key --fail -f $keyringid @s $keyring4id
expect_error EDEADLK

# create a fifth keyring in the fourth
marker "CREATE KEYRING 5"
create_keyring --new=keyring5id "fifth" $keyring4id
set_key_perm $keyring5id 0x3f3f0000

# attempt to move a keyring to its great great grandchild keyring
marker "RECURSE 5"
move_key --fail $keyringid @s $keyring5id
expect_error EDEADLK
marker "RECURSE 5F"
move_key --fail -f $keyringid @s $keyring5id
expect_error EDEADLK

# create a sixth keyring in the fifth
marker "CREATE KEYRING 6"
create_keyring --new=keyring6id "sixth" $keyring5id
set_key_perm $keyring6id 0x3f3f0000

# attempt to move a keyring to its great great great grandchild keyring
marker "RECURSE 6"
move_key --fail $keyringid @s $keyring6id
expect_error EDEADLK
marker "RECURSE 6F"
move_key --fail -f $keyringid @s $keyring6id
expect_error EDEADLK

# create a seventh keyring in the sixth
marker "CREATE KEYRING 7"
create_keyring --new=keyring7id "seventh" $keyring6id
set_key_perm $keyring7id 0x3f3f0000

# attempt to move a keyring to its great great great great grandchild keyring
marker "RECURSE 7"
move_key --fail $keyringid @s $keyring7id
expect_error EDEADLK
marker "RECURSE 7F"
move_key --fail -f $keyringid @s $keyring7id
expect_error EDEADLK

# create an eigth keyring in the seventh
marker "CREATE KEYRING 8"
create_keyring --new=keyring8id "eighth" @s
set_key_perm $keyring8id 0x3f3f0000
move_key $keyring8id @s $keyring7id

# attempt to move a keyring to its great great great great great grandchild keyring
marker "RECURSE 8"
move_key --fail $keyringid @s $keyring8id
expect_error EDEADLK

# create a ninth keyring in the eighth
marker "CREATE KEYRING 9"
create_keyring --new=keyring9id "ninth" @s
set_key_perm $keyring9id 0x3f3f0000
move_key $keyring9id @s $keyring8id

# attempt to move a keyring to its great great great great great great grandchild keyring
marker "RECURSE 9"
move_key --fail $keyringid @s $keyring9id
expect_error ELOOP
marker "RECURSE 9F"
move_key --fail -f $keyringid @s $keyring9id
expect_error ELOOP

# remove the first keyring we added
marker "UNLINK KEYRING"
unlink_key $keyringid @s

# create two stacks of keyrings
marker "CREATE KEYRING STACKS"
create_keyring --new=aroot "A1" @s
create_keyring --new=broot "B1" @s
a=$aroot
b=$broot

for ((i=2; i<=4; i++))
  do
  create_keyring --new=a "A$i" $a
  create_keyring --new=b "B$i" $b
done

# make sure we can't create a cycle by linking the two stacks together
marker "LINK A TO B"
link_key $aroot $b

marker "MOVE B TO A"
move_key --fail $broot @s $a
expect_error EDEADLK
marker "FORCE MOVE B TO A"
move_key --fail -f $broot @s $a
expect_error EDEADLK

marker "UNLINK A FROM B"
unlink_key $aroot $b

marker "LINK B TO A"
link_key $broot $a

marker "MOVE A TO B"
move_key --fail $aroot @s $b
expect_error EDEADLK
marker "FORCE MOVE A TO B"
move_key --fail -f $aroot @s $b
expect_error EDEADLK

marker "UNLINK B FROM A"
unlink_key $broot $a

# extend the stacks
marker "EXTEND STACKS"
create_keyring --new=a "A5" $a
create_keyring --new=b "B5" $b

# make sure we can't hide a cycle by linking the two bigger stacks together
marker "CHECK MAXDEPTH A TO B"
link_key $aroot $b
move_key --fail $broot @s $a
expect_error ELOOP
unlink_key $aroot $b

marker "CHECK MAXDEPTH B TO A"
link_key $broot $a
move_key --fail $aroot @s $b
expect_error ELOOP
unlink_key $broot $a

# remove the two stacks
marker "UNLINK STACKS"
unlink_key $aroot @s
unlink_key $broot @s

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
