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

# check that the key is in the keyring
marker "LIST KEYRING"
list_keyring $keyringid
expect_keyring_rlist rlist $keyid

# read the contents of the key
marker "PRINT KEY"
print_key $keyid
expect_payload payload "gizzard"

# pipe the contents of the key and add a LF as the key doesn't have one
marker "PIPE KEY"
pipe_key $keyid
echo >>$OUTPUTFILE
expect_payload payload "gizzard"

# read the key as hex
marker "READ KEY"
read_key $keyid
expect_payload payload "67697a7a 617264"

# read the contents of the keyring as hex and match it to the key ID
marker "READ KEYRING"
read_key $keyringid
tmp=`printf %08x $keyid`
if [ "$endian" = "LE" ]
then
    tmp=`echo $tmp | sed 's/\(..\)\(..\)\(..\)\(..\)/\4\3\2\1/'`
fi
expect_payload payload $tmp

# remove read permission from the key and try reading it again
# - we should still have read permission because it's searchable in our
#   keyrings
marker "REMOVE READ PERM"
set_key_perm $keyid 0x3d0000
print_key $keyid
expect_payload payload "gizzard"

# remove search permission from the key as well
# - we do not have read permission because it's no longer searchable in our
#   keyrings
marker "REMOVE SEARCH PERM"
set_key_perm $keyid 0x350000
print_key --fail $keyid
expect_error EACCES

# check that we can read it if we have to rely on possessor perms
# - we should still have read permission because it's searchable in our
#   keyrings
marker "CHECK POSSESSOR READ"
set_key_perm $keyid 0x3d000000
print_key $keyid
expect_payload payload "gizzard"

# put read permission back again
marker "REINSTATE READ PERM"
set_key_perm $keyid 0x370000
print_key $keyid
expect_payload payload "gizzard"

# revoke the key
marker "REVOKE KEY"
revoke_key $keyid
print_key --fail $keyid
expect_error EKEYREVOKED

# remove the keyring we added
marker "UNLINK KEYRING"
unlink_key $keyringid @s

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
