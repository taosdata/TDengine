#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# create a pair of keyrings and attach them to the session keyring
marker "ADD KEYRING"
create_keyring --new=keyringid wibble @s
create_keyring --new=keyring2id wibble2 @s

# stick a key in the keyring
marker "ADD KEY"
create_key --new=keyid user lizard gizzard $keyringid

# check that we can list it
marker "LIST KEYRING WITH ONE"
list_keyring $keyringid
expect_keyring_rlist rlist $keyid

# search the session keyring for a non-existent key
marker "SEARCH SESSION FOR NON-EXISTENT KEY"
search_for_key --fail @s user snake
expect_error ENOKEY

# search the session keyring for the key
marker "SEARCH SESSION"
search_for_key --expect=$keyid @s user lizard

# search the session keyring for the key using the wrong type
marker "SEARCH SESSION USING WRONG TYPE"
search_for_key --fail @s keyring lizard $keyring2id
expect_error ENOKEY

# search the session keyring for the key and attach to second keyring
marker "SEARCH SESSION AND ATTACH"
search_for_key --expect=$keyid @s user lizard $keyring2id

# check it's attached to the second keyring
marker "CHECK ATTACHED"
list_keyring $keyring2id
expect_keyring_rlist rlist $keyid

# check the key contains what we expect
marker "CHECK PAYLOAD"
print_key $keyid
expect_payload payload "gizzard"

# detach the attachment just made
marker "DETACH KEY"
unlink_key $keyid $keyring2id

# create an overlapping key in the second keyring
create_key --new=keyid2 user lizard skin $keyring2id

# check the two keys contain what we expect
marker "CHECK PAYLOADS"
print_key $keyid
expect_payload payload "gizzard"
print_key $keyid2
expect_payload payload "skin"

# a search from the session keyring should find the first key
marker "SEARCH SESSION AGAIN"
search_for_key --expect=$keyid @s user lizard

# a search from the first keyring should find the first key
marker "SEARCH FIRST KEYRING"
search_for_key --expect=$keyid $keyringid user lizard

# a search from the second keyring should find the second key
marker "SEARCH SECOND KEYRING"
search_for_key --expect=$keyid2 $keyring2id user lizard

# link the second keyring to the first
marker "LINK FIRST KEYRING TO SECOND"
link_key $keyring2id $keyringid

# a search from the first keyring should again find the first key
marker "SEARCH FIRST KEYRING AGAIN"
search_for_key --expect=$keyid $keyringid user lizard

# revoking the first key should cause the second key to be available
revoke_key $keyid
search_for_key --expect=$keyid2 $keyringid user lizard

# get rid of the dead key
marker "UNLINK FIRST KEY"
unlink_key $keyid $keyringid

# a search from the first keyring should now find the second key
marker "SEARCH FIRST KEYRING AGAIN 2"
search_for_key --expect=$keyid2 $keyringid user lizard

# a search from the session keyring should now find the second key
marker "SEARCH SESSION KEYRING AGAIN 2"
search_for_key --expect=$keyid2 @s user lizard

# unlink the second keyring from the first
marker "UNLINK SECOND KEYRING FROM FIRST"
unlink_key $keyring2id $keyringid

# a search from the first keyring should now fail
marker "SEARCH FIRST KEYRING FOR FAIL"
search_for_key --fail $keyringid user lizard
expect_error ENOKEY

# a search from the session keyring should still find the second key
marker "SEARCH SESSION KEYRING AGAIN 3"
search_for_key --expect=$keyid2 @s user lizard

# move the second keyring into the first
marker "MOVE SECOND KEYRING INTO FIRST"
link_key $keyring2id $keyringid
unlink_key $keyring2id @s

# a search from the first keyring should now find the second key once again
marker "SEARCH FIRST KEYRING AGAIN 4"
search_for_key --expect=$keyid2 $keyringid user lizard

# Removing search permission on the first keyring should hide the key
# - This fails with EACCES as we don't have permission to initiate a search.
marker "SEARCH WITH NO-SEARCH KEYRING"
set_key_perm $keyringid 0x370000
search_for_key --fail $keyringid user lizard
expect_error EACCES

# But if we start at the session keyring, we just can't find the key
search_for_key --fail @s user lizard
expect_error ENOKEY

# putting search permission on the first keyring back again should make it
# available again
set_key_perm $keyringid 0x3f0000
search_for_key --expect=$keyid2 $keyringid user lizard

# Removing search permission on the second key should hide the key
# - This fails with ENOKEY because we're allowed to start the search, but then
#   don't find the key because there's an unsearchable keyring in the path.
marker "SEARCH WITH NO-SEARCH KEYRING2"
set_key_perm $keyring2id 0x370000
search_for_key --fail $keyringid user lizard
expect_error ENOKEY
search_for_key --fail @s user lizard
expect_error ENOKEY

# putting search permission on the second key back again should make it
# available again
set_key_perm $keyring2id 0x3f0000
search_for_key --expect=$keyid2 $keyringid user lizard

# Removing search permission on the second key should hide the key
# - This fails with EACCES because we found the key, but we're not allowed to
#   find it.
marker "SEARCH WITH NO-SEARCH KEY2"
set_key_perm $keyid2 0x370000
search_for_key --fail $keyringid user lizard
expect_error EACCES
search_for_key --fail @s user lizard
expect_error EACCES

# putting search permission on the second key back again should make it
# available again
set_key_perm $keyid2 0x3f0000
search_for_key --expect=$keyid2 $keyringid user lizard

# revoking the key should make the key unavailable
revoke_key $keyid2
search_for_key --fail $keyringid user lizard
kver=`uname -r`
case $kver in
    *.el7*)
	expect_error EKEYREVOKED
	;;
    *)
	if kernel_older_than 3.13
	then
	    expect_error ENOKEY
	else
	    expect_error EKEYREVOKED
	fi
	;;
esac

# remove the keyrings we added
marker "UNLINK KEYRING"
unlink_key $keyringid @s

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
