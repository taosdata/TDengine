#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that an empty key type fails correctly
marker "CHECK EMPTY KEY TYPE"
search_for_key --fail @s "" wibble
expect_error EINVAL
search_for_key --fail @s "" wibble @p
expect_error EINVAL

# check that an unsupported key type fails correctly
marker "CHECK UNSUPPORTED KEY TYPE"
search_for_key --fail @s lizardsgizzards wibble
expect_error ENOKEY
search_for_key --fail @s lizardsgizzards wibble @p
expect_error ENOKEY

# check that an invalid key type fails correctly
marker "CHECK INVALID KEY TYPE"
search_for_key --fail @s .user wibble
expect_error EPERM
search_for_key --fail @s .user wibble @p
expect_error EPERM

# check that an overlong key type fails correctly
marker "CHECK OVERLONG KEY TYPE"
search_for_key --fail @s $maxtype wibble
expect_error ENOKEY
search_for_key --fail @s a$maxtype wibble @p
expect_error EINVAL

# check that an max length key description works correctly (4095 inc NUL)
marker "CHECK MAXLEN DESC"
search_for_key --fail @s user $maxdesc
expect_error ENOKEY

search_for_key --fail @s user $maxdesc @p
expect_error ENOKEY

# This doesn't work on MIPS earlier than 3.19 because of a kernel bug
kver=`uname -r`
kmch=`uname -m`
if kernel_at_or_later_than 3.19 ||
	[ "$kmch" != "mips" -a "$kmch" != "mips64" ]
then
	# check that an overlong key description fails correctly (>4095 inc NUL)
	marker "CHECK OVERLONG DESC"
	search_for_key --fail @s user a$maxdesc
	expect_error EINVAL
fi

search_for_key --fail @s user a$maxdesc @p
expect_error EINVAL

# check that a bad key ID fails correctly
marker "CHECK BAD KEY ID"
search_for_key --fail @s user wibble -2000
expect_error EINVAL

# create a non-keyring key
marker "CREATE KEY"
create_key --new=keyid user a a @s

# search the non-keyring key
marker "SEARCH KEY"
search_for_key --fail $keyid user a
expect_error ENOTDIR
search_for_key --fail $keyid user a @p
expect_error ENOTDIR

# dispose of the key
marker "UNLINK KEY"
unlink_key $keyid @s

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
