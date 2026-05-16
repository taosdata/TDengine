#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that an empty key type fails correctly
marker "CHECK EMPTY KEY TYPE"
pcreate_key --fail stuff "" wibble @p
expect_error EINVAL

# check that an unsupported key type fails correctly
marker "CHECK UNSUPPORTED KEY TYPE"
pcreate_key --fail stuff lizardsgizzards wibble @p
expect_error ENODEV

# check that an invalid key type fails correctly
marker "CHECK INVALID KEY TYPE"
pcreate_key --fail stuff .user wibble @p
expect_error EPERM

# check that an maximum length invalid key type fails correctly
marker "CHECK MAXLEN KEY TYPE"
pcreate_key --fail stuff $maxtype wibble @p
expect_error ENODEV

# check that an overlong key type fails correctly
marker "CHECK OVERLONG KEY TYPE"
pcreate_key --fail stuff a$maxtype wibble @p
expect_error EINVAL

# check that creation of a keyring with non-empty payload fails correctly
marker "CHECK ADD KEYRING WITH PAYLOAD"
pcreate_key --fail stuff keyring wibble @p
expect_error EINVAL

# check that an max length key description works correctly
if [ $PAGE_SIZE -lt $maxsquota ]
then
    marker "CHECK MAXLEN DESC"
    pcreate_key --new=keyid stuff user $maxdesc @s
    clear_keyring @s
else
    marker "CHECK MAXLEN DESC FAILS WITH EDQUOT"
    pcreate_key --fail stuff user $maxdesc @p
    expect_error EDQUOT
fi

# This doesn't work on MIPS earlier than 3.19 because of a kernel bug
kver=`uname -r`
kmch=`uname -m`
if kernel_at_or_later_than 3.19 ||
	[ "$kmch" != "mips" -a "$kmch" != "mips64" ]
then
	# check that an overlong key description fails correctly (>4095 inc NUL)
	marker "CHECK OVERLONG DESC"
	pcreate_key --fail stuff user a$maxdesc @p
	expect_error EINVAL
fi

# check that a bad key ID fails correctly
marker "CHECK BAD KEY ID"
pcreate_key --fail stuff user wibble 0
expect_error EINVAL

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
