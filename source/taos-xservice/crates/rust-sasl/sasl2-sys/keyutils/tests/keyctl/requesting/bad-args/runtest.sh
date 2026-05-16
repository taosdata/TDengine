#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
if [ $skip_install_required -eq 1 ]
then
    echo "++++ SKIPPING TEST" >$OUTPUTFILE
    marker "SKIP BECAUSE TEST REQUIRES FULL INSTALL (for /sbin/request-key)"
    toolbox_report_result $TEST PASS
    exit 0
else
    echo "++++ BEGINNING TEST" >$OUTPUTFILE
fi

# check that an empty key type fails correctly
marker "CHECK EMPTY KEY TYPE"
request_key --fail "" debug:wibble
expect_error EINVAL
request_key --fail "" debug:wibble @p
expect_error EINVAL
request_key_callout --fail "" debug:wibble stuff
expect_error EINVAL
request_key_callout --fail "" debug:wibble stuff @p
expect_error EINVAL
prequest_key_callout --fail stuff "" debug:wibble
expect_error EINVAL
prequest_key_callout --fail stuff "" debug:wibble @p
expect_error EINVAL

# check that an unsupported key type fails correctly
marker "CHECK UNSUPPORTED KEY TYPE"
request_key --fail "lizardsgizzards" debug:wibble
expect_error ENOKEY
request_key --fail "lizardsgizzards" debug:wibble @p
expect_error ENOKEY
request_key_callout --fail "lizardsgizzards" debug:wibble stuff
expect_error ENOKEY
request_key_callout --fail "lizardsgizzards" debug:wibble stuff @p
expect_error ENOKEY
prequest_key_callout --fail stuff "lizardsgizzards" debug:wibble
expect_error ENOKEY
prequest_key_callout --fail stuff "lizardsgizzards" debug:wibble @p
expect_error ENOKEY

# check that an invalid key type fails correctly
# - key types beginning with a dot are internal use only
marker "CHECK INVALID KEY TYPE"
request_key --fail ".user" debug:wibble
expect_error EPERM
request_key --fail ".user" debug:wibble @p
expect_error EPERM
request_key_callout --fail ".user" debug:wibble stuff
expect_error EPERM
request_key_callout --fail ".user" debug:wibble stuff @p
expect_error EPERM
prequest_key_callout --fail stuff ".user" debug:wibble
expect_error EPERM
prequest_key_callout --fail stuff ".user" debug:wibble @p
expect_error EPERM

# check that an maximum length invalid key type fails correctly
marker "CHECK MAXLEN INVALID KEY TYPE"
request_key --fail $maxtype debug:wibble
expect_error ENOKEY
request_key --fail $maxtype debug:wibble @p
expect_error ENOKEY
request_key_callout --fail $maxtype debug:wibble stuff
expect_error ENOKEY
request_key_callout --fail $maxtype debug:wibble stuff @p
expect_error ENOKEY

# check that an overlong key type fails correctly
marker "CHECK OVERLONG KEY TYPE"
request_key --fail a$maxtype debug:wibble
expect_error EINVAL
request_key --fail a$maxtype debug:wibble @p
expect_error EINVAL
request_key_callout --fail a$maxtype debug:wibble stuff
expect_error EINVAL
request_key_callout --fail a$maxtype debug:wibble stuff @p
expect_error EINVAL

# check that an max length key description works correctly
marker "CHECK MAXLEN DESC"
request_key --fail user $maxdesc
expect_error ENOKEY

# This doesn't work on MIPS earlier than 3.19 because of a kernel bug
kver=`uname -r`
kmch=`uname -m`
if kernel_at_or_later_than 3.19 ||
	[ "$kmch" != "mips" -a "$kmch" != "mips64" ]
then
	# check that an overlong key description fails correctly
	marker "CHECK OVERLONG DESC"
	request_key --fail user a$maxdesc
	expect_error EINVAL
fi

# check that a max length callout info works correctly
marker "CHECK MAXLEN CALLOUT"
request_key_callout --fail user wibble $maxdesc @p
expect_error ENOKEY

# check that an overlong callout info fails correctly
marker "CHECK OVERLONG CALLOUT"
request_key_callout --fail user wibble a$maxcall
expect_error EINVAL

# check that a max length callout info works correctly
marker "CHECK MAXLEN PIPED CALLOUT"
prequest_key_callout --fail $maxcall user wibble @p
expect_error ENOKEY

# check that an overlong callout info fails correctly
marker "CHECK OVERLONG PIPED CALLOUT"
prequest_key_callout --fail a$maxcall user wibble
expect_error EINVAL

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
