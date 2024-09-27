#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that an max length key description works correctly (4096 inc NUL)
if [ $PAGE_SIZE -lt $maxsquota ]
then
    marker "CHECK MAXLEN DESC"
    create_keyring --new=keyid $maxdesc @s
    clear_keyring @s
else
    marker "CHECK MAXLEN DESC FAILS WITH EDQUOT"
    create_keyring --fail $maxdesc @s
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
	create_keyring --fail a$maxdesc @s
	expect_error EINVAL
fi

# check that an empty keyring name fails
marker "CHECK EMPTY KEYRING NAME"
create_keyring --fail "" @s
expect_error EINVAL

# check that a bad key ID fails correctly
marker "CHECK BAD KEY ID"
create_keyring --fail wibble 0
expect_error EINVAL

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
