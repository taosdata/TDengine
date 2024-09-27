#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that an empty keyring name fails correctly
marker "SESSION WITH EMPTY KEYRING NAME"
new_session --fail ""
expect_error EINVAL

# This doesn't work on MIPS earlier than 3.19 because of a kernel bug
kver=`uname -r`
kmch=`uname -m`
if kernel_at_or_later_than 3.19 ||
	[ "$kmch" != "mips" -a "$kmch" != "mips64" ]
then
	# check that an overlong keyring name fails correctly
	marker "SESSION WITH OVERLONG KEYRING NAME"
	new_session --fail a$maxdesc
	expect_error EINVAL
fi

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
