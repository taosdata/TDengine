#!/bin/bash

. ../../prepare.inc.sh
. ../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# This doesn't work on MIPS earler than 3.19 because of a kernel bug
kver=`uname -r`
kmch=`uname -m`
if kernel_at_or_later_than 3.19 ||
	[ "$kmch" != "mips" -a "$kmch" != "mips64" ]
then
    marker "TEST TYPE AND DESC LIMITS"
    if ! keyctl --test limits
    then
	failed
    fi
fi

marker "TEST PAYLOAD LIMIT"
if ! keyctl --test limits2
then
    failed
fi

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
