#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that show shows us our session keyring
marker "SHOW SESSION KEYRING"
keyctl show >>$OUTPUTFILE 2>&1
if [ $? != 0 ]
then
    failed
fi

# must be at least two lines in the output (plus the test banner lines)
nlines=`wc -l $OUTPUTFILE | cut -d\  -f1`
if [ "$nlines" -lt 4 ]
then
    failed
fi

# there must be a session keyring section on the third line
if [ "`sed -n -e 3p $OUTPUTFILE`" != "Session Keyring" ]
then
    failed
fi

# the first key listed (line 2) should be a keying (the session keyring) ...
keyring1="`grep -n keyring $OUTPUTFILE | cut -d: -f1 | head -1`"
if [ "$keyring1" != "4" ]
then
    failed
fi

# ... and it should be the session keyring
keyring1name="`sed -n -e 4p $OUTPUTFILE | awk '{print $6}'`"
if ! expr "$keyring1name" : "^RHTS/keyctl" >&/dev/null
then
    failed
fi


echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
