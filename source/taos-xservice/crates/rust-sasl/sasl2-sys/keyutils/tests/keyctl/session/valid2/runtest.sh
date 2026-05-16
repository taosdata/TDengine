#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# Replace the script's session keyring with an anonymous keyring
marker "ANON SESSION TO PARENT"
id_key --to=ses1 @s
new_session_to_parent
id_key --to=ses2 @s

if [ $ses2 = $ses1 ]
then
    failed
fi

describe_key @s "@"
expect_key_rdesc rdesc "keyring@.*@.*@.*@_ses"

# Replace the script's session keyring with a named keyring
marker "NAMED SESSION TO PARENT"
new_session_to_parent lizard
id_key --to=ses3 @s

if [ $ses3 = $ses2 -o $ses3 = $ses1 ]
then
    failed
fi

describe_key @s "@"
expect_key_rdesc rdesc "keyring@.*@.*@.*@lizard"


echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
