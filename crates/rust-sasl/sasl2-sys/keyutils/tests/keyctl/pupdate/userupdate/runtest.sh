#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that we can add a user key to the session keyring
marker "ADD USER KEY"
create_key --new=keyid user wibble stuff @s

# read back what we put in it
marker "PRINT PAYLOAD"
print_key $keyid
expect_payload payload "stuff"

# check that we can update a user key
marker "PUPDATE USER KEY"
echo -n "lizard" | pupdate_key $keyid

# read back what we changed it to
marker "PRINT UPDATED PAYLOAD"
print_key $keyid
expect_payload payload "lizard"

# remove the key we added
marker "UNLINK KEY"
unlink_key $keyid @s

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
