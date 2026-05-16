#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh

# ---- do the actual testing ----

if [ $have_restrict_keyring = 0 ]
then
    toolbox_skip_test $TEST "SKIPPING DUE TO LACK OF KEYRING RESTRICTION"
    exit 0
fi

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# create a keyring for CA keys
marker "ADD CA KEYRING"
create_keyring --new=cakeyringid cakeyring @s

# create a keyring
marker "ADD KEYRING TO RESTRICT"
create_keyring --new=restrictid restrict @s

# invalid payload
marker "INVALID EXTRA PARAMETER 1"
restrict_keyring --fail $restrictid "asymmetric" "key_or_keyring:$cakeyringid:bad_param"

marker "INVALID EXTRA PARAMETER 2"
restrict_keyring --fail $restrictid "asymmetric" "builtin_trusted:bad_param"

marker "INVALID RESTRICT METHOD"
restrict_keyring --fail $restrictid "asymmetric" "no_such_method:$cakeyringid"

marker "INVALID KEY TYPE"
restrict_keyring --fail $restrictid "not_a_key_type" "builtin_trusted"

marker "INVALID KEY ID"
restrict_keyring --fail $restrictid "asymmetric" "key_or_keyring:abcxyz"

# invalid key option
marker "USE KEY ID 0 FOR KEYRING"
restrict_keyring --fail $restrictid "asymmetric" "key_or_keyring:0"

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
