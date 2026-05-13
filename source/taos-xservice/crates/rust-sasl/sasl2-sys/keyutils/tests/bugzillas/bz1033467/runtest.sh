#!/bin/bash

# Test for https://bugzilla.redhat.com/show_bug.cgi?id=1033467

. ../../prepare.inc.sh
. ../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# create a keyring and attach it to the session keyring
marker "ADD SANDBOX KEYRING"
create_keyring --new=sandbox sandbox @s

# create a bunch of nested keyrings in the sandbox
marker "ADD NESTED KEYRINGS"
declare -a ring
for ((i=0; i<=16; i++))
do
    create_keyring --new="ring[$i]" ring$i $sandbox
done

# create a key in each of those keyrings
marker "ADD KEYS"
keys=""
for ((i=0; i<=16; i++))
do
    create_key --new=id user a$i a ${ring[$i]}
    keys="$keys $id"
    keyid[$i]=$id
done

# search for the added keys, beginning at sandbox and exercising the nesting
marker "SEARCH KEYS"
keys2=""
for ((i=0; i<=16; i++))
do
    search_for_key --expect=${keyid[$i]} $sandbox user a$i
    keys2="$keys2 ${keyid[$i]}"
done

marker "COMPARE KEY LISTS"
if [ "$keys" != "$keys2" ]
then
	echo "Key lists differ" >>$OUTPUTFILE
	echo List 1: "\"$keys\"" >>$OUTPUTFILE
	echo List 2: "\"$keys2\"" >>$OUTPUTFILE
	failed
fi

# search for some unadded keys and make sure we get an error
marker "SEARCH MISSES"
for ((i=17; i<=20; i++))
do
    search_for_key --fail $sandbox user a$i
    expect_error ENOKEY
done

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
