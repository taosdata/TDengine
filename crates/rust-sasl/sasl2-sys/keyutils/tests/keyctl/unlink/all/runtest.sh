#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS

if keyutils_at_or_later_than 1.5
then
    echo "++++ BEGINNING TEST" >$OUTPUTFILE

    # create a keyring and attach it to the session keyring
    marker "ADD KEYRING"
    create_keyring --new=keyringid wibble @s

    # stick a key in the keyring
    marker "ADD KEY"
    create_key --new=keyid user lizard gizzard $keyringid

    # check that we can list it
    marker "LIST KEYRING WITH ONE"
    list_keyring $keyringid
    expect_keyring_rlist rlist $keyid

    # dispose of the key and make sure it gets destroyed
    marker "UNLINK KEY FROM KEYRING"
    unlink_key --wait $keyid $keyringid

    # trying a tree-wide unlink should succeed with no links removed
    marker "CHECK NO UNLINK KEY FROM TREE"
    unlink_key $keyid
    expect_unlink_count n_unlinked 0

    # check that the keyring is now empty
    marker "LIST KEYRING"
    list_keyring $keyringid
    expect_keyring_rlist rlist empty

    # create a key to be massively linked
    marker "ADD MULTI KEY"
    create_key --new=keyid user lizard gizzard $keyringid

    # stick twenty keyrings in the keyring with twenty links
    marker "ADD TWENTY KEYRINGS WITH LINKS"
    subrings=
    for ((i=0; i<20; i++))
    do
	create_keyring --new=x ring$i $keyringid
	keys="$keys $x"
	subrings="$subrings $x"
	list_keyring $keyringid
	expect_keyring_rlist rlist $x

	link_key $keyid $x
	list_keyring $x
	expect_keyring_rlist rlist $keyid
    done

    marker "SHOW"
    if ! keyctl show >>$OUTPUTFILE 2>&1
    then
        failed
    fi

    # delete all the keys from the keyring tree
    marker "REMOVE ALL LINKS TO KEY"
    unlink_key $keyid
    expect_unlink_count n_unlinked 21

    # there should not now be any left
    unlink_key $keyid
    expect_unlink_count n_unlinked 0

    # check that the key is no longer in the main keyring
    marker "CHECK GONE"
    list_keyring $keyringid
    expect_keyring_rlist rlist $keyid --absent

    for i in $subrings
    do
	list_keyring $i
	expect_keyring_rlist rlist $keyid --absent
    done

    # remove the keyring we added
    marker "UNLINK KEY"
    unlink_key $keyringid @s

    echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE
else
    toolbox_skip_test $TEST "SKIPPING TEST DUE TO LACK OF UNLINK-ALL"
    exit 0
fi

# --- then report the results in the database ---
toolbox_report_result $TEST $result
