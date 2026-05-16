#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that we can add a user key to the session keyring
marker "ADD USER KEY"
pcreate_key --new=keyid stuff user wibble @s

# read back what we put in it
marker "PRINT PAYLOAD"
print_key $keyid
expect_payload payload "stuff"

# check that we can add a hex-encoded user key to the session keyring
marker "ADD HEX USER KEY"
pcreate_key --update=$keyid "73  7475 66  66  " -x user wibble @s

# read back what we put in it
marker "PRINT PAYLOAD"
print_key $keyid
expect_payload payload "stuff"

# check that we can update a user key
marker "UPDATE USER KEY"
pcreate_key --update=$keyid lizard user wibble @s

# read back what we changed it to
marker "PRINT UPDATED PAYLOAD"
print_key $keyid
expect_payload payload "lizard"

# remove the key we added
marker "UNLINK KEY"
unlink_key $keyid @s

if [ $skip_root_required = 0 ] && {
        [ $OSDIST = RHEL ] && ! version_less_than $OSRELEASE 6.6 ||
        keyutils_at_or_later_than 1.5.6 ;
        }
then
    # add keys with huge payloads
    old_root_quota=`cat /proc/sys/kernel/keys/root_maxbytes`
    if [ $old_root_quota -lt 65536 ]
    then
	marker "INCREASE QUOTA"
	echo 65536 >/proc/sys/kernel/keys/root_maxbytes
    fi

    marker "ADD LARGE USER KEY"
    pcreate_key_by_size --new=keyid 32767 user large @s
    md5sum_key $keyid
    expect_payload payload "f128f774ede3fe931e7c6745c4292f40"

    if [ $have_big_key_type = 1 ]
    then
	marker "ADD SMALL BIG KEY"
	pcreate_key_by_size --new=keyid 128 big_key small @s
	md5sum_key $keyid
	expect_payload payload "f09f35a5637839458e462e6350ecbce4"

	marker "ADD HUGE BIG KEY"
	pcreate_key_by_size --new=keyid $((1024*1024-1)) big_key huge @s
	md5sum_key $keyid
	expect_payload payload "e57598cd670284cf7d09e16ed9d4b2ac"
    fi

    marker "CLEAR KEYRING"
    clear_keyring @s

    if [ $old_root_quota -lt 65536 ]
    then
	marker "RESET QUOTA"
	echo $old_root_quota >/proc/sys/kernel/keys/root_maxbytes
	sleep 1
    fi
fi

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
