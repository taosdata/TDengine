#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# create a keyring and attach it to the session keyring
marker "ADD KEYRING"
create_keyring --new=keyringid wibble @s

# stick a key in the keyring
marker "ADD KEY"
create_key --new=keyid user lizard gizzard $keyringid

# changing the key's ownership is not supported before 2.6.18-rc1
if kernel_older_than 2.6.18
then
    marker "CHOWN"
    chown_key --fail $keyid 1
    expect_error EOPNOTSUPP
elif [ `id -u` != 0 ]
then
    # must be running as root for this to work
    marker "CHOWN"
    chown_key --fail $keyid 1
    expect_error EACCES
else
    marker "CHOWN"
    chown_key $keyid 1

    marker "CHOWN BACK"
    chown_key $keyid 0
fi

# changing the key's group ownership is supported (change to "bin" group)
if [ `id -u` != 0 ]
then
    marker "CHGRP"
    chgrp_key --fail $keyid 1
    expect_error EACCES
else
    marker "CHGRP"
    chgrp_key $keyid 1
    describe_key $keyid
    expect_key_rdesc rdesc "user@.*@1@[0-9a-f]*@lizard"
fi

# check that each permission can be granted to the key
marker "ITERATE PERMISSIONS"
for i in \
    00210002 00210004 00210008 00210010 \
    00210200 00210400 00210800 00211000 \
    00230000 00250000 00290000 00310000 \
    02210000 04210000 08210000 10210000
  do
  set_key_perm $keyid 0x$i
  describe_key $keyid
  expect_key_rdesc rdesc "user@.*@.*@$i@lizard"
done

# check that we can't use group perms instead of user perms to view the key
# (our UID matches that of the key)
marker "VIEW GROUP PERMISSIONS"
set_key_perm $keyid 0x00201f00
describe_key --fail $keyid
expect_error EACCES

# check that we can't use other perms instead of user perms to view the key
# (our UID matches that of the key)
marker "VIEW OTHER PERMISSIONS"
set_key_perm $keyid 0x0020001f
describe_key --fail $keyid
expect_error EACCES

# check that taking away setattr permission renders the key immune to setperm
marker "REMOVE SETATTR"
set_key_perm $keyid 0x1f1f1f1f
describe_key $keyid
expect_key_rdesc rdesc "user@.*@.*@.*@lizard"

marker "REINSTATE SETATTR"
set_key_perm --fail $keyid 0x3f3f1f1f
expect_error EACCES

# remove the keyring we added
marker "UNLINK KEYRING"
unlink_key $keyringid @s

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
