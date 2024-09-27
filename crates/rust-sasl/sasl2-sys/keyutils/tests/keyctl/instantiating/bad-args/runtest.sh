#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# check that a bad key ID fails correctly
marker "CHECK BAD KEY ID"
instantiate_key --fail 0 a @p
expect_error EPERM
pinstantiate_key --fail a 0 @p
expect_error EPERM
negate_key --fail 0 10 @p
expect_error EPERM
reject_key --fail 0 10 rejected @p
expect_error EPERM

# create a non-keyring
marker "CREATE KEY"
create_key --new=keyid user lizard gizzard @s

# check that instantiation of an instantiated key fails
marker "CHECK ALREADY INSTANTIATED KEY"
instantiate_key --fail $keyid a @p
expect_error EPERM
pinstantiate_key --fail a $keyid @p
expect_error EPERM
reject_key --fail $keyid 10 rejected @p
expect_error EPERM
negate_key --fail $keyid 10 @p
expect_error EPERM

# check reject key timeout must be a number
marker "CHECK REJECT TIMEOUT"
expect_args_error keyctl reject $keyid aa rejected @p

# check reject key timeout must be a number
marker "CHECK REJECT ERRORS"
expect_args_error keyctl reject $keyid 10 notanerror @p

# check negative key timeout must be a number
marker "CHECK NEGATE TIMEOUT"
expect_args_error keyctl negate $keyid aa @p

# dispose of the key we were using
marker "UNLINK KEY"
unlink_key --wait $keyid @s

# check that a non-existent key ID fails correctly
marker "CHECK NON-EXISTENT KEY ID"
instantiate_key --fail $keyid a @p
expect_error EPERM
pinstantiate_key --fail a $keyid @p
expect_error EPERM
reject_key --fail $keyid 10 rejected @p
expect_error EPERM
negate_key --fail $keyid 10 @p
expect_error EPERM

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
