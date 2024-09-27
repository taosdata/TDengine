#!/bin/bash

# Test for https://bugzilla.redhat.com/show_bug.cgi?id=1071346

. ../../prepare.inc.sh
. ../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# create a keyring and attach it to the session keyring
marker "ADD SANDBOX KEYRING"
create_keyring --new=sandbox sandbox @s

# Add a second keyring of the same name into the sandbox
marker "ADD SECOND SANDBOX KEYRING"
create_keyring --new=second sandbox $sandbox

# Now try and link keyrings together in ways that should fail
marker "CHECK NO LINK SESSION TO SECOND"
link_key --fail @s $second
expect_error EDEADLK
marker "CHECK NO LINK SANDBOX TO SECOND"
link_key --fail $sandbox $second
expect_error EDEADLK
marker "CHECK NO LINK SECOND TO SECOND"
link_key --fail $second $second
expect_error EDEADLK

# Add another keyring into sandbox and stick a third sandbox keyring in that
marker "ADD SIDE KEYRING"
create_keyring --new=side side $sandbox
marker "ADD THIRD SANDBOX KEYRING"
create_keyring --new=third sandbox $side

# Make sure we can't link the session keyring, the sandbox, the side
# keyring or the third keyring itself into the third keyring.
marker "CHECK NO LINK SESSION TO THIRD"
link_key --fail @s $third
expect_error EDEADLK
marker "CHECK NO LINK SANDBOX TO THIRD"
link_key --fail $sandbox $third
expect_error EDEADLK
marker "CHECK NO LINK SIDE TO THIRD"
link_key --fail $side $third
expect_error EDEADLK
marker "CHECK NO LINK THIRD TO THIRD"
link_key --fail $sandbox $third
expect_error EDEADLK

# We should, however, be able to link second to third but not then
# third to second
marker "CHECK LINK SECOND TO THIRD"
link_key $second $third
marker "CHECK NO LINK THIRD TO SECOND"
link_key --fail $third $second
expect_error EDEADLK

# We can then detach the link we just made and check the reverse
# linkage.
marker "UNLINK SECOND FROM THIRD"
unlink_key $second $third
marker "CHECK LINK THIRD TO SECOND"
link_key $third $second
marker "CHECK NO LINK SECOND TO THIRD"
link_key --fail $second $third
expect_error EDEADLK

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
