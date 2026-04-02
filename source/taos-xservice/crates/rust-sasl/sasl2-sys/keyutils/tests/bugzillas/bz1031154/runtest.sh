#!/bin/bash

# Test for https://bugzilla.redhat.com/show_bug.cgi?id=1031154

. ../../prepare.inc.sh
. ../../toolbox.inc.sh

# We intentionally generate AVCs so the test system shouldn't fail us
# because the AVCs were generated.
export AVC_ERROR=+no_avc_check
export RHTS_OPTION_STRONGER_AVC=

# ---- do the actual testing ----

result=PASS

if [ $have_big_key_type = 0 ]
then
    toolbox_skip_test $TEST "SKIPPING TEST DUE TO LACK OF BIG_KEY TYPE"
    exit 0
fi

require_selinux
require_command getenforce
require_command setenforce
require_command runcon
require_command ausearch

echo "++++ BEGINNING TEST" >$OUTPUTFILE

# we need a reference time to scan the audit log from so as not to pick up old
# results from this test.
base_date=`date +"%x@%X"`
base_time=${base_date#*@}
base_date=${base_date%@*}
sleep 1

# reset the permissive audit log autocancel thing
load_policy

# we need to be in permissive mode
marker "ENTER SELINUX PERMISSIVE MODE"

mode=`getenforce`

if [ "$mode" != "Permissive" ]
then
	echo setenforce Permissive >>$OUTPUTFILE
	if ! setenforce Permissive
	then
		failed
	fi
fi

# create a big key to probe
marker "CREATE BIG KEY"
pcreate_key_by_size --new=id 8192 big_key test-key @s

# check the big key is file backed and the right size
marker "CHECK BIG KEY"
xid=`printf %08x $id`

pk=`cat /proc/keys | grep "^$xid.*test-key: 8192 \\[file\\]"`
echo $pk >>$OUTPUTFILE
if [ -z "$pk" ]
then
	echo "+++ Incorrectly created key" >>$OUTPUTFILE
	cat /proc/keys | grep "^$xid" >>$OUTPUTFILE
	failed
fi

# use a separate context to access the key
marker "ACCESS INTERCONTEXT"

echo runcon system_u:system_r:httpd_t:s0-s0:c0.c1023 keyctl print $id >>$OUTPUTFILE
if ! runcon system_u:system_r:httpd_t:s0-s0:c0.c1023 keyctl print $id >/dev/null 2>>$OUTPUTFILE
then
	failed
fi

# examine the audit logs
marker "EXAMINE AUDIT LOGS"

echo ausearch -m AVC -i --subject httpd_t -ts $base_date $base_time \| audit2allow \| grep '-P "allow httpd_t user_tmpfs_t:file [{] (open |read )+[}];"' >>$OUTPUTFILE
if ausearch -m AVC -i --subject httpd_t -ts $base_date $base_time 2>>$OUTPUTFILE | audit2allow 2>>$OUTPUTFILE | grep -P "allow httpd_t user_tmpfs_t:file [{] (open |read )+[}];"
then
	failed
fi

marker "RESTORE SELINUX MODE"
if [ "$mode" != "Permissive" ]
then
	echo setenforce $mode >>$OUTPUTFILE
	if ! setenforce $mode
	then
		failed
	fi
fi

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
