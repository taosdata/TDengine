###############################################################################
#
# Copyright (C) 2005 Red Hat, Inc. All Rights Reserved.
# Written by David Howells (dhowells@redhat.com)
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version
# 2 of the License, or (at your option) any later version.
#
###############################################################################

echo === $OUTPUTFILE ===

endian=`file -L /proc/$$/exe`
if expr "$endian" : '.* MSB \+\(pie executable\|executable\|shared object\).*' >&/dev/null
then
    endian=BE
elif expr "$endian" : '.* LSB \+\(pie executable\|executable\|shared object\).*' >&/dev/null
then
    endian=LE
else
    echo -e "+++ \e[31;1mCan't Determine Endianness\e[0m"
    echo "+++ Can't Determine Endianness" >>$OUTPUTFILE
    exit 2
fi

maxtypelen=31
maxtype=`for ((i=0; i<$((maxtypelen)); i++)); do echo -n a; done`

PAGE_SIZE=`getconf PAGESIZE`
pagelen=$((PAGE_SIZE - 1))
fullpage=`for ((i=0; i<$((pagelen)); i++)); do echo -n a; done`
string4095=`for ((i=0; i<4095; i++)); do echo -n a; done`

if kernel_at_or_later_than 3.18
then
    maxdesc=$string4095
elif rhel6_kernel_at_or_later_than 2.6.32-589.el6
then
    maxdesc=$string4095
else
    maxdesc=$fullpage
fi

maxcall=$fullpage

maxsquota=`grep '^ *0': /proc/key-users | sed s@.*/@@`

key_gc_delay_file="/proc/sys/kernel/keys/gc_delay"
if [ -f $key_gc_delay_file ]; then
    orig_gc_delay=`cat $key_gc_delay_file`
else
    orig_gc_delay=300
fi


function marker ()
{
    echo -e "+++ \e[33m$*\e[0m"
    echo +++ $* >>$OUTPUTFILE
    if [ "$watch_log" != "" ]
    then
	echo +++ $* >>$watch_log
    fi
}

function failed()
{
    echo -e "\e[31;1mFAILED\e[0m"
    echo === FAILED === >>$OUTPUTFILE
    keyctl show >>$OUTPUTFILE
    echo ============== >>$OUTPUTFILE
    result=FAIL
}

function expect_args_error ()
{
    "$@" >>$OUTPUTFILE 2>&1
    if [ $? != 2 ]
    then
	failed
    fi

}

function toolbox_report_result()
{
    if [ $RUNNING_UNDER_RHTS = 1 ]
    then
	report_result $1 $2
    fi
    if [ $2 = FAIL ]
    then
	exit 1
    fi
}

function toolbox_skip_test()
{
    echo "++++ SKIPPING TEST" >>$OUTPUTFILE
    marker "$2"
    toolbox_report_result $1 PASS
}

###############################################################################
#
# Return true if the command is found in $PATH. If not, log that the test is
# being skipped, report the result as PASS, and exit.
#
###############################################################################
function require_command ()
{
    which "$1" >&/dev/null
    if [ $? != 0 ]
    then
	toolbox_skip_test "SKIP DUE TO MISSING COMMAND: $1"
        exit 0
    fi
}

function require_selinux ()
{
    if ! grep -q selinuxfs /proc/mounts;
    then
	toolbox_skip_test $TEST "SKIP DUE TO DISABLED SELINUX"
	exit 0
    fi
}

###############################################################################
#
# extract an error message from the log file and check it
#
###############################################################################
function expect_error ()
{
    my_varname=$1

    my_errmsg="`tail -1 $OUTPUTFILE`"
    eval $my_varname="\"$my_errmsg\""

    if [ $# != 1 ]
    then
	echo "Format: expect_error <symbol>" >>$OUTPUTFILE
	failed
    fi

    case $1 in
	EPERM)		my_err="Operation not permitted";;
	EAGAIN)		my_err="Resource temporarily unavailable";;
	ENOENT)		my_err="No such file or directory";;
	EEXIST)		my_err="File exists";;
	ENOTDIR)	my_err="Not a directory";;
	EACCES)		my_err="Permission denied";;
	EINVAL)		my_err="Invalid argument";;
	ENODEV)		my_err="No such device";;
	ELOOP)		my_err="Too many levels of symbolic links";;
	EOPNOTSUPP)	my_err="Operation not supported";;
	EDEADLK)	my_err="Resource deadlock avoided";;
	EDQUOT)		my_err="Disk quota exceeded";;
	ENOKEY)
	    my_err="Required key not available"
	    old_err="Requested key not available"
	    alt_err="Unknown error 126"
	    ;;
	EKEYEXPIRED)
	    my_err="Key has expired"
	    alt_err="Unknown error 127"
	    ;;
	EKEYREVOKED)
	    my_err="Key has been revoked"
	    alt_err="Unknown error 128"
	    ;;
	EKEYREJECTED)
	    my_err="Key has been rejected"
	    alt_err="Unknown error 129"
	    ;;
	*)
	    echo "Unknown error message $1" >>$OUTPUTFILE
	    failed
	    ;;
    esac

    if expr "$my_errmsg" : ".*: $my_err" >&/dev/null
    then
	:
    elif [ "x$alt_err" != "x" ] && expr "$my_errmsg" : ".*: $alt_err" >&/dev/null
    then
	:
    elif [ "x$old_err" != "x" ] && expr "$my_errmsg" : ".*: $old_err" >&/dev/null
    then
	:
    else
	failed
    fi
}

###############################################################################
#
# Watch a key for notifications.
#
###############################################################################
function watch_add_key ()
{
    my_keyid=$1

    if [ $watch_fd = 0 ]; then return; fi

    keyctl watch_add $watch_fd $my_keyid || failed
}

###############################################################################
#
# Check for a notification on the last or last-but-one lines of the
# notification log.
#
###############################################################################
function check_notify ()
{
    if [ $watch_fd = 0 ]; then return; fi

    keyctl watch_sync $watch_fd || failed

    if [ "$1" = "-2" ]
    then
	shift
	my_logline="`tail -2 $watch_log | head -1`"
    else
	my_logline="`tail -1 $watch_log`"
    fi

    my_subtype=$1
    case $my_subtype in
	revoked)
	    my_key1=$2
	    ;;
	invalidated)
	    my_key1=$2
	    ;;
	*)
	    case $2 in
		@*)
		    my_key1=`keyctl id $2`
		    ;;
		*)
		    my_key1=$2
	    esac
	;;
    esac
    my_key2=$3

    case $my_subtype in
	instantiated)
	    exp="$my_key1 inst"
	    ;;
	updated)
	    exp="$my_key1 upd"
	    ;;
	linked)
	    exp="$my_key1 link $my_key2"
	    ;;
	unlinked)
	    exp="$my_key1 unlk $my_key2"
	    ;;
	cleared)
	    exp="$my_key1 clr"
	    ;;
	revoked)
	    exp="$my_key1 rev"
	    ;;
	invalidated)
	    exp="$my_key1 inv"
	    ;;
	setattr)
	    exp="$my_key1 attr"
	    ;;
	*)
	    echo "INCORRECT check_notify SUBTYPE" >&2
	    failed
	    ;;
    esac

    if [ "$exp" != "$my_logline" ]
    then
	echo "\"$exp\"" != "\"$my_logline\""
	echo "check_notify: \"$exp\"" != "\"$my_logline\"" >>$OUTPUTFILE
	echo "^^^ failed ^^^" >>$watch_log
	failed
    fi
}

###############################################################################
#
# wait for a key to be destroyed (get removed from /proc/keys)
#
###############################################################################
function pause_till_key_destroyed ()
{
    echo "+++ WAITING FOR KEY TO BE DESTROYED" >>$OUTPUTFILE
    hexkeyid=`printf %08x $1`

    while grep $hexkeyid /proc/keys
    do
	sleep 1
    done
}

###############################################################################
#
# wait for a key to be unlinked
#
###############################################################################
function pause_till_key_unlinked ()
{
    echo "+++ WAITING FOR KEY TO BE UNLINKED" >>$OUTPUTFILE

    while true
    do
	echo keyctl unlink $1 $2 >>$OUTPUTFILE
	keyctl unlink $1 $2 >>$OUTPUTFILE 2>&1
	if [ $? != 1 ]
	then
	    failed
	fi

	my_errmsg="`tail -1 $OUTPUTFILE`"
	if ! expr "$my_errmsg" : ".*: No such file or directory" >&/dev/null
	then
	    break
	fi
	sleep 1
    done
}

###############################################################################
#
# Get the ID of a key or keyring.
#
###############################################################################
function id_key ()
{
    my_exitval=0
    case "x$1" in
	x--to=*)
	    my_exitval=0
	    my_varname=${1#--to=}
	    my_keyid=v
	    ;;
	x--fail)
	    my_exitval=1
	    my_keyid=x
	    ;;
	x--fail2)
	    my_exitval=2
	    my_keyid=x
	    ;;
	*)
	    echo "BAD id_key ARGUMENT" >&2
	    failed
	    return
	    ;;
    esac
    shift

    echo keyctl id "$@" >>$OUTPUTFILE
    keyctl id "$@" >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e == $my_exitval ]
    then
	if [ $e = 0 ]
	then
	    got_keyid="`tail -1 $OUTPUTFILE`"
	    if expr "$got_keyid" : '[1-9][0-9]*' >&/dev/null
	    then
		if [ $my_keyid == v ]
		then
		    eval $my_varname=$got_keyid
		fi
	    else
		echo "CAN'T EXTRACT KEY ID FROM create_key OUTPUT" >&2
		eval $my_varname=no
		result=FAIL
	    fi
	fi
    else
	failed
    fi
}

###############################################################################
#
# request a key and attach it to the new keyring
#
###############################################################################
function request_key ()
{
    my_exitval=0
    case "x$1" in
	x--new=*)
	    my_exitval=0
	    my_varname=${1#--new=}
	    my_keyid=v
	    ;;
	x--old=*)
	    my_exitval=0
	    my_keyid=${1#--old=}
	    ;;
	x--fail)
	    my_exitval=1
	    my_keyid=x
	    ;;
	*)
	    echo "BAD request_key ARGUMENT" >&2
	    failed
	    return
	    ;;
    esac
    shift

    my_keyring=$3

    echo keyctl request "$@" >>$OUTPUTFILE
    keyctl request "$@" >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e == $my_exitval ]
    then
	if [ $e = 0 ]
	then
	    got_keyid="`tail -1 $OUTPUTFILE`"
	    if expr "$got_keyid" : '[1-9][0-9]*' >&/dev/null
	    then
		if [ $my_keyid == v ]
		then
		    eval $my_varname=$got_keyid
		    watch_add_key $got_keyid
		fi

		if [ $# = 3 ]
		then
		    check_notify linked $my_keyring $got_keyid
		fi
	    else
		echo "CAN'T EXTRACT KEY ID FROM create_key OUTPUT" >&2
		eval $my_varname=no
		result=FAIL
	    fi
	fi
    else
	failed
    fi
}

###############################################################################
#
# request a key and attach it to the new keyring, calling out if necessary
#
###############################################################################
function request_key_callout ()
{
    my_exitval=0
    case "x$1" in
	x--new=*)
	    my_exitval=0
	    my_varname=${1#--new=}
	    my_keyid=v
	    ;;
	x--old=*)
	    my_exitval=0
	    my_keyid=${1#--old=}
	    ;;
	x--fail)
	    my_exitval=1
	    my_keyid=x
	    ;;
	*)
	    echo "BAD request_key_callout ARGUMENT" >&2
	    failed
	    return
	    ;;
    esac
    shift

    my_keyring=$4

    echo keyctl request2 "$@" >>$OUTPUTFILE
    keyctl request2 "$@" >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e == $my_exitval ]
    then
	if [ $e = 0 ]
	then
	    got_keyid="`tail -1 $OUTPUTFILE`"
	    if expr "$got_keyid" : '[1-9][0-9]*' >&/dev/null
	    then
		if [ $my_keyid == v ]
		then
		    eval $my_varname=$got_keyid
		    watch_add_key $got_keyid
		    if [ $# = 4 ]
		    then
			check_notify -2 linked $my_keyring $got_keyid
		    fi
		else
		    if [ $# = 4 ]
		    then
			check_notify linked $my_keyring $got_keyid
		    fi
		fi
	    else
		echo "CAN'T EXTRACT KEY ID FROM create_key OUTPUT" >&2
		eval $my_varname=no
		result=FAIL
	    fi
	fi
    else
	failed
    fi
}

###############################################################################
#
# request a key and attach it to the new keyring, calling out if necessary and
# passing the callout data in on stdin
#
###############################################################################
function prequest_key_callout ()
{
    my_exitval=0
    case "x$1" in
	x--new=*)
	    my_exitval=0
	    my_varname=${1#--new=}
	    my_keyid=v
	    ;;
	x--old=*)
	    my_exitval=0
	    my_keyid=${1#--old=}
	    ;;
	x--fail)
	    my_exitval=1
	    my_keyid=x
	    ;;
	*)
	    echo "BAD prequest_key_callout ARGUMENT" >&2
	    failed
	    return
	    ;;
    esac
    shift

    data="$1"
    shift

    my_keyring=$3

    echo echo -n $data \| keyctl prequest2 "$@" >>$OUTPUTFILE
    echo -n $data | keyctl prequest2 "$@" >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e == $my_exitval ]
    then
	if [ $e = 0 ]
	then
	    got_keyid="`tail -1 $OUTPUTFILE`"
	    if expr "$got_keyid" : '[1-9][0-9]*' >&/dev/null
	    then
		if [ $my_keyid == v ]
		then
		    eval $my_varname=$got_keyid
		    watch_add_key $got_keyid
		    if [ $# = 4 ]
		    then
			check_notify -2 linked $my_keyring $got_keyid
		    fi
		else
		    if [ $# = 3 ]
		    then
			check_notify linked $my_keyring $got_keyid
		    fi
		fi
	    else
		echo "CAN'T EXTRACT KEY ID FROM create_key OUTPUT" >&2
		eval $my_varname=no
		result=FAIL
	    fi
	fi
    else
	failed
    fi
}

###############################################################################
#
# create a key and attach it to the new keyring
#
###############################################################################
function create_key ()
{
    my_exitval=0
    case "x$1" in
	x--new=*)
	    my_exitval=0
	    my_varname=${1#--new=}
	    my_keyid=v
	    ;;
	x--update=*)
	    my_exitval=0
	    my_keyid=${1#--update=}
	    ;;
	x--fail)
	    my_exitval=1
	    my_keyid=x
	    ;;
	*)
	    echo "BAD create_key ARGUMENT" >&2
	    failed
	    return
	    ;;
    esac
    shift

    if [ "$1" = "-x" ]
    then
	my_keyring=$5
    else
	my_keyring=$4
    fi

    echo keyctl add "$@" >>$OUTPUTFILE
    keyctl add "$@" >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e == $my_exitval ]
    then
	if [ $e = 0 ]
	then
	    got_keyid="`tail -1 $OUTPUTFILE`"
	    if expr "$got_keyid" : '[1-9][0-9]*' >&/dev/null
	    then
		if [ $my_keyid == v ]
		then
		    eval $my_varname=$got_keyid
		    watch_add_key $got_keyid
		    check_notify linked $my_keyring $got_keyid
		else
		    check_notify updated $got_keyid
		fi

	    else
		echo "CAN'T EXTRACT KEY ID FROM create_key OUTPUT" >&2
		eval $my_varname=no
		result=FAIL
	    fi
	fi
    else
	failed
    fi
}

###############################################################################
#
# create a key and attach it to the new keyring, piping in the data
#
###############################################################################
function pcreate_key ()
{
    my_exitval=0
    case "x$1" in
	x--new=*)
	    my_exitval=0
	    my_varname=${1#--new=}
	    my_keyid=v
	    ;;
	x--update=*)
	    my_exitval=0
	    my_keyid=${1#--update=}
	    ;;
	x--fail)
	    my_exitval=1
	    my_keyid=x
	    ;;
	*)
	    echo "BAD pcreate_key ARGUMENT" >&2
	    failed
	    return
	    ;;
    esac
    shift
    data="$1"
    shift

    if [ "$1" = "-x" ]
    then
	my_keyring=$4
    else
	my_keyring=$3
    fi

    echo echo -n $data \| keyctl padd "$@" >>$OUTPUTFILE
    echo -n $data | keyctl padd "$@" >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e == $my_exitval ]
    then
	if [ $e = 0 ]
	then
	    got_keyid="`tail -1 $OUTPUTFILE`"
	    if expr "$got_keyid" : '[1-9][0-9]*' >&/dev/null
	    then
		if [ $my_keyid == v ]
		then
		    eval $my_varname=$got_keyid
		    watch_add_key $got_keyid
		    check_notify linked $my_keyring $got_keyid
		else
		    check_notify updated $got_keyid
		fi

	    else
		echo "CAN'T EXTRACT KEY ID FROM pcreate_key OUTPUT" >&2
		eval $my_varname=no
		result=FAIL
	    fi
	fi
    else
	failed
    fi
}

###############################################################################
#
# create a key and attach it to the new keyring, piping in the data
#
###############################################################################
function pcreate_key_by_size ()
{
    my_exitval=0
    case "x$1" in
	x--new=*)
	    my_exitval=0
	    my_varname=${1#--new=}
	    my_keyid=v
	    ;;
	x--update=*)
	    my_exitval=0
	    my_keyid=${1#--update=}
	    ;;
	x--fail)
	    my_exitval=1
	    my_keyid=x
	    ;;
	*)
	    echo "BAD pcreate_key_by_size ARGUMENT" >&2
	    failed
	    return
	    ;;
    esac
    shift
    data="$1"
    shift
    my_keyring=$3

    echo dd if=/dev/zero count=1 bs=$data \| keyctl padd "$@" >>$OUTPUTFILE
    dd if=/dev/zero count=1 bs=$data 2>/dev/null | keyctl padd "$@" >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e == $my_exitval ]
    then
	if [ $e = 0 ]
	then
	    got_keyid="`tail -1 $OUTPUTFILE`"
	    if expr "$got_keyid" : '[1-9][0-9]*' >&/dev/null
	    then
		if [ $my_keyid == v ]
		then
		    eval $my_varname=$got_keyid
		    watch_add_key $got_keyid
		    check_notify linked $my_keyring $got_keyid
		else
		    check_notify updated $got_keyid
		fi

	    else
		echo "CAN'T EXTRACT KEY ID FROM pcreate_key_by_size OUTPUT" >&2
		eval $my_varname=no
		result=FAIL
	    fi
	fi
    else
	failed
    fi
}

###############################################################################
#
# create a key and attach it to the new keyring
#
###############################################################################
function create_keyring ()
{
    my_exitval=0
    case "x$1" in
	x--new=*)
	    my_exitval=0
	    my_varname=${1#--new=}
	    my_keyid=v
	    ;;
	x--fail)
	    my_exitval=1
	    my_keyid=x
	    ;;
	*)
	    echo "BAD create_keyring ARGUMENT" >&2
	    failed
	    return
	    ;;
    esac
    shift
    my_keyring=$2

    echo keyctl newring "$@" >>$OUTPUTFILE
    keyctl newring "$@" >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e == $my_exitval ]
    then
	if [ $e = 0 ]
	then
	    got_keyid="`tail -1 $OUTPUTFILE`"
	    if expr "$got_keyid" : '[1-9][0-9]*' >&/dev/null
	    then
		eval $my_varname=$got_keyid
		watch_add_key $got_keyid
		check_notify linked $my_keyring $got_keyid
	    else
		echo "CAN'T EXTRACT KEY ID FROM create_keyring OUTPUT" >&2
		eval $my_varname=no
		result=FAIL
	    fi
	fi
    else
	failed
    fi
}

###############################################################################
#
# prettily list a keyring
#
###############################################################################
function pretty_list_keyring ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl list $1 >>$OUTPUTFILE
    keyctl list $1 >>$OUTPUTFILE 2>&1
    if [ $? != $my_exitval ]
    then
	failed
    fi
}

###############################################################################
#
# list a keyring
#
###############################################################################
function list_keyring ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl rlist $1 >>$OUTPUTFILE
    keyctl rlist $1 >>$OUTPUTFILE 2>&1
    if [ $? != $my_exitval ]
    then
	failed
    fi
}

###############################################################################
#
# extract a keyring listing from the log file and see if a key ID is contained
# therein
#
###############################################################################
function expect_keyring_rlist ()
{
    my_varname=$1

    my_rlist="`tail -1 $OUTPUTFILE`"
    eval $my_varname="\"$my_rlist\""

    if [ $# = 2 -o $# = 3 ]
    then
	if [ "$2" = "empty" ]
	then
	    if [ "x$my_rlist" != "x" ]
	    then
		failed
	    fi
	else
	    my_keyid=$2
	    my_found=0
	    my_expected=1
	    if [ $# = 3 -a "x$3" = "x--absent" ]; then my_expected=0; fi

	    for k in $my_rlist
	    do
		if [ $k = $my_keyid ]
		then
		    my_found=1
		    break;
		fi
	    done

	    if [ $my_found != $my_expected ]
	    then
		failed
	    fi
	fi
    fi
}

###############################################################################
#
# prettily describe a key
#
###############################################################################
function pretty_describe_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl describe $1 >>$OUTPUTFILE
    keyctl describe $1 >>$OUTPUTFILE 2>&1
    if [ $? != $my_exitval ]
    then
	failed
    fi
}

###############################################################################
#
# describe a key
#
###############################################################################
function describe_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl rdescribe $1 "@" >>$OUTPUTFILE
    keyctl rdescribe $1 "@" >>$OUTPUTFILE 2>&1
    if [ $? != $my_exitval ]
    then
	failed
    fi
}

###############################################################################
#
# extract a raw key description from the log file and check it
#
###############################################################################
function expect_key_rdesc ()
{
    my_varname=$1

    my_rdesc="`tail -1 $OUTPUTFILE`"
    eval $my_varname="\"$my_rdesc\""

    if ! expr "$my_rdesc" : "$2" >&/dev/null
    then
	failed
    fi
}

###############################################################################
#
# read a key's payload as a hex dump
#
###############################################################################
function read_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl read $1 >>$OUTPUTFILE
    keyctl read $1 >>$OUTPUTFILE 2>&1
    if [ $? != $my_exitval ]
    then
	failed
    fi
}

###############################################################################
#
# read a key's payload as a printable string
#
###############################################################################
function print_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl print $1 >>$OUTPUTFILE
    keyctl print $1 >>$OUTPUTFILE 2>&1
    if [ $? != $my_exitval ]
    then
	failed
    fi
}

###############################################################################
#
# pipe a key's raw payload to stdout
#
###############################################################################
function pipe_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl pipe $1 >>$OUTPUTFILE
    keyctl pipe $1 >>$OUTPUTFILE 2>&1
    if [ $? != $my_exitval ]
    then
	failed
    fi
}

###############################################################################
#
# pipe a key's raw payload through md5sum
#
###############################################################################
function md5sum_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl pipe $1 \| md5sum \| cut -c1-32 >>$OUTPUTFILE
    keyctl pipe $1 | md5sum | cut -c1-32 >>$OUTPUTFILE 2>&1
    if [ $? != $my_exitval ]
    then
	failed
    fi
}

###############################################################################
#
# extract a printed payload from the log file
#
###############################################################################
function expect_payload ()
{
    my_varname=$1

    my_payload="`tail -1 $OUTPUTFILE`"
    eval $my_varname="\"$my_payload\""

    if [ $# == 2 -a "x$my_payload" != "x$2" ]
    then
	failed
    fi
}

###############################################################################
#
# extract multiline output from the log file
#
###############################################################################
function expect_multiline ()
{
    my_varname=$1
    my_linecount="`echo \"$2\" | wc -l`"

    my_payload=$(tail -$my_linecount $OUTPUTFILE)
    eval $my_varname="\"$my_payload\""

    if [ $# != 2 -o "x$my_payload" != "x$2" ]
    then
	failed
    fi
}

###############################################################################
#
# revoke a key
#
###############################################################################
function revoke_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl revoke $1 >>$OUTPUTFILE
    keyctl revoke $1 >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e == $my_exitval ]
    then
	if [ $e = 0 ]
	then
	    check_notify revoked $1
	fi
    else
	failed
    fi
}

###############################################################################
#
# unlink a key from a keyring
#
###############################################################################
function unlink_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    my_wait=0
    if [ "x$1" = "x--wait" ]
    then
	my_wait=1
	shift
    fi

    echo keyctl unlink $1 $2 >>$OUTPUTFILE
    keyctl unlink $1 $2 >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e = $my_exitval ]
    then
	if [ $e == 0 -a $# = 2 ]
	then
	    check_notify unlinked $2 $1
	fi
    else
	failed
    fi

    # keys are destroyed lazily
    if [ $my_wait = 1 ]
    then
	pause_till_key_unlinked $1 $2
    fi
}

###############################################################################
#
# extract a message about the number of keys unlinked
#
###############################################################################
function expect_unlink_count ()
{
    my_varname=$1

    my_nunlinks="`tail -1 $OUTPUTFILE`"

    if ! expr "$my_nunlinks" : '^[0-9][0-9]* links removed$'
    then
	failed
    fi

    my_nunlinks=`echo $my_nunlinks | awk '{printf $1}'`
    eval $my_varname="\"$my_nunlinks\""

    if [ $# == 2 -a $my_nunlinks != $2 ]
    then
	failed
    fi
}

###############################################################################
#
# update a key from a keyring
#
###############################################################################
function update_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl update "$@" >>$OUTPUTFILE
    keyctl update "$@" >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e == $my_exitval ]
    then
	if [ $e = 0 ]
	then
	    if [ "$1" = "-x" ]
	    then
		shift
	    fi
	    check_notify updated $1
	fi
    else
	failed
    fi
}

###############################################################################
#
# update a key from a keyring, piping the data in over stdin
#
###############################################################################
function pupdate_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl pupdate "$@" >>$OUTPUTFILE
    keyctl pupdate "$@" >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e == $my_exitval ]
    then
	if [ $e = 0 ]
	then
	    check_notify updated $1
	fi
    else
	failed
    fi
}

###############################################################################
#
# clear a keyring
#
###############################################################################
function clear_keyring ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl clear $1 >>$OUTPUTFILE
    keyctl clear $1 >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e == $my_exitval ]
    then
	if [ $e = 0 ]
	then
	    check_notify cleared $1
	fi
    else
	failed
    fi
}

###############################################################################
#
# restrict a keyring
#
###############################################################################
function restrict_keyring ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl restrict_keyring $1 $2 $3 >>$OUTPUTFILE
    keyctl restrict_keyring $1 $2 $3 >>$OUTPUTFILE 2>&1
    if [ $? != $my_exitval ]
    then
	failed
    fi
}

###############################################################################
#
# link a key to a keyring
#
###############################################################################
function link_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl link $1 $2 >>$OUTPUTFILE
    keyctl link $1 $2 >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e == $my_exitval ]
    then
	if [ $e == 0 ]
	then
	    check_notify linked $2 $1
	fi
    else
	failed
    fi
}

###############################################################################
#
# search for a key in a keyring
#
###############################################################################
function search_for_key ()
{
    my_exitval=0
    case "x$1" in
	x--expect=*)
	    my_exitval=0
	    my_keyid=${1#--expect=}
	    ;;
	x--fail)
	    my_exitval=1
	    my_keyid=x
	    ;;
	*)
	    echo "BAD search_for_key ARGUMENT" >&2
	    failed
	    return
	    ;;
    esac
    shift

    echo keyctl search "$@" >>$OUTPUTFILE
    keyctl search "$@" >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e == $my_exitval ]
    then
	if [ $e = 0 ]
	then
	    got_keyid="`tail -1 $OUTPUTFILE`"
	    if expr "$got_keyid" : '[1-9][0-9]*' >&/dev/null
	    then
		if [ $got_keyid = $my_keyid ]
		then
		    if [ $e == 0 -a $# == 4 ]
		    then
			check_notify linked $4 $got_keyid
		    fi
		else
		    echo "KEY MISMATCH $got_keyid != $my_keyid" >&2
		    failed
		fi
	    else
		echo "CAN'T EXTRACT KEY ID FROM search_for_key OUTPUT" >&2
		eval $my_varname=no
		result=FAIL
	    fi
	fi
    else
	failed
    fi
}

###############################################################################
#
# set the permissions mask on a key
#
###############################################################################
function set_key_perm ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl setperm "$@" >>$OUTPUTFILE
    keyctl setperm "$@" >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e == $my_exitval ]
    then
	if [ $e = 0 ]
	then
	    check_notify setattr $1
	fi
    else
	failed
    fi
}

###############################################################################
#
# set the ownership of a key
#
###############################################################################
function chown_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl chown "$@" >>$OUTPUTFILE
    keyctl chown "$@" >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e == $my_exitval ]
    then
	if [ $e = 0 ]
	then
	    check_notify setattr $1
	fi
    else
	failed
    fi
}

###############################################################################
#
# set the group ownership of a key
#
###############################################################################
function chgrp_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl chgrp "$@" >>$OUTPUTFILE
    keyctl chgrp "$@" >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e == $my_exitval ]
    then
	if [ $e = 0 ]
	then
	    check_notify setattr $1
	fi
    else
	failed
    fi
}

###############################################################################
#
# run as a new session
#
###############################################################################
function new_session ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl session "$@" >>$OUTPUTFILE
    keyctl session "$@" >>$OUTPUTFILE 2>&1
    if [ $? != $my_exitval ]
    then
	failed
    fi
}

###############################################################################
#
# Create a new session and attach to the parent process (ie. the script)
#
###############################################################################
function new_session_to_parent ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl new_session "$@" >>$OUTPUTFILE
    keyctl new_session "$@" >>$OUTPUTFILE 2>&1
    if [ $? != $my_exitval ]
    then
	failed
    fi
}

###############################################################################
#
# instantiate a key
#
###############################################################################
function instantiate_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl instantiate "$@" >>$OUTPUTFILE
    keyctl instantiate "$@" >>$OUTPUTFILE 2>&1
    if [ $? != $my_exitval ]
    then
	failed
    fi
}

###############################################################################
#
# instantiate a key, piping the data in over stdin
#
###############################################################################
function pinstantiate_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    data="$1"
    shift

    echo echo -n $data \| keyctl pinstantiate "$@" >>$OUTPUTFILE
    echo -n $data | keyctl pinstantiate "$@" >>$OUTPUTFILE 2>&1
    if [ $? != $my_exitval ]
    then
	failed
    fi
}

###############################################################################
#
# reject a key
#
###############################################################################
function reject_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl reject "$@" >>$OUTPUTFILE
    keyctl reject "$@" >>$OUTPUTFILE 2>&1
    if [ $? != $my_exitval ]
    then
	failed
    fi
}

###############################################################################
#
# negate a key
#
###############################################################################
function negate_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl negate "$@" >>$OUTPUTFILE
    keyctl negate "$@" >>$OUTPUTFILE 2>&1
    if [ $? != $my_exitval ]
    then
	failed
    fi
}

###############################################################################
#
# set a key's expiry time
#
###############################################################################
function timeout_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl timeout $1 $2 >>$OUTPUTFILE
    keyctl timeout $1 $2 >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e == $my_exitval ]
    then
	if [ $e = 0 ]
	then
	    check_notify setattr $1
	fi
    else
	failed
    fi
}

###############################################################################
#
# Invalidate a key
#
###############################################################################
function invalidate_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl invalidate $1 >>$OUTPUTFILE
    keyctl invalidate $1 >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e == $my_exitval ]
    then
	if [ $e = 0 ]
	then
	    check_notify invalidated $1
	fi
    else
	failed
    fi
}

###############################################################################
#
# Do a DH computation
#
###############################################################################
function dh_compute ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl dh_compute $@ >>$OUTPUTFILE
    keyctl dh_compute $@ >>$OUTPUTFILE 2>&1
    if [ $? != $my_exitval ]
    then
	failed
    fi
}

###############################################################################
#
# Do a DH computation post-processed by a KDF
#
###############################################################################
function dh_compute_kdf ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl dh_compute_kdf $@ >>$OUTPUTFILE
    keyctl dh_compute_kdf $@ >>$OUTPUTFILE 2>&1
    if [ $? != $my_exitval ]
    then
	failed
    fi
}

###############################################################################
#
# Do a DH computation post-processed by a KDF with other information
#
###############################################################################
function dh_compute_kdf_oi ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl dh_compute_kdf_oi $@ >>$OUTPUTFILE
    keyctl dh_compute_kdf_oi $@ >>$OUTPUTFILE 2>&1
    if [ $? != $my_exitval ]
    then
	failed
    fi
}

###############################################################################
#
# Move a key between keyrings
#
###############################################################################
function move_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    fi

    echo keyctl move $* >>$OUTPUTFILE
    keyctl move $* >>$OUTPUTFILE 2>&1
    e=$?
    if [ $e = $my_exitval ]
    then
	if [ "x$1" = "x-f" ]; then shift; fi
	if [ $e = 0 -a $2 != $3 ]
	then
	    check_notify -2 unlinked $2 $1
	    check_notify linked $3 $1
	fi
    else
	failed
    fi
}

###############################################################################
#
# Query supported features
#
###############################################################################
function supports ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    elif [ "x$1" = "x--unrecognised" ]
    then
	my_exitval=3
	shift
    fi

    echo keyctl supports $* >>$OUTPUTFILE
    keyctl supports $* >>$OUTPUTFILE 2>&1
    err=$?
    if [ $err != $my_exitval ]
    then
	echo exitcode=$err >>$OUTPUTFILE
	failed
    fi
}

###############################################################################
#
# Make sure we sleep at least N seconds
#
###############################################################################
function sleep_at_least ()
{
    my_now=`date +%s`
    my_done_at=$(($my_now+$1+1))
    sleep $1
    while [ `date +%s` -lt $my_done_at ]
    do
	# Sleep in 1/50th of a second bursts till the time catches up
	sleep .02
    done
}

###############################################################################
#
# set gc delay time, return original value
#
###############################################################################
function set_gc_delay()
{
    delay=$1
    if [ -f $key_gc_delay_file ]; then
        echo $delay > $key_gc_delay_file
        echo "Set $key_gc_delay_file to $delay, orig: $orig_gc_delay"
    fi
}

###############################################################################
#
# watch a key
#
###############################################################################
function watch_key ()
{
    my_exitval=0
    if [ "x$1" = "x--fail" ]
    then
	my_exitval=1
	shift
    elif [ "x$1" = "x--fail2" ]
    then
	my_exitval=2
	shift
    fi

    echo keyctl watch "$@" >>$OUTPUTFILE
    nice --adjustment=-3 keyctl watch "$@" >>$PWD/notify.log 2>>$OUTPUTFILE
    if [ $? != $my_exitval ]
    then
	failed
    fi
}

###############################################################################
#
# Check for a notification
#
#	expect_notification [--filter=[i|p|l|n|c|r|v|s]] <keyid> <op> [<alt>]
#
###############################################################################
function expect_notification ()
{
    local want

    local filter=""
    case "x$1" in
	x--filter*)
	    case $1 in
		--filter=)  filter=;;
		--filter=i) filter=inst;;
		--filter=p) filter=upd;;
		--filter=l) filter=link;;
		--filter=n) filter=unlk;;
		--filter=c) filter=clr;;
		--filter=r) filter=rev;;
		--filter=v) filter=inv;;
		--filter=s) filter=attr;;
		*)
		    echo "Unknown param $1 to expect_notification()" >&2
		    exit 2
		    ;;
	    esac
	    shift
	    ;;
    esac
    
    if [ $# = 2 ]
    then
	want="$1 $2"
	op=$2
    elif [ $# = 3 ]
    then
	want="$1 $2 $3"
	op=$2
    else
	echo "Wrong parameters to expect_notification" >&2
	exit 2
    fi

    if tail -3 $PWD/notify.log | grep "^${want}\$" >/dev/null
    then
	echo "Found notification '$*'" >>$OUTPUTFILE
	if [ "$filter" != "" -a $op != "$filter" ]
	then
	    echo "Notification '$want' should be filtered" >&2
	    failed
	fi
    else
	echo "Notification '$*' not present" >>$OUTPUTFILE
	if [ "$filter" = "" ]
	then
	    echo "Missing notification '$want'" >&2
	    failed
	elif [ $op = "$filter" ]
	then
	    echo "Notification unexpectedly filtered '$want' $filter" >&2
	    failed
	fi
    fi
}

###############################################################################
#
# Note the creation of a new key
#
#	expect_new_key <variable_name> <keyring> [<expected_id>]
#
###############################################################################
function xxx_expect_new_key ()
{
    my_varname=$1
    my_keyring=$2

    my_keyid="`tail -1 $OUTPUTFILE`"
    if expr "$my_keyid" : '[1-9][0-9]*' >&/dev/null
    then
	eval $my_varname=$my_keyid

	if [ $# = 3 -a "x$my_keyid" != "x$2" ]
	then
	    failed
	fi

	watch_add_key $my_keyid
	check_notify linked $my_keyring $my_keyid
    else
	eval $my_varname=no
	result=FAIL
    fi
}

###############################################################################
#
# Note implicit update of a key
#
#	implicit_update <key_id>
#
###############################################################################
function xxx_implicit_update ()
{
    my_keyid=$1

    got_keyid="`tail -1 $OUTPUTFILE`"
    if expr "$got_keyid" : '[1-9][0-9]*' >&/dev/null
    then
	if [ "x$got_keyid" == "x$my_keyid" ]
	then
	    check_notify updated $my_keyid
	else
	    failed
	fi
    else
	result=FAIL
    fi
}

###############################################################################
#
# Note the explicit update of new key
#
###############################################################################
function xxx_key_updated ()
{
    my_keyid=$1

    check_notify updated $my_keyid
}

###############################################################################
#
# extract a key ID from the log file
#
###############################################################################
function xxx_expect_found_key ()
{
    my_keyid="`tail -1 $OUTPUTFILE`"
    if expr "$my_keyid" : '[1-9][0-9]*' >&/dev/null
    then
	if [ "x$my_keyid" != "x$1" ]
	then
	    failed
	fi
    else
	eval $my_varname=no
	result=FAIL
    fi
}
