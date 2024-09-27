# Version comparison shell functions
#
###############################################################################
#
# Copyright (C) 2005, 2013 Red Hat, Inc. All Rights Reserved.
# Written by David Howells (dhowells@redhat.com)
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version
# 2 of the License, or (at your option) any later version.
#
###############################################################################

###############################################################################
#
# compare version numbers to see if the first is less (older) than the second
#
###############################################################################
function version_less_than ()
{
    a=$1
    b=$2

    if [ "$a" = "$b" ]
    then
	return 1
    fi

    # grab the leaders
    a_version=${a%%-*} a_release=${a#*-}
    b_version=${b%%-*} b_release=${b#*-}

    if [ "$a_version" = "$b_version" ]
    then
	case "$a_release" in
	    rc[0-9]*)
		case "$b_release" in
		    rc[0-9]*)
			__version_less_than_dot "${a_release#rc}" "${b_release#rc}"
			return $?
			;;
		    *)
			return 0;
			;;
		esac
		;;
	esac

	case "$b_release" in
	    rc[0-9]*)
		return 1;
		;;
	esac

	if [ "$a_version" = "$a" -o "$b_version" = "$b" ]
	then
	    if [ "$a_version" = "$b_version" ]
	    then
		[ "$a_version" = "$a" ]
	    else
		__version_less_than_dot "$a_version" "$b_version"
	    fi
	fi
	__version_less_than_dot "$a_release" "$b_release"
    else
	__version_less_than_dot "$a_version" "$b_version"
    fi
}

function __version_less_than_dot ()
{
    a=$1
    b=$2

    if [ "$a" = "$b" ]
    then
	return 1
    fi

    # grab the leaders
    x=${a%%.*}
    y=${b%%.*}

    if [ "$x" = "$a" -o "$y" = "$b" ]
    then
	if [ "$x" = "$y" ]
	then
	    [ "$x" = "$a" ]
	else
	    expr "$x" \< "$y" >/dev/null
	fi
    elif [ "$x" = "$y" ]
    then
	__version_less_than_dot "${a#*.}" "${b#*.}"
    else
	expr "$x" \< "$y" >/dev/null
    fi
}

###############################################################################
#
# Return true if the keyutils package being tested is older than the given
# version.
#
###############################################################################
function keyutils_older_than ()
{
    version_less_than $KEYUTILSVER $1
}

###############################################################################
#
# Return true if the keyutils package being tested is at or later than the
# given version.
#
###############################################################################
function keyutils_at_or_later_than ()
{
    ! keyutils_older_than $1
}

###############################################################################
#
# Return true if the keyutils package being tested is newer than the given
# version.
#
###############################################################################
function keyutils_newer_than ()
{
    version_less_than $1 $KEYUTILSVER
}

###############################################################################
#
# Return true if the keyutils package being tested is at or older than the
# given version.
#
###############################################################################
function keyutils_at_or_older_than ()
{
    ! keyutils_newer_than $1
}

###############################################################################
#
# Return true if the kernel being tested is older than the given version.
#
###############################################################################
function kernel_older_than ()
{
    version_less_than $KERNELVER $1
}

###############################################################################
#
# Return true if the kernel being tested is at or later than the given version.
#
###############################################################################
function kernel_at_or_later_than ()
{
    ! kernel_older_than $1
}

###############################################################################
#
# Return true if the kernel being tested is a RHEL-6 kernel and is at or later
# than the given version.
#
###############################################################################
function rhel6_kernel_at_or_later_than ()
{
    case $OSDIST-$OSRELEASE in
	RHEL-6.*)
	    ! kernel_older_than $1
	    ;;
	*)
	    false
	    ;;
	esac
}

###############################################################################
#
# Return true if the kernel being tested is a RHEL-7 kernel and is at or later
# than the given version.
#
###############################################################################
function rhel7_kernel_at_or_later_than ()
{
    case $OSDIST-$OSRELEASE in
	RHEL-7.*)
	    ! kernel_older_than $1
	    ;;
	*)
	    false
	    ;;
	esac
}
