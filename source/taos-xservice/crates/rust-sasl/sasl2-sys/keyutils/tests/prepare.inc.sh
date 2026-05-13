# preparation script for running keyring tests

# Find the relative path from pwd to the directory holding this file
includes=${BASH_SOURCE[0]}
includes=${includes%/*}/

# Check if currently running kernel has option set
function has_kernel_config()
{
        local option=$1
        local uname=$(uname -r)
        local config_list="$KCONFIG_PATH
                     /lib/modules/$uname/build/.config
                     /boot/config-$uname
                     /lib/kernel/config-$uname"

        for config in $config_list; do
                [ ! -f $config ] && continue
                grep -qE "^${option}=[my]" $config
                return
        done

        echo "Failed to find kernel configuration file"
        return false
}

# --- need to run in own session keyring
watch_fd=0
if [ "$1" != "--inside-test-session" ]
then
    session_name=RHTS/keyctl/$$
    if keyctl supports notify >&/dev/null
    then
	# Create a session keyring and set up a watcher on it.  The watch queue
	# is exposed on fd 9 inside the child process.
	echo "Running with watched session keyring $session_name"
	export watch_log=$PWD/watch.out
	export gc_log=$PWD/gc.out
	watch_fd=9
	echo "starting" >$watch_log
	echo "starting" >$gc_log
	exec keyctl watch_session -n $session_name $watch_log $gc_log $watch_fd \
	     bash $0 --inside-test-session $@ || exit 8
    else
	echo "Running with session keyring $session_name"
	exec keyctl session $session_name bash $0 --inside-test-session $@ || exit 8
    fi
else
    shift
    if [ "$KEYCTL_WATCH_FD" != "" ]
    then
	watch_fd=$KEYCTL_WATCH_FD
    fi
fi

# Set up for the Red Hat Test System
RUNNING_UNDER_RHTS=0
if [ -x /usr/bin/rhts_environment.sh ]
then
    PACKAGE=$(rpm -q --qf "%{name}" --whatprovides /bin/keyctl)
    . /usr/bin/rhts_environment.sh
    RUNNING_UNDER_RHTS=1
elif [ -z "$OUTPUTFILE" ]
then
    OUTPUTFILE=$PWD/test.out
    echo -n >$OUTPUTFILE
fi

case `lsb_release -i -s` in
    Fedora*)		OSDIST=Fedora;;
    RedHatEnterprise*)	OSDIST=RHEL;;
    *)			OSDIST=Unknown;;
esac

OSRELEASE=`lsb_release -r -s`

KEYUTILSVER=`keyctl --version 2>/dev/null`
if [ -n "$KEYUTILSVER" ]
then
    :
elif [ -x /bin/rpm ]
then
    KEYUTILSVER=`rpm -q keyutils`
else
    echo "Can't determine keyutils version" >&2
    exit 9
fi

echo "keyutils version: $KEYUTILSVER"
KEYUTILSVER=`expr "$KEYUTILSVER" : '.*keyutils-\([0-9.]*\).*'`

. $includes/version.inc.sh

KERNELVER=`uname -r`

#
# Make sure the TEST envvar is set.
#
if [ -z "$TEST" ]
then
    p=`pwd`
    case $p in
	*/keyctl/*)
	    TEST=keyctl/${p#*/keyctl/}
	    ;;
	*/bugzillas/*)
	    TEST=bugzillas/${p#*/bugzillas/}
	    ;;
	*)
	    TEST=unknown
	    ;;
    esac
fi

have_key_invalidate=0
have_big_key_type=0
have_dh_compute=0
have_restrict_keyring=0
have_notify=0

if keyctl supports capabilities >&/dev/null
then
    eval `keyctl supports`
else
    #
    # Work out whether key invalidation is supported by the kernel
    #
    if keyutils_at_or_later_than 1.5.6 && kernel_at_or_later_than 3.5-rc1
    then
	have_key_invalidate=1
    fi

    #
    # Work out whether the big_key type is supported by the kernel.
    #
    if [ $OSDIST = RHEL ] && ! version_less_than $OSRELEASE 7
    then
	# big_key is backported to 3.10 for RHEL-7
	have_big_key_type=1
    elif kernel_at_or_later_than 3.13-rc1
    then
	have_big_key_type=1
    fi

    #
    # Work out whether Diffie-Hellman is supported by the kernel
    #
    if [ $OSDIST = RHEL ]
    then
	:
    elif keyutils_at_or_later_than 1.5.10 && kernel_at_or_later_than 4.7-rc1
    then
	have_dh_compute=1
    fi

    #
    # Work out whether keyring restrictions are supported by the kernel
    #
    if keyutils_at_or_later_than 1.6 && kernel_at_or_later_than 4.12-rc1
    then
	have_restrict_keyring=1
    fi
fi

#
# Check if skipping of tests requiring root was requested
#
skip_root_required=0
if [ "$SKIPROOTREQ" = "yes" ]
then
    skip_root_required=1
fi

#
# Check if skipping of tests requiring installation was requested
#
skip_install_required=0
if [ "$SKIPINSTALLREQ" = "yes" ]
then
    skip_install_required=1
fi
