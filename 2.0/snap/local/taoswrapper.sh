#!/bin/sh
# Wrapper to check for custom config in $SNAP_USER_COMMON or $SNAP_COMMON and
# use it otherwise fall back to the included basic config which will at least
# allow mosquitto to run and do something.
# This script will also copy the full example config in to SNAP_USER_COMMON or
# SNAP_COMMON so that people can refer to it.
#
# The decision about whether to use SNAP_USER_COMMON or SNAP_COMMON is taken
# based on the user that runs the command. If the user is root, it is assumed
# that mosquitto is being run as a system daemon, and SNAP_COMMON will be used.
# If a non-root user runs the command, then SNAP_USER_COMMON will be used.

case "$SNAP_USER_COMMON" in
	*/root/snap/tdengine/common*) COMMON=$SNAP_COMMON ;;
	*)                             COMMON=$SNAP_USER_COMMON ;;
esac

if [ -d /etc/taos ]; then
    CONFIG_FILE="/etc/taos"
else
    CONFIG_FILE="$SNAP/etc/taos"
fi

# Launch the snap
$SNAP/usr/bin/taos -c $CONFIG_FILE $@
