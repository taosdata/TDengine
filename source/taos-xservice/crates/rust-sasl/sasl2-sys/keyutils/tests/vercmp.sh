# Commandline-driver tester for version comparison shell functions
#
###############################################################################
#
# Copyright (C) 2013 Red Hat, Inc. All Rights Reserved.
# Written by David Howells (dhowells@redhat.com)
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version
# 2 of the License, or (at your option) any later version.
#
###############################################################################

. ./version.inc.sh

#
# Compare versions
#
if [ "$1" = "" -o "$2" = "" ]
then
    echo "Missing version parameters" >&2
    exit 2
fi

if version_less_than $1 $2
then
    echo "$1 < $2"
else
    echo "$1 >= $2"
fi
