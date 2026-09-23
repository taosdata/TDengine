#!/bin/bash

# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

while getopts "c:f:h" arg
do
  case "$arg" in
  c)
    func_list_file=$OPTARG
    ;;
  f)
    source_file=$OPTARG
    ;;
  h)
    echo "Usage: -c [function list file]       "   
    echo "       -f [source file or directory] "
    echo "       -h help"
    exit 0
    ;;
  *) 
    echo "Invalid option: -$OPTARG"
    exit 1
    ;;
  esac
done

# echo "funclist list configuration file: $func_list_file"

if [ ! -e "$func_list_file" ] || [ ! -r "$func_list_file" ]
then
    echo "$func_list_file doesn't exist or is not readable"
fi

# cat $func_list_file

grep_string=""
separator="\|"

while read -r line
do
    # echo "current function: $line"
    tmp="$grep_string"
    grep_string="$tmp$line$separator"
    # echo "grep_string: $grep_string"
done < $func_list_file

# echo "${grep_string:1:-2}"

# echo "We are going to check (void) function invocation in $*"

grep -Rn "${grep_string:1:-2}" $source_file |grep -v "test" > ./check_void_result.txt

if [ -s ./check_void_result.txt ]
then
    failed_number=$(cat ./check_void_result.txt|wc -l )
    echo "Found  $failed_number (void) function invocation  in $source_file:"
    cat ./check_void_result.txt
    exit 1
else
    echo "No (void) function invocation in $source_file"
    exit 0
fi