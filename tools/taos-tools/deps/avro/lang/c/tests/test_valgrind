#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0 
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
# https://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License. 
set +e
set -x

if ! which valgrind; then
	echo "Unable to find valgrind installed. Test will not run."
	# This special exit value will show that we skipped this test
	exit 77
fi

../libtool execute valgrind --leak-check=full -q test_avro_data 2>&1 |\
grep -E '^==[0-9]+== '
if [ $? -eq 0 ]; then
	# Expression found. Test failed.
	exit 1
else
	# We're all clean
	exit 0
fi
