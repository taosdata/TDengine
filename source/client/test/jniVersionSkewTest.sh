#!/usr/bin/env bash

# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

# Manual check, not registered with ctest: it needs a JDK, which the unit-test
# image does not have.  Run it after touching the JNI layer of the wrapper
# (src/client/wrapper/{src,jni}).
#
# The JNI layer caches jclass/jmethodID/jfieldID values during its first call
# (TSDBJNIConnector.initImp).  A jar and a libtaos.so from different releases --
# the usual half-finished upgrade -- make those lookups fail, and handing a NULL
# id to GetMethodID()/NewObject()/CallVoidMethod() is undefined behaviour:
# HotSpot aborts the whole JVM with SIGABRT/SIGSEGV and a core dump instead of
# letting the connector report the mismatch.  The stub class below declares the
# native methods but none of the classes the layer looks for, which is the
# smallest thing that reproduces it.  The stub then calls a guarded entry point
# with made-up handles, which has to return TSDB_CODE_DLL_NOT_LOAD instead of
# dereferencing what those handles fake.
#
# Usage:
#   bash jniVersionSkewTest.sh <libtaos.so> [libtaos.so of an older build]
#
# The JVM must survive and say why.  With a second library the same check runs
# against it first and is expected to abort, which is how the fix was verified.
#
# Side effects to expect on a failure: the kernel core_pattern may drop a core
# into /corefile (the JVM's own hs_err file is kept inside a temporary
# directory and removed on exit).

set -uo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: bash $(basename "$0") <path to libtaos.so> [path to an older libtaos.so]" >&2
  exit 2
fi

NEW_LIB=$1
OLD_LIB=${2:-}

if [[ ! -f "$NEW_LIB" ]]; then
  echo "FAIL: $NEW_LIB does not exist" >&2
  exit 2
fi

for tool in javac java; do
  if ! command -v "$tool" > /dev/null 2>&1; then
    echo "SKIP: $tool not found, this check needs a JDK" >&2
    exit 0
  fi
done

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

mkdir -p "$WORK/src/com/taosdata/jdbc" "$WORK/classes" "$WORK/cfg"
cat > "$WORK/src/com/taosdata/jdbc/TSDBJNIConnector.java" <<'JAVA'
package com.taosdata.jdbc;

import java.util.ArrayList;

public class TSDBJNIConnector {
  public native void initImp(String configDir);

  public native int getSchemaMetaDataImp(long con, long res, ArrayList<Object> list);

  public static void main(String[] args) {
    System.load(args[0]);
    TSDBJNIConnector conn = new TSDBJNIConnector();
    conn.initImp(args[1]);

    // The init above failed, so the entry point that uses the cached ids has to
    // report it.  The handles are made-up non-NULL values on purpose: reaching
    // the ids with them would dereference junk, which is the crash being tested.
    int code = conn.getSchemaMetaDataImp(1L, 1L, new ArrayList<Object>());
    System.out.println("getSchemaMetaDataImp -> 0x" + Integer.toHexString(code));
  }
}
JAVA

javac -d "$WORK/classes" "$WORK/src/com/taosdata/jdbc/TSDBJNIConnector.java" || {
  echo "FAIL: cannot compile the stub connector" >&2
  exit 2
}

# $1: library, $2: "expect ok" / "expect abort"
run_case() {
  local lib=$1 expect=$2 out rc
  out=$(java -XX:ErrorFile="$WORK/hs_err_%p.log" -cp "$WORK/classes" \
    com.taosdata.jdbc.TSDBJNIConnector "$lib" "$WORK/cfg" 2>&1)
  rc=$?

  echo "  library: $lib"
  echo "$out" | sed 's/^/    /'

  if [[ $expect == ok ]]; then
    if [[ $rc -ne 0 ]]; then
      echo "  FAIL: the JVM died with exit code $rc"
      ls "$WORK"/hs_err_*.log > /dev/null 2>&1 && sed 's/^/    /' "$WORK"/hs_err_*.log | head -30
      return 1
    fi
    if ! grep -q 'JNI ERROR cannot find class' <<< "$out"; then
      echo "  FAIL: the missing class was not reported"
      return 1
    fi
    # TSDB_CODE_DLL_NOT_LOAD, the code a guarded entry point returns
    if ! grep -q 'getSchemaMetaDataImp -> 0x8000010b' <<< "$out"; then
      echo "  FAIL: the guarded entry point did not report TSDB_CODE_DLL_NOT_LOAD"
      return 1
    fi
    echo "  ok: JVM alive, the reason was reported and the guarded entry point failed cleanly"
    return 0
  fi

  # expect abort: rc 134 is SIGABRT, the pre-fix behaviour
  if [[ $rc -eq 0 ]]; then
    echo "  FAIL: expected the JVM to abort, it exited 0 instead"
    return 1
  fi
  echo "  ok: aborted as expected (exit code $rc)"
  return 0
}

failed=0
if [[ -n "$OLD_LIB" ]]; then
  echo "--- negative control: the library without the guard has to abort"
  run_case "$OLD_LIB" abort || failed=1
fi

echo "--- the library under test has to survive the mismatch"
run_case "$NEW_LIB" ok || failed=1

if [[ $failed -ne 0 ]]; then
  echo "jniVersionSkewTest: FAILED"
  exit 1
fi

echo "jniVersionSkewTest: PASSED"
